# Flink jobs

Flink 2.2.1 session cluster (compose profile `stream`: 1 JobManager, 1 TaskManager with 4 slots), PyFlink 2.2.1
DataStream API, Kafka connector 5.0.0-2.2. Jobs live in `pipelines/flink/` (mounted at `/opt/flink/jobs`). UI:
http://127.0.0.1:8088.

```bash
docker compose -f infra/docker-compose.yaml --profile core --profile stream up -d --build
python pipelines/flink/check_windows.py --replay-from "2026-06-01 00:00" --limit 20000     # late events + duplicates
python pipelines/flink/check_features.py --replay-from "2025-03-01 00:00" --limit 100000   # realtime features
python pipelines/flink/check_ato.py --replay-from "2024-03-01 00:00" --limit 150000        # out-of-order sessions
python pipelines/flink/check_hot_key.py --replay-from "2025-01-01 00:00" --limit 1000000   # hot key, parallelism 4
docker exec flink-jobmanager flink run -py /opt/flink/jobs/raw_landing.py         # raw events -> S3 landing
```

Each `check_*.py` loads a fixed slice of the generated stream into a replay topic, runs the job over it as a bounded
job (from the earliest to the latest offset, then it stops), reads the output topic and recomputes the expected
result from the Parquet files with pandas. The numbers below are those comparisons.

| Job | Input | Output |
|---|---|---|
| `auth_windows.py --mode baseline\|lateness\|dedup` | `card_auth_replay` | `auth_counts_<mode>`, late events `auth_late_<mode>` |
| `rt_features.py` | `card_auth_replay` | `card_features_rt` (feature view `card_auth_realtime`) |
| `ato_sequence.py --order arrival\|event` | `app_events_replay` | `ato_alerts_<order>` |
| `hot_key.py --mode hot\|two_phase` (parallelism 4) | `card_auth_replay` | `unit_counts_<mode>` |
| `raw_landing.py` (Flink SQL) | `card_authorizations` | `s3://lakehouse/landing/card_authorizations/auth_date=*/` |

## Event time

The producer sets each Kafka message timestamp to the event time (`auth_ts`, `event_ts`), as a card network or app
would. The jobs use it directly: `WatermarkStrategy.for_bounded_out_of_orderness(...)` without a timestamp assigner,
so watermarks are generated inside the Kafka source.

## Late events and duplicates (`pipelines/flink/auth_windows.py`)

Authorizations per card per 5-minute tumbling window, watermark 5 s (normal network delay is at most 2 s). The three
modes add one fix each. Replay: 20,000 authorizations from 2026-06-01, of which 301 are network-retry duplicates and
485 arrive 5 to 30 minutes late (offline POS terminals); 19,696 card windows.

| Mode | What it adds | Late events dropped | Card windows exact | Overcounted | Undercounted |
|---|---|---:|---:|---:|---:|
| `baseline` | watermark 5 s | 483 (to side output) | 96.08% | 295 | 477 |
| `lateness` | `allowed_lateness(30 min)` + side output | 0 | 98.47% | 301 | 0 |
| `dedup` | keyed state on `auth_id` (TTL 1 h) before the window | 0 | **100.00%** | 0 | 0 |

- In `baseline` the late events miss their window and vanish (here they go to a side output, so they can be
  counted), and retries are counted twice.
- Allowed lateness keeps each window's state 30 minutes past the watermark. The window fires on time and fires again
  when a late event arrives, so results are early and still complete. Raising the watermark delay to 30 minutes
  instead would hold every result back by 30 minutes.
- Dedup keeps a flag per `auth_id` for 1 hour (retries come 1 to 10 s later) and passes only the first copy.
  It runs before any count.

![auth_windows dedup job: 20,000 events in, 19,699 after dedup, 19,696 windows](images/flink_auth_windows_dedup.png)

### What it took to see the late events in a replay

The first runs reported 0 late events. Three replay effects hid them, each fixed in the job or the setup:

1. PyFlink 2.2 ignores a Python `TimestampAssigner` passed to `from_source`: windows fell on the Kafka timestamp,
   which was then the send time, so every event landed in the current 5 minutes. Fix: the producer writes event time
   as the Kafka timestamp.
2. The watermark moves in steps. It is emitted every 200 ms by default, and a replay covers days of event time in
   seconds, so one step jumped hours and a 30-minute-late event still arrived "before" it. The Python map also takes
   1,000 events per bundle, which lets the source run ahead. Fix: watermark every 10 ms and a Python bundle size of
   1, so a step covers a few events, as with live traffic.
3. The watermark is the minimum over Kafka partitions. With the 6-partition topic, the partitions were read at
   very different speeds: a debug job measured the watermark at `Long.MIN_VALUE` for the first 30,000 events and
   then days behind the newest event, so almost nothing looked late (206 of 1,218 in a 50,000-event replay).
   Watermark alignment (`with_watermark_alignment`, which pauses partitions that run ahead) is the production fix for
   replays and backfills, but with a bounded source it failed in this setup with
   `IllegalStateException: No current assignment for partition card_authorizations-5` once a partition was finished.
   Fix for the demo: the replay topic has 1 partition and the job runs with parallelism 1.

With live traffic none of this applies: partitions advance with the wall clock and a 200 ms step covers a few events.

## Realtime features (`pipelines/flink/rt_features.py`)

One feature row per authorization, computed when it arrives over the same card's earlier authorizations:
`auth_count_10m`, `amount_sum_1h`, `distinct_merchants_1h`, `distinct_units_1h`. The job is a `KeyedProcessFunction`
per card that holds the last 90 minutes of authorizations in state. A window would only report when it closes, and
the scoring model needs the features of the payment being authorized now. Duplicates are dropped first, and
locations are mapped to the 34 units (the 63-row mapping is loaded once; a broadcast stream would be needed only if
it changed at runtime).

Replay: 100,000 authorizations from 2025-03-01 (before the reorganization, when 20% of location strings are old
province names); 98,529 unique, one feature row each.

| Feature | Rows equal to the pandas recomputation |
|---|---:|
| `auth_count_10m`, `amount_sum_1h`, `distinct_merchants_1h`, `distinct_units_1h` | 100.00% each |

| Authorizations whose card was in 2 or more places within the hour | Count |
|---|---:|
| by raw location name | 122 |
| by unit (what the job emits as `distinct_units_1h`) | 14 |

108 of the 122 would be "impossible travel" alerts caused only by old and new names of the same place (for example
"Binh Duong" and "Ho Chi Minh").

## Out-of-order app events (`pipelines/flink/ato_sequence.py`)

The `new_device` account takeover pattern: `password_reset` → `add_payee` → `transfer` within 10 minutes in one
session. 15% of app events reach Kafka after the next event of their session (1 to 20 s, parallel uploads).

- `arrival`: a state machine per session fed in arrival order.
- `event`: each session's events wait in keyed state; an event-time timer at each `event_ts` releases them in
  `event_ts` order once the watermark (30 s behind the newest event) passes, then the same state machine runs.

Replay: 150,000 app events from 2024-03-01 (before the biometric rule, which later stops most takeovers before the
transfer); 67,443 sessions, 771 with the full pattern in event time, 658 of them generator-labelled `new_device` ATO.

| Order | Alerts | Pattern sessions caught | ATO caught |
|---|---:|---:|---:|
| arrival | 578 | 557 / 771 (72.2%) | 461 / 658 (70.1%) |
| event time | 795 | **771 / 771 (100%)** | **658 / 658 (100%)** |

Reordering costs 30 s of latency per alert and finds every takeover; arrival order misses 3 in 10. Both modes raise
about 20 alerts on sessions outside the truth query (21 and 24): the pandas query compares the first `add_payee`
and `transfer` of the session, while the state machine takes the first ones after the `password_reset`.

## Hot key (`pipelines/flink/hot_key.py`)

Authorizations per unit per day (an operations metric: payment volume by province), with parallelism 4. Ho Chi Minh
is 60% of all authorizations, so `key_by(unit)` sends 60% of the stream, plus whatever other units hash to the same
place, to one subtask.

```bash
python pipelines/flink/check_hot_key.py --replay-from "2025-01-01 00:00" --limit 1000000                  # hot + two_phase
python pipelines/flink/check_hot_key.py --replay-from "2025-01-01 00:00" --limit 1000000 --no-replay --modes two_phase --salt-all
```

Replay: 1,000,000 authorizations from 2025-01-01 (Ho Chi Minh 59.9%, Ha Noi 18.0%, Da Nang 5.0%), 4,283 unit-days.
Every mode returns exactly the pandas counts (4,283 of 4,283 unit-days, 0 late events: the watermark waits 31
minutes, longer than the latest POS event). Per-subtask records and accumulated busy time come from the REST API
after the job ends.

| Mode | Job time | Records into the counting subtasks | Busiest subtask |
|---|---:|---|---|
| `hot`: `key_by(unit)` | 71.4 s | 97,976 / **842,368** / 32,823 / 26,833 | 84% of records, busy 100%; the other three 69-84% idle |
| `two_phase`, 8 salts on Ho Chi Minh only | **50.7 s** | 247,514 / 318,138 / 107,782 / 326,566 | 33% of records; three subtasks busy 94-100% |
| `two_phase`, 8 salts on every unit (`--salt-all`) | 112.5 s | 192,699 / 176,333 / 149,634 / 481,334 | 48%, and slower than no fix |

While the hot job runs, the UI colours the counting vertex red (busy 99%, data skew 100%) and shows the operators in
front of it backpressured (88% and 100%): they could go faster, but the one hot subtask cannot take more.

![hot: the counting vertex busy 99% with data skew 100%; upstream backpressured 88-100%](images/flink_hot_key_hot.png)

![Data Skew tab: 100% on the counting vertex](images/flink_hot_key_skew.png)

In two-phase aggregation, Ho Chi Minh's key gets a salt from `auth_id` (`Ho Chi Minh#0` .. `#7`), so its records
spread over all subtasks; each counts its share per day, then `key_by(unit)` adds the at most 8 partial counts of a
unit-day. Phase 2 receives 5,165 records instead of 1,000,000. The salt comes from `auth_id`, so a network retry
lands in the same partial count as its original. With two phases the skew of the counting vertex drops to 28% and
backpressure on the operator before it to 48%:

![two_phase: data skew 28%, upstream backpressure 48%](images/flink_hot_key_two_phase.png)

Only hot keys should get a salt. Salting all 34 units made the job slower (112.5 s) even though its records were
spread better. In PyFlink each new (key, window) pair costs a state entry and a timer registration through the Python
runtime; a small unit has a few records per day, so with 8 salts it pays that cost 8 times for almost no records.
The measurements show it: subtasks holding many small units process about 4,500 records/s, the Ho Chi Minh subtask
12,000 records/s, and two-phase on every unit got slower with each salt added (4 salts 102 s, 8 salts 111 s, 16 salts
135 s). Ho Chi Minh has about 4,700 records per day, so its 8 partial keys are still full. A salt chosen from the
event minute (fewer key switches) changed nothing (99.8 s vs 102.0 s for 4 salts), which rules out key switching as
the cost.

The source stays backpressured in both modes: the Python operators remain the limit of this job. Two-phase removes
the hot spot; it does not make Python faster. `key_by(card_id)` would also spread the load, but it answers a
different question (per card, not per unit).

## Raw events to bronze (`pipelines/flink/raw_landing.py` + `pipelines/spark/ingest_bronze.py`)

Flink SQL copies `card_authorizations` unchanged (duplicates and late events included, plus Kafka partition and
offset for tracing) to `s3://lakehouse/landing/card_authorizations/auth_date=YYYY-MM-DD/` as JSON lines; the file sink
commits files on checkpoints (every 10 s). `ingest_bronze.py --part landing` then appends them to the Delta table
`bronze.raw_card_authorizations` with a Spark file stream (`trigger(availableNow=True)`): its checkpoint remembers
which files were loaded, so a late event that adds a file to an older `auth_date` is still picked up, and a re-run
loads nothing twice.

| Step | Result |
|---|---|
| `raw_landing.py` on the 50,000-event topic | 8 `auth_date` partitions, 14.8 MB, 5 s |
| `ingest_bronze.py --part landing` | `bronze.raw_card_authorizations`: 50,000 rows (52 s) |
| same command again | still 50,000 rows |

The landing files are JSON because Flink's Parquet writer needs the Hadoop client jars on the classpath. JSON needs
none, and Spark writes Delta in the next step.

## Notes

- Topics written with event-time timestamps need `retention.ms=-1`: with the default 7 days Kafka deletes 2024-2026
  messages as soon as a segment rolls. `infra/init_infra.py` and the check scripts create them that way.
- PyFlink 2.2: windows and state TTL take `Time`, watermarks take `Duration`; a side-output tag typed as a tuple
  fails to pickle, so the late-event tag is untyped.
