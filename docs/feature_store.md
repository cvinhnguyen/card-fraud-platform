# Feature store (Feast)

The model reads the same features in training and in serving through one Feast repository: the Spark offline store
over the Delta tables of the lakehouse for training, Redis for the Scoring API. Feast is the hand-over between the
layers: Layer 1 writes the features into it and Layer 2 reads them. Feast 0.66, SQL registry in Postgres (`ml-db`,
database `feast`).

| | |
|---|---|
| Code | [`ml/feature_repo/`](https://github.com/cvinhnguyen/card-fraud-platform/tree/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/feature_repo), [`ml/jobs/push_job.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/jobs/push_job.py), [`pipelines/airflow/dags/feast_materialize.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/pipelines/airflow/dags/feast_materialize.py) |
| Runs on | a library inside the Scoring API, the push jobs, Airflow, Jupyter and the KFP steps; push jobs `push-online` / `push-offline`: compose profile `ml`; materialization: Airflow DAG `feast_materialize` |
| Reads | `gold.feat_card_txn_stats_daily`, `gold.dim_card`, Kafka `card_features_rt` and `card_device_seen` |
| Writes | Redis online store; `gold.feat_card_auth_rt` and `gold.feat_card_device` (push job B) |
| Related | [ml_training.md](ml_training.md), [serving.md](serving.md), [flink_jobs.md](flink_jobs.md), [schema_design.md](schema_design.md) |

How the features flow into Feast and out to training and serving:

```mermaid
flowchart LR
    DP3["DP3 (Spark, nightly)"] --> stats["gold.feat_card_txn_stats_daily"]
    dbt --> dim["gold.dim_card"]
    auth["Kafka card_authorizations"] --> rt["Flink rt_features.py"] --> trt["card_features_rt"]
    auth --> dev["Flink device_seen.py"] --> tdev["card_device_seen"]
    trt & tdev --> A["push job A"] --> redis[("Redis online store")]
    trt & tdev --> B["push job B"] --> deltart["gold.feat_card_auth_rt / feat_card_device"]
    stats --> mat["DAG feast_materialize"] --> redis
    stats & dim & deltart --> off["Spark offline store"] --> train["training (point-in-time join)"]
    redis --> api["Scoring API"]
```

## Run it

With DP2 and DP3 run (the gold and feature tables exist) and the `ml` profile:

```bash
docker compose -f infra/docker-compose.yaml --profile core --profile ml up -d
docker compose -f infra/docker-compose.yaml exec notebook feast -c /opt/ml/feature_repo apply   # definitions, after a change
# bootstrap, once before the first daily run (see Pipelines)
docker compose -f infra/docker-compose.yaml exec notebook feast -c /opt/ml/feature_repo materialize 2000-01-01T00:00:00 <ds+1>T00:00:00
docker compose -f infra/docker-compose.yaml --profile ml up -d push-online push-offline         # push jobs A and B
```

Replace `<ds+1>` with the day after the last feature date (`YYYY-MM-DD`).

`feast apply` needs the push-source Delta tables to exist (see [Gotchas](#gotchas)). The daily materialization runs in
the DAG `feast_materialize`, triggered by DP3.

## Feature views

The views are defined in
[`ml/feature_repo/fraud_features.py` L46-L72](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/feature_repo/fraud_features.py#L46-L72),
the on-demand view in
[`ml/feature_repo/fraud_features.py` L80-L120](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/feature_repo/fraud_features.py#L80-L120):

| View | Source | Key | TTL | Written by |
|---|---|---|---|---|
| `card_txn_stats_daily` | `gold.feat_card_txn_stats_daily` | card | none | DP3, one row the night after each day the card was active |
| `card_profile` | `gold.dim_card` (SQL query) | card | none | dbt |
| `card_auth_realtime` | push source `card_auth_rt_push`, batch source `gold.feat_card_auth_rt` | card | 1 h | Flink `rt_features.py`, push jobs A / B |
| `card_device` | push source `card_device_push`, batch source `gold.feat_card_device` | card, device | none | Flink `device_seen.py`, push jobs A / B |
| `fraud_request_features` | on demand: the request + the four views | | | computed per request (pandas) |

Feature service `fraud_model_v1` = 7 daily stats + the 13 on-demand features
([`ml/feature_repo/fraud_features.py` L123-L128](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/feature_repo/fraud_features.py#L123-L128)).
The model is trained on exactly this list and the API asks for exactly this list, so the two cannot drift apart: a
model with other inputs gets `fraud_model_v2`.

## TTL

TTL is how old a stored value may be and still describe the card. One Feast behaviour drives every choice: the
Redis lookup returns the stored value however old it is. TTL bounds the point-in-time join (training) and the
materialization window, and online reads ignore it. Anything the model must see "aged" is therefore computed in the
on-demand view from the value's own timestamp, so training and serving age it the same way.

| View | TTL | Why |
|---|---|---|
| daily stats | none (`timedelta(0)`) | the newest row is the card's history however old it is; a card idle for 5 months still has a 180-day dispute count. Its age is a feature (`days_since_last_active`, from `stats_ts`) instead of a cut-off |
| card profile | none | issue date, type, network never change |
| realtime | 1 h, the longest window | the join drops older rows (NULL). Online the stored row stays, so the on-demand view turns a value older than its own window (10 min or 1 h, from `rt_ts`) into 0 and NULL into 0: "no authorization in the window". Both paths give the model the same number |
| card device | none | "this card has used this device before" never expires |

The ageing in the on-demand view, from each value's own timestamp
([`ml/feature_repo/fraud_features.py` L103-L111](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/feature_repo/fraud_features.py#L103-L111)):

```python
    # Realtime values were computed at the card's previous authorization: "now" they still hold only within their
    # window; past it there was no authorization in that window, i.e. 0. (Inside it, an upper bound: the oldest
    # authorizations may have slid out since.)
    age_s = (now - utc(df["rt_ts"])).dt.total_seconds()
    for name, window_s in (("auth_count_10m", 600), ("amount_sum_1h", 3600), ("distinct_merchants_1h", 3600),
                           ("distinct_units_1h", 3600)):
        fresh = age_s.notna() & (age_s <= window_s)
        out[f"{name}_now"] = df[name].where(fresh, 0).fillna(0).astype("int64")
    out["days_since_last_active"] = ((now - utc(df["stats_ts"])).dt.total_seconds() / 86400).astype("float64")
```

## Train / serve consistency

Three decisions keep the training rows identical to what the API sees:

1. Entity timestamp = payment time - 1 ms. When the API scores a payment, Flink has not processed it yet: the
   online store holds features of the *previous* authorizations. Training joins at `txn_ts - 1 ms`
   ([`ml/fraud_ml/data.py` L88-L91](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/fraud_ml/data.py#L88-L91)),
   so the point-in-time join also excludes the payment itself.
2. DP3 rows land the night after the activity. A row for day L has `event_timestamp = L + 1` and windows that
   end at L
   ([`pipelines/spark/build_features.py` L69-L70](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/pipelines/spark/build_features.py#L69-L70)).
   A payment on day L+1 sees the stats up to L, which is what Redis holds that day (materialized after DP3 of L).
   Layer 1 wrote one row on each day D the card *paid*, windows ending at D-1: that row existed only because of the
   payment being scored. For a card that last paid 20 days ago, training saw windows ending yesterday; serving has
   the row of 20 days ago, whose 30-day window covers other days.
3. Streaming features for history come from replaying history (Kappa). The full authorization history is
   replayed through the same Flink jobs and push job B, so the offline rows were produced by the code that produces
   the online ones, and there is no second implementation in Spark to keep equal.

## Pipelines

| What | How | When |
|---|---|---|
| Daily stats to Redis | DAG `feast_materialize` ([`pipelines/airflow/dags/feast_materialize.py` L20-L29](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/pipelines/airflow/dags/feast_materialize.py#L20-L29), [run of 2026-11-03](#materialize-run-of-2026-11-03)): `feast materialize-incremental <ds+1>` then `ml/tools/check_online.py` (sample of cards active on ds read back from Redis, values compared with Delta) | triggered by DP3 |
| Streaming to Redis | push job A (`ml/jobs/push_job.py --to online`), consumer group per job, offsets committed after the write | continuous |
| Streaming to Delta | push job B (`--to offline`), `deltalake` append (Feast's Spark offline push appends plain Parquet next to the Delta log, which Delta readers ignore) | continuous; `--until-idle` for the backfill |
| Definitions | `feast apply` from `ml/feature_repo/` | after a change |
| Bootstrap | `feast materialize 2000-01-01T00:00:00 <ds+1>T00:00:00` once: the first `materialize-incremental` of a view without TTL only looks back a limited window, which would skip `card_profile` rows (stamped at the card's issue date) and cards idle for a long time | once, before the first daily run |

Both push jobs write a batch first and commit the Kafka offsets after it (at-least-once: after a crash the last
batch is written again)
([`ml/jobs/push_job.py` L113-L122](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/jobs/push_job.py#L113-L122)):

```python
        if full or (now - last_flush >= max_wait_s and any(pending.values())):
            for topic, values in pending.items():
                if values:
                    source, schema = TOPICS[topic]
                    sink.write(source, to_frame(values, schema), schema)
                    written += len(values)
            consumer.commit(asynchronous=False)
            print(f"{group}: {written:,} rows written", flush=True)
            pending = {t: [] for t in TOPICS}
            last_flush = now
```

The two push jobs
([`ml/jobs/push_job.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/jobs/push_job.py))
are deployed by their own CI pipeline
([`ml/jobs/Jenkinsfile.stream`](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/jobs/Jenkinsfile.stream), [cicd.md](cicd.md#stream-feature-jobs)):
unit tests, the ml image, `push-online` and `push-offline` recreated on it, then a smoke test that sends one marked
message to Kafka and waits for it in both stores. Build #2: `ci_smoke_fabe0b34: online 12.4 s, offline 8.4 s
(s3://lakehouse/gold/feat_card_auth_rt)`. The Jenkins stage view of that build:

![Jenkins stage view of stream-feature-jobs: build #2 ran tests, image, deploy and the online + offline smoke test](images/cicd_stream-feature-jobs.png)

### Materialize run of 2026-11-03

The run for ds 2026-11-03 (triggered by DP3): `materialize-incremental` to 2026-11-04, then `check_online` over
200 cards active that day: 0 missing online, 0 stale. In Airflow, both tasks of the run succeeded:

![Airflow feast_materialize, run of 2026-11-03: materialize_incremental, then check_online, both success](images/airflow_feast_materialize.png)

## Kappa backfill (history of the streaming features)

The streaming features of the whole history are rebuilt from the start of the topic, Flink first and the push jobs
after it:

```bash
# empty topics card_authorizations / card_features_rt / card_device_seen, landing, bronze.raw_card_authorizations
python generator/producer.py --topic card_authorizations --speed 0            # 8,119,499 events, in 4 chunks (RAM)
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/rt_features.py --topic card_authorizations --parallelism 4
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/device_seen.py --topic card_authorizations
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/raw_landing.py
docker compose -f infra/docker-compose.yaml --profile ml up -d push-online push-offline   # after Flink: catch up
```

Result: 8,119,499 authorizations gave 7,999,526 feature rows (the 119,973 = 1.48% network retries are dropped by the
dedup) and 214,808 first (card, device) pairs. The producer needs 111 s. Flink is the slow part, and it took three
runs to reach a steady rate:

| Run | Setting | Rate of `rt_features` |
|---|---|---|
| 1 | 4 slots, parallelism 1, Python state cache 1,000 keys (default) | 188 events/s (12 h) |
| 2 | 8 slots (4 GB TaskManager), parallelism 5, state cache 200k keys | 1,020 events/s for 2 minutes, then the 8 GB Docker VM swapped (91 MB free, 2.2 GB swap) and it nearly stopped |
| 3 | 6 slots (3.5 GB), parallelism 4, state cache 30k keys, push jobs started only after Flink | **~870 events/s, steady** (~2.5 h) |

Each element of a PyFlink function crosses to a Python worker, and one worker uses one core, so the job scales with
parallelism (state is keyed by card) until memory runs out: every slot adds a Python process (~0.3 GB). The state
cache only needs to hold the keys of one worker (130k cards / 4). Bounded runs do not commit Kafka offsets: before
any `--live` run, move the `rt_features` and `device_seen` groups to the end of the topic, or live mode replays the
history again.

## Gotchas

- Live Flink jobs need checkpoint storage. Bounded backfill runs never checkpoint; the live `rt_features`
  checkpoints every 10 s (offsets are committed on checkpoints), and by default into JobManager memory, capped at
  5 MB. Its state (card histories + dedup ids) passed that during the replay: `Size of the state is larger than the
  maximum permitted memory-backed state. Size=5380688, maxSize=5242880`, 25 failed checkpoints, then a restart loop
  that looked like a slow job (lag 24k events). `execution.checkpointing.storage: filesystem` with
  `execution.checkpointing.dir: s3://lakehouse/flink/checkpoints` in the cluster config
  ([`infra/docker-compose.yaml` L313-L316](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/infra/docker-compose.yaml#L313-L316))
  fixed it: checkpoints of ~2 MB on S3, ~270 events/s.
- `raw_landing.py` always started at the earliest offset, so landing the live period would have landed the 8.1M
  history events again; `--from-ms` starts at a Kafka timestamp instead (the producers stamp event time).
- `SparkSource(file_format=...)` accepts only csv / json / parquet / avro: a Delta table is
  `file_format="parquet", table_format=DeltaFormat()`
  ([`ml/feature_repo/fraud_features.py` L39-L43](https://github.com/cvinhnguyen/card-fraud-platform/blob/1416656ca28dbf52a0663508c5f0a81fb869faed/ml/feature_repo/fraud_features.py#L39-L43)).
- The on-demand view runs from the source text stored in the registry: module-level helpers and imports are not
  there at run time (`NameError`). The function is self-contained.
- `feast apply` infers schemas by reading the sources: the push-source Delta tables must exist first (push job B
  creates them empty on its first run).
- `${VAR:-default}` is not expanded in `feature_store.yaml`, only `${VAR}`.
- Feast 0.66's local materialization engine pulls every row of the window and keeps the latest per key in
  pandas unless `materialization: pull_latest_features: true`: the 7.6M daily-stats rows took the Python process to
  5.7 GB and the kernel killed it. With the flag Spark returns one row per card; the bootstrap took 52 s.
