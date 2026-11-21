# Drift: simulation, detection, retraining

A fraud model loses accuracy when fraud or customer behaviour changes, so Layer 2 runs two checks. The Drift API
measures input drift on every scored payment as a PSI (population stability index: how far a feature's distribution
moved from training); a daily Airflow DAG scores the model against ground-truth labels and starts retraining on
Kubeflow Pipelines (KFP) when it degrades. Labels arrive weeks late, so only the live check is immediate and only the
daily one measures accuracy. A generated month-long live period with data and concept drift tests both.

| | |
|---|---|
| Code | [`generator/generate_drift.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/generator/generate_drift.py), [`services/drift_api/`](https://github.com/cvinhnguyen/card-fraud-platform/tree/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/drift_api), [`infra/k8s/kserve-card-fraud.yaml`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/infra/k8s/kserve-card-fraud.yaml) (payload logger, Broker, Trigger), [`ml/jobs/drift_job.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/ml/jobs/drift_job.py), [`pipelines/airflow/dags/drift_monitoring.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/pipelines/airflow/dags/drift_monitoring.py) |
| Runs on | Drift API: compose service `drift-api` (profile `ml`, port 8002), fed through Knative Eventing in kind (Broker `default` and Trigger `drift-detector` in namespace `serving`); daily check: Airflow DAG `drift_monitoring` (compose profile `orch`); retraining: Kubeflow pipeline `train_fraud_model` in kind |
| Reads | every request and response of InferenceService `card-fraud`, as CloudEvents from the KServe payload logger; the production model's `reference_profile.json`; ground truth `s3://lakehouse/ml/ground_truth/labels.parquet`; Feast offline store |
| Writes | `data/drift/` (generator); live histograms in Redis (`drift:<minute>:<input>`); gauges `drift_psi{input}`; MLflow experiment `card_fraud_monitoring`; KFP runs `drift-retrain-<ds>` |
| Related | [serving.md](serving.md), [ml_training.md](ml_training.md), [feature_store.md](feature_store.md), [airflow.md](../data-platform/airflow.md) |

## Where it fits

Drift detection is the monitoring of Layer 2, the brown M flow of the
[overview](../architecture.md#layer-2-the-ml-platform-right-panel):

- Live input: every model request and response ([serving.md](serving.md)), against the production model's profile.
- Daily input: ground-truth labels and Feast offline features ([feature_store.md](feature_store.md)).
- Output: a PSI per input (endpoint, Prometheus gauges, MLflow) and, on drift, a training run
  ([ml_training.md](ml_training.md)).

## How it works

1. The generator extends the 2-year history with a month-long live period: 10 normal days, then concept drift
   (fraud moves to the victim's own phone) and a 4-day sale season (data drift) ([The live period](#the-live-period)).
2. Live: KServe's payload logger sends every model call through a Knative Broker to the Drift API, which keeps
   per-minute Redis histograms per input; `GET /v1/drift` returns each input's PSI over the last 15 minutes, drift
   above 0.25 ([Real-time detection](#real-time-detection-drift-api)).
3. Daily: DAG `drift_monitoring` scores the last 7 days against the labels and logs PR-AUC (area under the
   precision-recall curve), recall at 1% FPR (false positive rate) and each input's PSI to MLflow
   ([Daily check with labels](#daily-check-with-labels)).
4. Retrain: a PSI above 0.25 or a PR-AUC drop of more than 0.05 starts a training run; people decide what the new
   model serves ([What happens to the new model](#what-happens-to-the-new-model)).

## Run it

Prerequisites: the production model in MLflow, the `ml` profile, and Airflow (`orch`) for the daily check.
`infra/k8s/platform.sh` installs Knative Eventing; the Broker and Trigger come with the model server
(`kserve-card-fraud.yaml`, [serving.md](serving.md)).

```bash
python generator/generate_drift.py --upload                      # live period -> data/drift/; export + labels to S3
MLFLOW_TRACKING_URI=http://127.0.0.1:5000 python services/predictor/fetch_model.py --alias production   # model + reference_profile.json, before drift-api starts
docker compose -f infra/docker-compose.yaml --profile ml up -d drift-api
kubectl get trigger drift-detector -n serving                     # READY True, subscriber http://drift-api:8000/...
curl -s http://127.0.0.1:8002/v1/drift                            # PSI per input over the last 15 minutes
docker exec airflow-scheduler airflow dags test drift_monitoring <ds>   # daily check with labels on the 7 days up to ds
```

Replace `<ds>` with the day to check (`YYYY-MM-DD`).

## The live period

The 2-year history ends on 2026-10-15; the generator continues the same world (customers, cards, merchants,
devices) by replaying `world()` with the same seeds ([`generate_drift.py`
L33-L44](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/generator/generate_drift.py#L33-L44)).
The live period covers 2026-10-16 .. 2026-11-15:

| Phase | Days | Kind | What changes |
|---|---|---|---|
| normal | 10-16 .. 10-25 | none | same as history |
| `fraud_shift` | from 10-26 | concept drift | fraud rings pay from the victim's own phone (malware): 60% of ecom frauds on a device the card already knows, so `is_new_device`, the model's second strongest feature by gain (after `is_otp`), fades; frauds lean to large night payments; fraud rate 0.3% -> 0.5% |
| `sale_season` | 11-09 .. 11-12 (11/11) | data drift | volume x2, online merchants' weight x2, amounts x1.6: the inputs move, fraud behaviour does not |

The `drift:` block of `generator/config.yaml` defines it all
([config.yaml L97-L112](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/generator/config.yaml#L97-L112)):

```yaml
drift:                           # Layer 2: live period after the training data (generator/generate_drift.py)
  start: 2026-10-16
  end: 2026-11-15
  daily_transactions: 8000       # the 2026 daily volume of the training period
  phases:                        # a day takes every phase whose [from, to] covers it (no `to` = until end)
    - name: sale_season          # 11/11 shopping festival (data drift: volume, channel mix, amounts)
      from: 2026-11-09
      to: 2026-11-12
      volume_scale: 2.0
      online_weight: 2.0         # online merchants' share of payments x2 (renormalized)
      amount_scale: 1.6
    - name: fraud_shift          # fraud rings switch to large night payments from the victim's own phone (malware):
      from: 2026-10-26           # concept drift, the "new device" signal fades
      fraud_rate: 0.005
      risk_weights: {ecom: 2.0, night: 5.0, high_amount: 4.0, foreign: 1.0}
      fraud_known_device_share: 0.6
```

Outputs in `data/drift/`
([`generate_drift.py` L8-L13](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/generator/generate_drift.py#L8-L13)):
the authorization stream (with retries and late events, as the history), the request fields per payment, the
core-banking export and the ground truth `labels/`, a `(transaction_id, is_fraud)` table of 279,915 rows that joins to
any table by `transaction_id`. `--upload` puts the export and labels on S3
(`s3://lakehouse/ml/ground_truth/labels.parquet`) for DP1, DP2 and DP3 ([airflow.md](../data-platform/airflow.md)) to
process like any other day. Labels joined to the export of 2026-11-03 (payments of 11-02; `is_fraud` = 1 is a fraud):

```text
  transaction_id  is_fraud       card_id              txn_ts  amount_vnd channel
txn_00010002127a         1 card_dbd353d5 2026-11-02 12:28:39      211000    ecom
txn_0001000212ca         1 card_50bf2cd5 2026-11-02 17:07:43      596000    ecom
txn_0001000211d5         0 card_a594c46e 2026-11-02 19:12:38      335000     pos
txn_0001000211d6         0 card_6aa9546c 2026-11-02 20:29:42       87000     pos
```

Generated 2026-10-19 (13 s, 279,915 payments):

| Phase | Days | Payments / day | Ecom share | Avg amount (VND) | Fraud rate | Frauds | of which on a known device |
|---|---:|---:|---:|---:|---:|---:|---:|
| normal | 10 | 7,989 | 71.7% | 429,369 | 0.30% | 236 | 0 |
| `fraud_shift` | 17 | 7,982 | 72.2% | 430,354 | 0.52% | 705 | 383 |
| `sale_season` + `fraud_shift` | 4 | 16,083 | 83.6% | 651,068 | 0.53% | 343 | 201 |

`fraud_shift` days keep the normal inputs (ecom share, amounts) while the fraud rate almost doubles and over half of
the ecom frauds come from a known device: concept drift with no visible data drift. The sale days move every input.

## Real-time detection (Drift API)

The Drift API
([`services/drift_api/`](https://github.com/cvinhnguyen/card-fraud-platform/tree/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/drift_api))
gets the model's inputs and outputs from the model server itself, so scoring never waits on it. It is KServe's
standard detector setup: outlier, adversarial and drift detectors each hang off the same Broker.

### How events reach the Drift API

```mermaid
flowchart LR
    api["Scoring API (compose)"] -->|"POST :predict"| np["NodePort 31080"]
    np --> agent["KServe agent :9081 (sidecar)"] --> model["model :8080"]
    agent -.->|"CloudEvents, after the answer"| broker["Broker default (serving)"]
    broker --> trigger["Trigger drift-detector"] -->|"POST /v1/inference-events"| drift["Drift API (compose)"]
```

1. The Scoring API calls `http://fraud-control-plane:31080/v1/models/card-fraud:predict` with
   `{"instances": [{feature: value}]}`. NodePort `card-fraud-nodeport` (namespace `serving`) sends it to port 9081 of
   the predictor pod.
2. Port 9081 is the KServe agent, a sidecar added because the InferenceService has `spec.predictor.logger`
   (`mode: all`); it passes the call to the model on 8080.
3. In the background it then posts two CloudEvents to the Broker
   (`http://broker-ingress.knative-eventing.svc.cluster.local/serving/default`):
   `ce-type: org.kubeflow.serving.inference.request` with the request body and
   `ce-type: org.kubeflow.serving.inference.response` with the answer (`{"predictions": [score], "model_version": ...}`),
   both with the same `ce-id` and `ce-inferenceservicename: card-fraud`.
4. Broker `default` (channel-based, in-memory) passes them to Trigger `drift-detector`, which posts each `card-fraud`
   event to `http://drift-api:8000/v1/inference-events`, by name, since the kind node is on the compose network.
5. The Drift API reads the `ce-*` headers `ce-type` and `ce-id` and the JSON body: a request event counts each
   instance's features, a response event each score, with no join.

Endpoint rules
([`services/drift_api/app/main.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/drift_api/app/main.py)):

- Instances are dicts by feature name, so order does not matter. A positional list is read in the reference
  profile's order, which `fetch_model.py` checks against the model's at release; a wrong length is rejected.
- The answer is `202` with an empty body; the broker treats any other body as a failed reply and retries.
- Logged GET calls (the Scoring API's readiness check) have no `instances` or `predictions`: accepted, not counted.
- An unknown `ce-type` or a broken body gets `400` (not retried) and a log warning with the `ce-id`.
- `drift_events_total{type="request"|"response"}` counts the instances read per event kind.

How the counts become a PSI:

- Reference: the production model's `reference_profile.json`, the validation period's decile edges and row shares
  per bin for each input and the score, plus a `missing` bin so a feature that suddenly goes NULL (a broken join or a
  dead stream) drifts too.
  [`services/predictor/fetch_model.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/predictor/fetch_model.py)
  downloads it next to the model; run it before `drift-api` starts, because compose bind-mounts the file
  ([`infra/docker-compose.yaml`
  L779-L782](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/infra/docker-compose.yaml#L779-L782))
  and a missing file becomes an empty directory.
- Live: one Redis hash per minute and input (`drift:<minute>:<input>`, `HINCRBY bin 1`, TTL 6 h,
  [`services/drift_api/app/store.py`
  L24-L35](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/drift_api/app/store.py#L24-L35)).
  Replicas keep no state, so any number can count into the same buckets.
- `GET /v1/drift` sums the last 15 minutes and computes the PSI per input,
  `sum (live% - ref%) * ln(live% / ref%)`: < 0.1 stable, 0.1-0.25 moderate, > 0.25 major. Below `MIN_EVENTS` (100,
  ~10 per decile bin) an input is `too_few_events` and cannot raise `drift`; the first live run reported PSI 12 and
  `drift: true` from 9 events
  ([`main.py` L112-L126](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/drift_api/app/main.py#L112-L126)).
  The gauges `drift_psi{input}` follow every report.
- It counts what the model received and answered: on a Scoring API fallback for a slow model both events count, on
  a model failure only the request.

The PSI of one input, with a floor `EPS = 1e-4` for empty bins
([`services/drift_api/app/psi.py`
L24-L34](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/services/drift_api/app/psi.py#L24-L34)):

```python
def psi(reference: dict[str, float], live: dict[str, int]) -> float | None:
    """PSI of live counts against reference shares (same bin labels); None without live data."""
    total = sum(live.values())
    if total == 0:
        return None
    out = 0.0
    for b in set(reference) | set(live):
        e = max(reference.get(b, 0.0), EPS)
        a = max(live.get(b, 0) / total, EPS)
        out += (a - e) * math.log(a / e)
    return out
```

PSI on inputs cannot see concept drift: in `fraud_shift` the inputs barely move while the meaning of
`is_new_device` changes.

### Event feed run on 2026-11-12

Only `redis`, `ml-db`, `scoring-api` and `drift-api` were up (Kafka and Flink off, so no Kafka decision log). The first 150 authorizations of 2026-11-03 went to `POST /v1/score` one at a time, 10 per
second:

| Check | Result |
|---|---|
| Scoring API | 150 scored, 1 fallback (`model_timeout`) |
| KServe agent log | 309 x `Sent with status code 202` (150 pairs, plus 9 readiness checks) |
| Broker ingress and filter logs | no error lines |
| `drift_events_total` | `request` 150, `response` 150 |
| Rejected events | 0 |
| `GET /v1/drift` | a PSI for all 21 inputs, `live_count` 150 each |

These PSI values say nothing about the model: with Flink off the realtime `*_now` features were stale (PSI 14 to
16), and a day's first 150 payments all fall between 00:00 and 01:00 (`is_night` PSI 12). Request inputs such as
`amount` (0.09) and `is_ecom` (0.0001) stayed stable.

Cost of the feed, measured the same day:

- Knative Eventing: 9 pods in `knative-eventing`, 514 MiB of memory requests, about 460 MiB in use.
- Agent sidecar: 100m CPU and 100 MiB requested, about 70 MiB in use.
- Model latency, 300 calls in a row per port from the kind node: the agent adds about 1 ms at p50 (1.9 ms on 8080,
  2.9 ms on 9081) and 2.5 to 4.5 ms at p99; the Scoring API's model budget is 200 ms in compose.

### Replay run on 2026-10-19

The live days 10-16 .. 11-03 went through Kafka, Flink, Feast (online and offline) and raw landing; four were also
scored:

```bash
python ml/tools/replay_scoring.py --from-day 2026-10-18 --rate 30 --concurrency 8 --max-lag 300 \
    --score-days 2026-10-18,2026-10-19,2026-10-27,2026-11-03     # the other days go to Kafka only
python ml/tools/drift_by_day.py --days 2026-10-19,2026-10-27,2026-11-03
```

Scoring at 30 requests/s (one worker scores ~45/s, [serving.md](serving.md)) gave 32,017 decisions, 34 fallbacks.
The Drift API then read them from Kafka `fraud_predictions` through a forwarder job (now the payload logger). Its
15-minute window is wall-clock time, so
[`ml/tools/drift_by_day.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/ml/tools/drift_by_day.py)
regroups the decisions by payment day with the Drift API's binning and PSI code, and joins the ground truth.

The replay outruns the feature path
([`ml/tools/replay_scoring.py`
L5-L10](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/ml/tools/replay_scoring.py#L5-L10)):
a day plays in ~80 s (~1,000x), so the few seconds from Kafka through Flink to Redis are hours of replayed time.
A card's last authorizations are often not online yet, so the realtime `*_now` features read low and show PSI that
production would not; request features (amount, channel, ...), daily stats and the offline store are unaffected.

| Day | Phase | Payments | Frauds | Live PR-AUC | Largest input PSI |
|---|---|---:|---:|---:|---|
| 10-19 | normal | 8,021 | 22 | 0.902 | `days_since_last_active` 0.81; every other input < 0.03 |
| 11-03 | `fraud_shift` | 7,935 | 48 | 0.293 | `days_since_last_active` 4.88; every other input < 0.03 |

- Concept drift, as designed: 11-03's inputs look like 10-19's (PSI < 0.03 on amount, channel, device, realtime
  counts...), but the fraud rate doubles and PR-AUC falls from 0.90 to 0.29.
- `days_since_last_active` (`request_ts - stats_ts`) drifts more every day (0.81 on 10-19, day 4 of the live period;
  4.88 on 11-03, day 19), because DP3 did not run during the replay and the online daily stats stayed at 10-16. In
  production, an alert on this input means "the nightly DP3 / materialize did not run".
- The sale days (11-09 .. 11-12) were not replayed, so there is no data-drift example on request inputs.

10-18 and 10-27 are left out ([Gotchas](#gotchas)). The replay stopped at 11-03: the rubric asks for a drift API and
an Airflow drift pipeline on the offline store that triggers retraining, and the daily check uses the replayed normal
and `fraud_shift` phases.

## Daily check with labels

Concept drift needs labels. Every day DAG `drift_monitoring`
([`drift_monitoring.py`
L23-L48](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/pipelines/airflow/dags/drift_monitoring.py#L23-L48))
runs
[`drift_job.py`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/ml/jobs/drift_job.py)
on the last 7 days:

1. entity rows of the window = ground truth + request fields + device, `event_timestamp = txn_ts - 1 ms` (as
   training), point-in-time features from the Feast offline store with the production model's feature service;
2. scores them with the production booster (MLflow alias `production`);
3. PR-AUC, recall at 1% FPR and fraud rate on the window; PSI of every input and the score vs the model's reference;
4. one MLflow run per check in experiment `card_fraud_monitoring` (the history of drift and live performance);
5. branch: retrain when any PSI > 0.25 (`drift_psi_threshold`) or the PR-AUC fell more than 0.05
   (`drift_pr_auc_drop`) below the model's own test PR-AUC
   ([`drift_job.py`
   L72-L76](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/ml/jobs/drift_job.py#L72-L76));
   `trigger_retraining` starts the KFP pipeline `train_fraud_model` through the KFP API with `label_as_of` = the run's
   day.

Labels here are known at once (simulated data); in production they mature over 60 days, and the daily check lags
that much (PSI does not).

### What happens to the new model

A retrained model reaches real payments in four steps, two of them human
([serving.md](serving.md#model-release-pipeline)):

1. It gets the alias `challenger` only when it beats production on the same test period
   ([ml_training.md](ml_training.md#evaluation-and-promotion)).
2. The Jenkins job `fraud-predictor` deploys it as its own InferenceService with no traffic.
3. After a person approves the A/B test in Jenkins, 10% of the cards (sticky by `crc32(card_id)`) reach it; it
   uses its own thresholds.
4. A person promotes it by moving the MLflow alias `production`; Jenkins releases it and removes the challenger. A
   rollback moves the alias back.

### Running the live days through Airflow

The live period is dated after the machine clock (runs on 2026-10-19), and Airflow does not run a task whose
logical date is in the future (`RunnableExecDateDep`), so a DP1 backfill for 2026-11-03 deadlocked. Two settings fix
it without changing any schedule:

- `AIRFLOW__SCHEDULER__ALLOW_TRIGGER_IN_FUTURE` (compose,
  [`infra/docker-compose.yaml`
  L61-L65](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/infra/docker-compose.yaml#L61-L65))
  lets the scheduler run the *triggered* DAGs DP2, DP3 and `feast_materialize` (then `schedule=None`, started by DP1's
  and DP2's trigger tasks).
- The daily DAGs `dp1_source_to_bronze` and `drift_monitoring` run in-process as `airflow dags test <dag> <ds>`,
  recorded like any run.

One run for ds = 11-03 loads every live export up to it (every load is incremental). DP2, DP3 and
`feast_materialize` now run on Airflow dataset events, dated when the event happens and carrying the business day
([airflow.md](../data-platform/airflow.md)); the live days have not run that way yet. Commands of the runs below (old
layout):

```bash
docker exec airflow-scheduler airflow dags test dp1_source_to_bronze 2026-11-03   # then DP2 -> DP3 -> materialize (then: trigger tasks)
docker exec airflow-scheduler airflow dags test drift_monitoring 2026-10-22
docker exec airflow-scheduler airflow dags test drift_monitoring 2026-11-02
```

### Runs on 2026-10-19

After one DP1, DP2, DP3 and materialize chain for ds = 11-03 (16 minutes; `check_online`: 200 cards active on
11-03, 0 missing online, 0 stale), the DAG checked two windows:

| `ds` | Window | Rows | Fraud rate | PR-AUC (test 0.828) | recall @ 1% FPR | Worst input PSI | Branch |
|---|---|---:|---:|---:|---:|---|---|
| 2026-10-22 | 10-16 .. 10-22, normal | 56,025 | 0.31% | 0.869 | 0.966 | 0.019 `days_since_card_issue` | `no_drift` |
| 2026-11-02 | 10-27 .. 11-02, `fraud_shift` | 55,850 | 0.52% | 0.277 | 0.369 | 0.022 `days_since_card_issue` | `trigger_retraining` |

Same inputs (no PSI above 0.03), opposite verdicts: the PR-AUC drop alone decided, as concept drift should. MLflow
(`card_fraud_monitoring`; 11-02 twice, identical, the first try failed at the trigger) shows PR-AUC 0.87 for 10-22,
0.28 for 11-02, the fraud rate rising and amount's PSI near 0:

![MLflow card_fraud_monitoring: PR-AUC 0.87 for the 10-22 window, 0.28 for 11-02; fraud rate up; PSI of amount ~0](../images/mlflow_monitoring.png)

In Airflow, the 11-02 run's `decide` branched to `trigger_retraining` and skipped `no_drift` (the other column is
the 10-22 run); the scheduler was stopped to free memory for the KFP run, hence the banner:

![Airflow drift_monitoring 2026-11-02: compute_drift, then decide, then trigger_retraining](../images/airflow_drift_1102.png)

The 11-02 run submitted the KFP run `drift-retrain-2026-11-02` (`a7cfc35f`, `label_as_of` 2026-11-02)
([`drift_monitoring.py`
L39-L58](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/pipelines/airflow/dags/drift_monitoring.py#L39-L58)).
It failed in step 2 with `OutOfMemoryError: Java heap space` (a heap tuned to the previous run's peak,
[ml_training.md](ml_training.md#gotchas)). The resubmitted run (`ce0b680c`, same argument) passed after a KFP retry
of its evaluate step, which failed once on MLflow's presigned URLs (same Gotchas table):

| Test period of 11-02 (same rows for both) | Retrained challenger | Production v1 |
|---|---:|---:|
| PR-AUC | 0.883 | 0.844 |
| recall @ 1% FPR | 0.9275 | 0.9283 |

Result: `passed = False`. PR-AUC gained 0.039 (the gate asks for 0.005), but recall at 1% FPR fell by 0.0008 and the
rule allows no loss. A model that did not pass was then not registered; now it is kept as a version tagged
`passed=False` without an alias ([ml_training.md](ml_training.md#evaluation-and-promotion)). Only the minimum PR-AUC
gain is a pipeline parameter (`min_pr_auc_gain`); the recall rule is fixed in `ModelEvaluator.passed`, and a person
can still move `production` to a version that did not pass.

## Gotchas

On the replay:

| Problem | Effect | Fix |
|---|---|---|
| Live `rt_features` checkpointed into JobManager memory (5 MB cap) | checkpoints failed past 5 MB of state; restart loop, 24k events behind | checkpoint storage on S3 ([feature_store.md](feature_store.md)) |
| Scoring a day while Flink is behind | 10-27 scored with features ~3 days stale (`is_new_device` = 1 for every device of the last 3 days): its PR-AUC 0.34 mixes concept drift and staleness, so 11-03 is the clean shift day | `--max-lag`: a day starts only when the Flink groups are within 300 events of the topic end |
| A first try at 100/s kept producing after scoring fell behind; a benchmark later re-scored 10-16 | 10-18's first 10 hours were scored with their own authorization already online (`auth_count_10m_now` = 1 for 22%, 0.05% in reality) | 10-18 left out; the window also counted the benchmark, so tag test traffic and exclude it from drift statistics |

On Airflow:

- DP1 failed its own validation on the first live run: a manual run's interval ends on ds, so the export bound
  `data_interval_end` loaded folders up to ds while the validation (correctly) wanted `ds + 1`. Sensor and load now
  use `macros.ds_add(ds, 1)`, the validation's day in every kind of run
  ([`pipelines/airflow/dags/dp1_source_to_bronze.py`
  L47-L54](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/pipelines/airflow/dags/dp1_source_to_bronze.py#L47-L54)).
- `The key 'ds' in args is a part of kwargs and therefore reserved` from `trigger_retraining(host, ds)`: Airflow
  passes its context by name, so the parameter is now `label_as_of`.
- The drift job's Spark session needs the Hive metastore up: Feast reads tables by path, but the shared
  `spark-defaults.conf` sets the Hive catalog.

On the event feed:

- Calls work but nothing is logged: NodePort 31080 points at the model's 8080 (as before the logger existed) and
  skips the agent. Point it at 9081
  ([`infra/k8s/nodeports.yaml`](https://github.com/cvinhnguyen/card-fraud-platform/blob/54d21eddbf6e602ae13fed46a1a9e0ef265d23cd/infra/k8s/nodeports.yaml));
  KServe's own Service `card-fraud-predictor` moves to 9081 by itself.
- Events minutes late, some counted twice: the first endpoint answered `{"counted": n}`, a broken reply event to the
  broker, which retried every delivery up to 10 times with a delay doubling from 0.2 s. It now answers `202`.
- Two rejected events every 15 seconds: the Scoring API's readiness check, a logged GET. Now accepted, not counted.
- Benchmarks and manual tests show up in the drift numbers, since every predictor call counts: keep them off the
  production InferenceService while its numbers matter.

## Limits

- The label delay limits retraining. `label_as_of` 2026-11-02 uses only payments at least 60 days old (the maturity
  window): its test period ends 60 days before and its training period about 300 days before (two 60-day evaluation
  periods, each after a 60-day gap, `ml/fraud_ml/split.py`), so no `fraud_shift` payment (from 10-26) is in training
  yet. Retraining cannot fix this drift until the labels mature (or confirmed frauds are labelled faster than the
  60-day worst case); today the platform detects and reports it the week it starts.
- The event feed can lose events: the Broker's in-memory channel drops events in flight when its dispatcher
  restarts, and drops a down Drift API's events after the broker's retries (10, a few minutes in total), with no dead
  letter sink. A Kafka-backed broker is the upgrade if live counts must survive restarts. Kafka `fraud_predictions`
  stays complete, and `ml/tools/drift_by_day.py` can compute the PSI from it.
