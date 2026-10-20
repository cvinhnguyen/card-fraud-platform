# Card Fraud Platform

Real-time card fraud detection for a Vietnamese digital bank, built as the EDAI K11 coursework in two layers: a
data platform (Layer 1) and an ML platform (Layer 2).

## Table of contents
- [Business domain](#business-domain)
- [Architecture](#architecture)
  - [Layer 1: data platform](#layer-1-data-platform) · [Layer 2: ML platform](#layer-2-ml-platform)
  - [System diagram](#system-diagram) · [Deployment](#deployment)
- [Repo structure](#repo-structure)
- [Quick start](#quick-start)
- [Docs](#docs)

## Business domain

A Vietnamese digital bank issues debit and credit cards and runs a mobile banking app. Fraud shows up as card payments
the cardholder did not make (mostly card-not-present, often abroad) and as account takeovers in the app (password
reset, new device, new payee, transfer). The bank needs to score each card authorization in real time and to train
and monitor the model on history, which means a data platform that brings together data owned by other departments:

| Source | Owner | How it arrives | Problems it brings |
|---|---|---|---|
| Customers, accounts, cards, merchants, disputes | core banking (Postgres) | DP1 reads it over JDBC | SCD2 history, province names before / after the 2025-07-01 reorganization (63 -> 34 units) |
| Settled transactions | core banking nightly export (files on S3) | DP1 loads the new export folders | 2% duplicates from export re-runs, auth columns added on 2024-07-01 (schema evolution), merchant and city skew |
| Card authorizations | card network | Kafka `card_authorizations` | late events from offline POS terminals, network-retry duplicates |
| App events | mobile app | Kafka `app_events` | events out of order within a session |
| Fraud labels | disputes, reported days to weeks later | DP2 (`gold.label_transaction`) | label delay |

All data is synthetic (`generator/`), with every problem injected at a measured rate and checked
([docs/data_characteristics.md](docs/data_characteristics.md)). Layer 1 is the data platform; Layer 2 (feature
store, training, serving, drift) consumes its `feat_*` tables and the realtime features.

## Architecture

![Platform overview: Layer 1 builds the features, Feast hands them to Layer 2, which trains, serves and monitors the model](docs/images/platform-overview.png)

Layer 1 turns the source systems into feature tables (batch) and realtime features (stream). Feast is the hand-over:
the same 20 features for training (offline store, point-in-time joins) and for serving (online store). Layer 2 trains
on them, serves the model behind the Scoring API and retrains when the drift check fails. The numbers are the steps
of the tables below.

### Layer 1: data platform

| Step | Component | What it does |
|---|---|---|
| 01 · DP1 | Spark ingest | Copies `core_banking` tables (JDBC) and the daily export files (S3A) into bronze `raw_*` Delta tables, unchanged |
| 02 · DP2 | Spark silver | Dedup, `mergeSchema` for the 2024-07-01 auth columns, 63 → 34 unit normalization, skew salting → `stg_*` |
| 03 · DP2 | dbt-spark (novel idea 1) | Gold `dim_*` / `fact_*` on Spark (own session in Airflow, Thrift Server by hand), SCD2 `dim_customer` model, incremental fact, 22 dbt tests |
| 04 · DP3 | Spark features | `feat_*` tables with `event_timestamp` and `created` for the Layer 2 feature store |
| 05 | Trino | SQL over gold and feature tables through the Hive Metastore |
| 06 | Flink | Watermarks, late events, dedup and windows on Kafka; raw events → bronze, realtime features → `card_features_rt` |
| 07 | Airflow | Runs DP1 → DP2 → DP3 (each: ingest stage + Great Expectations validate stage); connections and variables in Airflow |
| 08 | DataHub | Tables of every zone, lineage sent by every Spark job through OpenLineage (novel idea 2), validation results as assertions |

### Layer 2: ML platform

| Step | Component | What it does |
|---|---|---|
| 09 | Feast (`ml/feature_repo/`) | Offline store = the lakehouse Delta tables (Spark), online store = Redis; daily stats materialized by Airflow, realtime and device features pushed from Kafka (`ml/jobs/push_job.py`) |
| 10 | Notebook | The same 7 steps the pipeline runs, one by one, ending with both models saved and an MLflow run (`ml/notebooks/fraud_model.ipynb`) |
| 11 | Kubeflow Pipelines on kind | `train_fraud_model`: labels -> Feast point-in-time features -> data checks -> time split -> distributed XGBoost (Spark executor pods) -> evaluation gate -> MLflow registry |
| 12 | MLflow | Experiment runs, data versions, model registry with aliases `challenger` / `production` |
| 13 | KServe | InferenceService (Standard mode) serving the `production` booster baked into the image |
| 14 | Fraud Scoring API | FastAPI: online features from Feast + KServe score -> approve / step_up / decline, with a rule fallback |
| 15 | Drift API + DAG | Logs every scored request, PSI per feature against the training profile; Airflow `drift_monitoring` triggers a retraining run in KFP |

Five classes carry the ML logic:

| Class | File | Job |
|---|---|---|
| `TrainingDataService` | `ml/fraud_ml/data.py` | Labels as of a date, Feast point-in-time join, versioned training set (Delta) |
| `FraudModelTrainer` | `ml/fraud_ml/train.py` | Logistic-regression baseline + distributed `SparkXGBClassifier` |
| `ModelEvaluator` | `ml/fraud_ml/evaluate.py` | Thresholds on validation, test metrics, gate vs baseline and `production` |
| `ModelRegistry` | `ml/fraud_ml/registry.py` | MLflow logging, model versions and aliases |
| `FraudScorer` | `services/scoring_api/app/scorer.py` | Request -> features -> score -> decision, fallback when KServe is down |

### System diagram

![System architecture: deployable units and the numbered data flows between them](docs/images/platform-architecture.png)

Every box is a deployable unit: a compose container or a workload on the kind cluster (Feast is a library inside
them, so it has no box). Arrows follow the data and carry it as their label; a call without data (B1, M7) points from
caller to callee. Each flow has its own colour, letter and numbering:

| Flow | Who / when | Path |
|---|---|---|
| **S** Payment scoring | end user, every authorization | card network -> Scoring API (features from Redis, score from KServe) -> decision; every decision logged to Kafka |
| **R** Streaming features | continuous | Kafka -> Flink -> Kafka -> push jobs -> Redis (online) and Delta (offline) |
| **B** Batch data | daily, Airflow | Postgres + export files -> Spark -> bronze -> silver -> gold -> feat_* -> Feast materialize -> Redis |
| **T** Training and deploy | ML engineer | lakehouse -> notebook / Kubeflow pipeline -> MLflow -> predictor image -> KServe |
| **M** Monitoring and retraining | live + daily | prediction log -> Drift API (PSI); drift DAG (PR-AUC, PSI) -> MLflow -> retraining run in KFP |

Every numbered arrow, one line each: [docs/architecture.md](docs/architecture.md#numbered-arrows).

### Deployment

![Deployment: host tools, the Docker Desktop VM with seven compose profiles, and the kind cluster for Kubeflow Pipelines and KServe](docs/images/platform-deployment.png)

Everything runs on one machine: the compose profiles start what a phase needs, and the kind cluster (joined to the
compose network) holds only the two pieces that need Kubernetes, Kubeflow Pipelines and KServe. Diagram sources:
`python docs/diagrams/platform.py` (stdlib only; PNG through headless Chrome; SVG next to each PNG).

## Repo structure

Grouped by role: data pipelines (Layer 1), ML code (Layer 2), online services, infrastructure. Repo paths differ from
the paths inside the containers (`pipelines/spark/` is `/opt/jobs`, `ml/` is `/opt/ml` in the ML image).

```
pipelines/            Layer 1 data pipelines
  airflow/            DAGs (DP1-DP3, DataHub ingestion, Feast materialization, drift monitoring), connections/variables bootstrap
  spark/              Spark jobs (mounted at /opt/jobs), benchmarks (skew, storage layout, 100 GB)
  dbt/                dbt-spark project: gold models and tests
  flink/              PyFlink jobs (mounted at /opt/flink/jobs) and their check scripts
ml/                   Layer 2 ML code (Dockerfile = the fraud/ml-train code image)
  feature_repo/       Feast repo: entities, sources, feature views, on-demand features, feature services
  fraud_ml/           library: the 5 classes above and the 7 pipeline steps
  pipelines/          Kubeflow pipeline train_fraud_model (.py) and its compiled .yaml
  jobs/               long-running / scheduled: push jobs, drift job, drift forwarder
  tools/              run by hand: replay_scoring, check_online, drift_by_day
  notebooks/          fraud_model.ipynb, executed with outputs (models/ = the saved models, gitignored)
services/             scoring_api (FastAPI), drift_api (FastAPI), predictor (KServe custom model)
generator/            synthetic source systems: data with injected problems, Kafka producer, the checks that prove them
infra/                docker-compose (profiles), secrets bootstrap (make_env.py, .env.example), bucket/topic init
  spark/ flink/ airflow/ hive/ ml/ mlflow/   the images built here (multi-stage, see docs/docker_optimization.md)
  k8s/                kind cluster, KFP + KServe install, namespaces, RBAC, secrets, the KServe InferenceService
  conf/               service configs (Spark, Hive, Trino, SeaweedFS, DataHub, notebook)
  sql/                Postgres serving SQL (indexes + EXPLAIN)
docs/                 one file per major part; diagrams/ = the scripts that draw the pictures (platform.py: the 3 above)
```

## Quick start

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt
.venv/Scripts/python infra/make_env.py                        # once: random local secrets (gitignored)
docker compose -f infra/docker-compose.yaml --profile core up -d
.venv/Scripts/python infra/init_infra.py                      # buckets + Kafka topics
.venv/Scripts/python generator/generate.py                    # source data -> data/source
.venv/Scripts/python generator/check_data.py                  # assert every injected problem
.venv/Scripts/python generator/load_source.py                 # Postgres core_banking (127.0.0.1:5433) + s3://core-banking-export
.venv/Scripts/python generator/producer.py --speed 200            # replay card_authorizations into Kafka (--topic app_events, --replay-from, --limit)

# P3: Spark + Hive Metastore + Trino, then DP1 (source -> bronze Delta tables)
docker compose -f infra/docker-compose.yaml --profile core --profile batch up -d --build
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/ingest_bronze.py
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_silver.py
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py
docker exec -it trino trino --execute "SELECT count(*) FROM delta.bronze.raw_transactions"

# P4 gold: dbt-spark through the Spark Thrift Server
docker compose -f infra/docker-compose.yaml --profile dbt up -d spark-thrift
.venv/Scripts/dbt build --project-dir pipelines/dbt --profiles-dir pipelines/dbt
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_features.py   # DP3, reads gold

# P6 streaming: Flink session cluster (UI http://127.0.0.1:8088), each check replays a slice and verifies the job
docker compose -f infra/docker-compose.yaml --profile core --profile stream up -d --build
.venv/Scripts/python pipelines/flink/check_windows.py --replay-from "2026-06-01 00:00" --limit 20000

# P7 + P8: Airflow runs DP1 -> DP2 -> DP3; DataHub receives lineage (OpenLineage) and validation results
docker compose -f infra/docker-compose.yaml --profile core --profile batch --profile orch --profile gov up -d --build
docker exec airflow-scheduler airflow dags backfill dp1_source_to_bronze -s 2026-10-12 -e 2026-10-15   # DP2/DP3 follow

# Layer 2: Feast, MLflow, notebook, APIs (profile ml); KFP + KServe on kind
docker build -f ml/Dockerfile -t fraud/ml-train:latest .           # code image: notebook, push jobs, KFP steps
docker compose -f infra/docker-compose.yaml --profile core --profile ml up -d
docker compose -f infra/docker-compose.yaml exec notebook feast -c /opt/ml/feature_repo apply   # order + bootstrap: docs/feature_store.md
bash infra/k8s/up.sh && bash infra/k8s/platform.sh                 # kind cluster, KFP 2.17, KServe 0.21
python ml/pipelines/train_fraud_model.py --run --label-as-of 2026-10-15                # see docs/ml_training.md
```

UIs: Spark master http://127.0.0.1:8080, running job http://127.0.0.1:4040, finished jobs http://127.0.0.1:18080, Trino http://127.0.0.1:8090, Flink http://127.0.0.1:8088, Airflow http://127.0.0.1:8082 (user `admin`), DataHub http://127.0.0.1:9002 (user `datahub`), MLflow http://127.0.0.1:5000, Jupyter http://127.0.0.1:8899 (token `JUPYTER_TOKEN`), KFP http://127.0.0.1:31888, Scoring API http://127.0.0.1:8000/docs, Drift API http://127.0.0.1:8001/docs; passwords are in `infra/.env`. The 8 GB Docker VM does not hold every profile at once: run `stream` on its own, and keep `trino` and the Thrift Server (`dbt` profile) off while `orch` + `gov` run. From Git Bash, prefix `docker compose exec` with `MSYS_NO_PATHCONV=1` so `/opt/...` paths are not rewritten.

Data volume is set in `generator/config.yaml` (`volume`, ~1 GB: about 7 min to generate, `check_data.py` needs ~4.3 GB RAM). For a quick smoke test: `generate.py --scale 0.05`. What the data contains and the measured problems: [docs/data_characteristics.md](docs/data_characteristics.md).

## Docs
- [Architecture](docs/architecture.md): overview, system diagram with every numbered arrow, deployment.
- [Data characteristics](docs/data_characteristics.md): datasets, injected problems, measured numbers.
- [Schema design](docs/schema_design.md): zones, naming, grain of every table, gold ER diagram.
- [Spark jobs](docs/spark_jobs.md): DP1 bronze ingest (schema evolution demos), DP2 silver (dedup, unit normalization, HCMC chart), dbt gold star schema, labels with label delay, DP3 features.
- [Spark optimization](docs/spark_optimization.md): merchant join skew, baseline vs salting vs AQE vs broadcast, skew level sweep.
- [Flink jobs](docs/flink_jobs.md): late events, duplicates, realtime features, out-of-order sessions, raw events to bronze; each checked against the Parquet truth.
- [Storage optimization](docs/storage_optimization.md): small files, partitioning, `OPTIMIZE` + Z-order on Delta; B-tree and BRIN indexes on the Postgres serving table.
- [Airflow](docs/airflow.md): DP1 / DP2 / DP3 DAGs, connections and variables in Airflow, validate stages (GE on Spark), backfill.
- [DataHub](docs/datahub.md): tables of every zone, lineage from source to features, which DAG produced each table, validation results.
- [Novel idea 1: dbt-spark](docs/novel_dbt.md): gold star schema in dbt, compared row by row with the same layer written by hand in PySpark.
- [Novel idea 2: OpenLineage](docs/novel_openlineage.md): lineage from every Spark job without a line of lineage code.
- [Docker optimization](docs/docker_optimization.md) and the [100 GB run](docs/scale_100g.md).
- Layer 2: [Feature store](docs/feature_store.md), [ML training](docs/ml_training.md), [Serving](docs/serving.md), [Drift](docs/drift.md), [Testing](docs/testing.md).
