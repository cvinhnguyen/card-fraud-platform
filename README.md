# Card Fraud Platform

Real-time card fraud detection platform for a Vietnamese digital bank — EDAI K11 coursework.
Layer 1: Data Platform. Layer 2: ML Platform.

## Table of contents
- [Business domain](#business-domain)
- [Architecture](#architecture)
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
([docs/data_characteristics.md](docs/data_characteristics.md)). Layer 1 (this repo) is the data platform; Layer 2
(feature store, training, serving) consumes its `feat_*` tables and the realtime features.

## Architecture

![Layer 1 overview](docs/images/layer1-overview.png)

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

Deployment by compose profile (`python docs/diagrams/deployment.py`, needs Graphviz):

![Layer 1 deployment](docs/images/layer1-deployment.png)

Diagram sources: `docs/diagrams/layer1-overview.html` (open in a browser) and `docs/diagrams/deployment.py`.

## Repo structure

```
infra/        docker-compose (profiles), secrets bootstrap, bucket/topic init, configs (conf/)
  spark/ flink/ airflow/ hive/   the four images built here (multi-stage, see docs/docker_optimization.md)
generator/    synthetic source data + injected data problems, and the checks that prove them
spark/        Spark jobs (mounted into the Spark containers at /opt/jobs)
dbt/          dbt-spark project: gold models and tests
sql/          Postgres serving SQL (indexes + EXPLAIN)
flink/        PyFlink jobs (mounted into the Flink containers at /opt/flink/jobs) and their check scripts
airflow/      DAGs DP1-DP3 + DataHub metadata ingestion (dags/), connections/variables bootstrap (setup_airflow.py)
docs/         one file per major part
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
.venv/Scripts/dbt build --project-dir dbt --profiles-dir dbt
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_features.py   # DP3, reads gold

# P6 streaming: Flink session cluster (UI http://127.0.0.1:8088), each check replays a slice and verifies the job
docker compose -f infra/docker-compose.yaml --profile core --profile stream up -d --build
.venv/Scripts/python flink/check_windows.py --replay-from "2026-06-01 00:00" --limit 20000

# P7 + P8: Airflow runs DP1 -> DP2 -> DP3; DataHub receives lineage (OpenLineage) and validation results
docker compose -f infra/docker-compose.yaml --profile core --profile batch --profile orch --profile gov up -d --build
docker exec airflow-scheduler airflow dags backfill dp1_source_to_bronze -s 2026-10-12 -e 2026-10-15   # DP2/DP3 follow
```

UIs: Spark master http://127.0.0.1:8080, running job http://127.0.0.1:4040, finished jobs http://127.0.0.1:18080, Trino http://127.0.0.1:8090, Flink http://127.0.0.1:8088, Airflow http://127.0.0.1:8082 (user `admin`), DataHub http://127.0.0.1:9002 (user `datahub`); passwords are in `infra/.env`. The 8 GB Docker VM does not hold every profile at once: run `stream` on its own, and keep `trino` and the Thrift Server (`dbt` profile) off while `orch` + `gov` run. From Git Bash, prefix `docker compose exec` with `MSYS_NO_PATHCONV=1` so `/opt/...` paths are not rewritten.

Data volume is set in `generator/config.yaml` (`volume`, ~1 GB: about 7 min to generate, `check_data.py` needs ~4.3 GB RAM). For a quick smoke test: `generate.py --scale 0.05`. What the data contains and the measured problems: [docs/data_characteristics.md](docs/data_characteristics.md).

## Docs
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
