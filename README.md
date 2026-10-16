# Card Fraud Platform

Real-time card fraud detection platform for a Vietnamese digital bank — EDAI K11 coursework.
Layer 1: Data Platform. Layer 2: ML Platform.

## Table of contents
- [Architecture](#architecture)
- [Repo structure](#repo-structure)
- [Quick start](#quick-start)
- [Docs](#docs)

## Architecture

![Layer 1 overview](docs/images/layer1-overview.png)

| Step | Component | What it does |
|---|---|---|
| 01 · DP1 | Spark ingest | Copies `core_banking` tables (JDBC) and the daily export files (S3A) into bronze `raw_*` Delta tables, unchanged |
| 02 · DP2 | Spark silver | Dedup, `mergeSchema` for the 2024-07-01 auth columns, 63 → 34 unit normalization, skew salting → `stg_*` |
| 03 · DP2 | dbt-spark (novel idea 1) | Gold `dim_*` / `fact_*` through Spark Thrift Server, SCD2 `dim_customer` as a dbt snapshot, dbt tests |
| 04 · DP3 | Spark features | `feat_*` tables with `event_timestamp` for the Layer 2 feature store |
| 05 | Trino | SQL over gold and feature tables through the Hive Metastore |
| 06 | Flink | Watermarks, late events, dedup and windows on Kafka; raw events → bronze, realtime features → `card_features_rt` |

Airflow runs DP1 → DP2 → DP3 with Great Expectations checks. Spark, Airflow and dbt send lineage to DataHub automatically through OpenLineage (novel idea 2).

Deployment by compose profile (`python docs/diagrams/deployment.py`, needs Graphviz):

![Layer 1 deployment](docs/images/layer1-deployment.png)

Diagram sources: `docs/diagrams/layer1-overview.html` (open in a browser) and `docs/diagrams/deployment.py`.

## Repo structure

```
infra/        docker-compose (profiles), secrets bootstrap, bucket/topic init
generator/    synthetic source data + injected data problems, and the checks that prove them
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
.venv/Scripts/python generator/producer.py --speed 200            # replay card_authorizations into Kafka (--replay-from, --limit)
```

Data volume is set in `generator/config.yaml` (`volume.n_transactions`).

## Docs
- Coming per phase: `data_characteristics.md`, `spark_jobs.md`, `flink_jobs.md`, `airflow.md`, `datahub.md`, `schema_design.md`, `storage_optimization.md`, `docker_optimization.md`.
