"""DP2, silver pipeline: bronze -> silver (PySpark), validated before anything reads it.

Scheduled on the Airflow dataset `lakehouse://bronze`: it runs when bronze has a new checked version, for the
business day carried by that event (common.DAY). DP1 does not trigger it and it does not wait for DP1: run by hand
at any time, it reads the last committed Delta version of bronze and cleans whatever export dates silver does not
hold yet.

| Task | What it does |
|---|---|
| `build_silver` | Spark: dedup on transaction_id, 63 -> 34 unit normalization, auth_method for pre-2024-07-01 rows; MERGE of the new export dates into `silver.stg_transactions`; `silver.stg_customers` |
| `validate_dp2_silver` | GE: one row per transaction_id and none lost (count = distinct ids in bronze), amount > 0, channel / currency / status / txn_unit (34 units) in their sets |
| `publish_silver` | Airflow dataset event `lakehouse://silver`: silver has a new checked version (starts `dp2_silver_to_gold`) |
"""
from datetime import datetime

from airflow import DAG
from datahub_airflow_plugin.entities import Dataset

from common import BRONZE, DEFAULT_ARGS, RELEASE_TAG, SILVER, publish, spark_job, validate

# OpenLineage reports the MERGE into silver.stg_transactions with its target only, not the bronze source (OpenLineage
# 1.53 on Delta 4; probed 2026-10-25: the same with a SQL MERGE, without cache, without source materialization).
# build_silver declares the tables it reads and writes, so silver keeps its link to bronze in DataHub.
SILVER_IN = [Dataset("hive", f"bronze.{t}") for t in ("raw_transactions", "raw_customers", "raw_ref_province_mapping")]
SILVER_OUT = [Dataset("hive", f"silver.{t}") for t in ("stg_transactions", "stg_customers")]

with DAG("dp2_bronze_to_silver", schedule=[BRONZE], start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp2", "silver", RELEASE_TAG]) as dag:
    silver = spark_job("build_silver", "build_silver.py", inlets=SILVER_IN, outlets=SILVER_OUT)
    silver >> validate("dp2_silver") >> publish(SILVER)
