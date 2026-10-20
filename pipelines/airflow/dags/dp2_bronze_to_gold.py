"""DP2: bronze -> silver (PySpark) -> gold (dbt-spark) + labels, each zone validated before the next reads it.

Triggered by DP1 with the same logical date (business day `ds`).

| Task | What it does |
|---|---|
| `build_silver` | Spark: dedup on transaction_id, 63 -> 34 unit normalization, auth_method for pre-2024-07-01 rows; MERGE of the new export dates into `silver.stg_transactions`; `silver.stg_customers` |
| `validate_dp2_silver` | GE: one row per transaction_id and none lost (count = distinct ids in bronze), amount > 0, channel / currency / status / txn_unit (34 units) in their sets |
| `dbt_build` | dbt-spark (session target: its own Spark session, pool `spark`): dims, SCD2 `dim_customer`, incremental `fact_transaction`, and the dbt tests (unique, not_null, relationships fact -> dims, reconciliation with silver) |
| `dbt_docs` | `dbt docs generate`: manifest + catalog for dbt docs and DataHub |
| `build_labels` | Spark: `gold.label_transaction` as of ds (new transactions + labels flipped by disputes reported since the last run) |
| `validate_dp2_gold` | GE on the fact rows of this export, `dim_customer`, and the labels written as of ds |
| `trigger_dp3` | starts `dp3_gold_to_features` for the same logical date |
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common import DEFAULT_ARGS, S3_ENV, spark_job, validate

DBT = "/opt/venvs/dbt/bin/dbt"
DBT_ARGS = "--project-dir {{ var.value.dbt_project_dir }} --profiles-dir {{ var.value.dbt_project_dir }} --target session"
# Target `session`: dbt opens a Spark session in this process (driver in the scheduler, same spark-defaults, so the
# OpenLineage listener sees dbt's SQL too). pyspark comes from SPARK_HOME; artifacts go to a volume (the project
# mount is read-only).
DBT_ENV = {
    **S3_ENV,
    "PYTHONPATH": "/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip",
    "PYSPARK_PYTHON": "python3",   # executors' Python; the driver is the dbt venv's
    "PYSPARK_SUBMIT_ARGS": "--conf spark.executor.memory=1536m --conf spark.openlineage.appName=dp2_bronze_to_gold "
                           "--name dbt_gold pyspark-shell",   # same settings as spark_job()
    "DBT_TARGET_PATH": "{{ var.value.dbt_artifacts_dir }}/target",
    "DBT_LOG_PATH": "{{ var.value.dbt_artifacts_dir }}/logs",
}

with DAG("dp2_bronze_to_gold", schedule=None, start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp2", "silver", "gold", "dbt"]) as dag:
    silver = spark_job("build_silver", "build_silver.py")
    # pool `spark`: a dbt run is a Spark application like the others
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build {DBT_ARGS}", env=DBT_ENV, append_env=True,
                             pool="spark")
    dbt_docs = BashOperator(task_id="dbt_docs", bash_command=f"{DBT} docs generate {DBT_ARGS}", env=DBT_ENV,
                            append_env=True, pool="spark")
    labels = spark_job("build_labels", "build_labels.py", ["--as-of", "{{ ds }}"])
    trigger = TriggerDagRunOperator(task_id="trigger_dp3", trigger_dag_id="dp3_gold_to_features",
                                    logical_date="{{ logical_date }}", reset_dag_run=True)

    gold_checks = validate("dp2_gold")
    silver >> validate("dp2_silver") >> [dbt_build, labels]
    dbt_build >> dbt_docs
    [dbt_build, labels] >> gold_checks >> trigger
