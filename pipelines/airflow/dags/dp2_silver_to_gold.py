"""DP2, gold pipeline: silver -> gold star schema (dbt-spark) + fraud labels (PySpark), validated.

Scheduled on the Airflow dataset `lakehouse://silver`: it runs when silver has a new checked version, for the
business day carried by that event (common.DAY). The silver pipeline does not trigger it; run by hand, it uses its
own `ds` and reads the last committed Delta version of silver.

| Task | What it does |
|---|---|
| `dbt_build` | dbt-spark (session target: its own Spark session, pool `spark`): dims, SCD2 `dim_customer`, incremental `fact_transaction`, and the dbt tests (unique, not_null, relationships fact -> dims, reconciliation with silver) |
| `dbt_docs` | `dbt docs generate`: manifest + catalog for dbt docs and DataHub |
| `build_labels` | Spark: `gold.label_transaction` as of the business day (new transactions + labels flipped by disputes reported since the last run) |
| `validate_dp2_gold` | GE on the fact rows of this export, `dim_customer`, and the labels written as of the business day |
| `publish_gold` | Airflow dataset event `lakehouse://gold`: gold has a new checked version (starts `dp3_gold_to_features`) |
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DAY, DEFAULT_ARGS, GOLD, S3_ENV, SILVER, publish, spark_job, validate

DBT = "/opt/venvs/dbt/bin/dbt"
DBT_ARGS = "--project-dir {{ var.value.dbt_project_dir }} --profiles-dir {{ var.value.dbt_project_dir }} --target session"
# Target `session`: dbt opens a Spark session in this process (driver in the scheduler, same spark-defaults, so the
# OpenLineage listener sees dbt's SQL too). pyspark comes from SPARK_HOME; artifacts go to a volume (the project
# mount is read-only).
DBT_ENV = {
    **S3_ENV,
    "PYTHONPATH": "/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip",
    "PYSPARK_PYTHON": "python3",   # executors' Python; the driver is the dbt venv's
    "PYSPARK_SUBMIT_ARGS": "--conf spark.executor.memory=1536m --conf spark.openlineage.appName=dp2_silver_to_gold "
                           "--conf spark.driver.bindAddress=0.0.0.0 --name dbt_gold pyspark-shell",   # as spark_job()
    "DBT_TARGET_PATH": "{{ var.value.dbt_artifacts_dir }}/target",
    "DBT_LOG_PATH": "{{ var.value.dbt_artifacts_dir }}/logs",
}

with DAG("dp2_silver_to_gold", schedule=[SILVER], start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp2", "gold", "dbt"]) as dag:
    # pool `spark`: a dbt run is a Spark application like the others
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build {DBT_ARGS}", env=DBT_ENV, append_env=True,
                             pool="spark")
    dbt_docs = BashOperator(task_id="dbt_docs", bash_command=f"{DBT} docs generate {DBT_ARGS}", env=DBT_ENV,
                            append_env=True, pool="spark")
    labels = spark_job("build_labels", "build_labels.py", ["--as-of", DAY])

    dbt_build >> dbt_docs
    [dbt_build, labels] >> validate("dp2_gold") >> publish(GOLD)
