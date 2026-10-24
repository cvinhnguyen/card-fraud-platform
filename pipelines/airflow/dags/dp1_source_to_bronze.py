"""DP1: core banking sources -> bronze Delta tables, unchanged, then validated, then published (Write-Audit-Publish).

Runs daily for business day `ds`. The core banking system exports day ds into `export_date = ds + 1` between 01:00
and 03:00, so the run waits for that folder before reading it. `ds + 1`, not `data_interval_end`: the same day the
validation checks (pipelines/spark/validate.py), also for a manual run, whose data interval ends on ds itself.

The export is the one source another department delivers as files, so it goes through Write-Audit-Publish
(docs/novel_lakefs_wap.md): bronze.raw_transactions lives in the lakeFS repository `bronze`, the run appends the new
export on its own branch `dp1_<ds>`, the checks read that branch, and only a run that passed is merged into main, which
the metastore table (silver, Trino) reads. A failed run leaves main as it was and its branch in place to inspect.

| Task | What it does |
|---|---|
| `create_branch` | lakeFS branch `dp1_<ds>` from main (an older one of the same day is replaced) |
| `wait_for_export` | S3 sensor on `core-banking-export/transactions/export_date={{ macros.ds_add(ds, 1) }}/` (reschedule mode: frees the worker slot while waiting) |
| `snapshot_core_banking` | Spark JDBC: full snapshot of the 6 Postgres tables into `bronze.raw_*` (overwrite; Delta keeps the old versions) |
| `ingest_exports` | Spark: appends the new export dates to `raw_transactions` on the run's branch (mergeSchema for the 2024-07-01 auth columns) |
| `ingest_stream_landing` | Spark file stream (availableNow): Flink's raw authorization files -> `bronze.raw_card_authorizations` |
| `commit_branch` | lakeFS commit on the branch: the state the checks read and the merge publishes |
| `validate_dp1` | GE on Spark over the new export (on the branch) + every snapshot table, then publish results and gate |
| `merge_to_main` | lakeFS merge of the branch into main: the export becomes visible to silver and Trino in one step; branch deleted |
| `trigger_dp2` | starts `dp2_bronze_to_gold` for the same logical date |

Every load is incremental by what bronze already holds, so a retry or a rerun of an old date adds nothing twice.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from common import CORE_DB_ENV, DEFAULT_ARGS, commit_branch, create_branch, merge_to_main, spark_job, validate

BRANCH = "dp1_{{ ds_nodash }}"
TXN_ROOT = "s3a://bronze/" + BRANCH   # the run's lakeFS branch; readers use s3a://bronze/main

with DAG("dp1_source_to_bronze", schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False,
         max_active_runs=1, default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp1", "bronze"]) as dag:
    wait = S3KeySensor(task_id="wait_for_export", aws_conn_id="lakehouse_s3", bucket_name="{{ var.value.export_bucket }}",
                       bucket_key="transactions/export_date={{ macros.ds_add(ds, 1) }}/*.parquet", wildcard_match=True,
                       mode="reschedule", poke_interval=300, timeout=6 * 3600)
    branch = create_branch(BRANCH)
    snapshot = spark_job("snapshot_core_banking", "ingest_bronze.py", ["--part", "core_banking"], env=CORE_DB_ENV)
    exports = spark_job("ingest_exports", "ingest_bronze.py",
                        ["--part", "exports", "--until", "{{ macros.ds_add(ds, 1) }}", "--txn-root", TXN_ROOT])
    landing = spark_job("ingest_stream_landing", "ingest_bronze.py", ["--part", "landing"])
    trigger = TriggerDagRunOperator(task_id="trigger_dp2", trigger_dag_id="dp2_bronze_to_gold",
                                    logical_date="{{ logical_date }}", reset_dag_run=True)

    [wait, branch] >> exports
    checks = validate("dp1", ["--txn-root", TXN_ROOT])   # reads the export on the run's branch
    [snapshot, exports >> commit_branch(BRANCH), landing] >> checks >> merge_to_main(BRANCH) >> trigger
