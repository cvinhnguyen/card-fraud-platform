"""DP3: gold -> offline feature table `gold.feat_card_txn_stats_daily` (Layer 2 feature view), then validated.

Triggered by DP2 with the same logical date (business day `ds`).

| Task | What it does |
|---|---|
| `build_features` | Spark: per card and active day L, windows over the 30/90/180 days up to L, `event_timestamp` = L + 1 day 00:00 (the night after), `created` = build time; the job re-counts 500 random rows by brute force and fails on any difference (leakage check) |
| `validate_dp3` | GE on the rows of activity on ds: one row per card, no NULL keys / timestamps, counts >= 0, ratios in [0, 1] |
| `trigger_feast_materialize` | Layer 2: offline -> online store (DAG `feast_materialize`, same logical date) |
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common import DEFAULT_ARGS, spark_job, validate

with DAG("dp3_gold_to_features", schedule=None, start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp3", "features"]) as dag:
    materialize = TriggerDagRunOperator(task_id="trigger_feast_materialize", trigger_dag_id="feast_materialize",
                                        logical_date="{{ logical_date }}", reset_dag_run=True)
    spark_job("build_features", "build_features.py") >> validate("dp3") >> materialize
