"""DP3: gold -> offline feature table `gold.feat_card_txn_stats_daily` (Layer 2 feature view), then validated.

Triggered by DP2 with the same logical date (business day `ds`).

| Task | What it does |
|---|---|
| `build_features` | Spark: per card and active day D, windows over the days before D (30/90/180 days), `event_timestamp` = D 00:00, `created` = build time; the job re-counts 500 random rows by brute force and fails on any difference (leakage check) |
| `validate_dp3` | GE on the rows of day ds: one row per card, no NULL keys / timestamps, counts >= 0, ratios in [0, 1] |
"""
from datetime import datetime

from airflow import DAG

from common import DEFAULT_ARGS, spark_job, validate

with DAG("dp3_gold_to_features", schedule=None, start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp3", "features"]) as dag:
    spark_job("build_features", "build_features.py") >> validate("dp3")
