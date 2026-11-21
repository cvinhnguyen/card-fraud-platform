"""DP3: gold -> offline feature table `gold.feat_card_txn_stats_daily` (Layer 2 feature view), then validated.

Scheduled on the Airflow dataset `lakehouse://gold`: it runs when gold has a new checked version, for the business
day carried by that event (common.DAY). No pipeline triggers it; run by hand, it uses its own `ds`.

| Task | What it does |
|---|---|
| `build_features` | Spark: per card and active day L, windows over the 30/90/180 days up to L, `event_timestamp` = L + 1 day 00:00 (the night after), `created` = build time; the job re-counts 500 random rows by brute force and fails on any difference (leakage check) |
| `validate_dp3` | GE on the rows of activity on the business day: one row per card, no NULL keys / timestamps, counts >= 0, ratios in [0, 1] |
| `publish_features` | Airflow dataset event `lakehouse://features`: the feature table has a new checked version (starts `feast_materialize`) |
"""
from datetime import datetime

from airflow import DAG

from common import DEFAULT_ARGS, FEATURES, GOLD, RELEASE_TAG, publish, spark_job, validate

with DAG("dp3_gold_to_features", schedule=[GOLD], start_date=datetime(2026, 10, 1), catchup=False, max_active_runs=1,
         default_args=DEFAULT_ARGS, doc_md=__doc__, tags=["dp3", "features", RELEASE_TAG]) as dag:
    spark_job("build_features", "build_features.py") >> validate("dp3") >> publish(FEATURES)
