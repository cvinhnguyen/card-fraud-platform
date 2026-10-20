"""Validate stage of DP1-DP3: Great Expectations on Spark DataFrames, one suite per pipeline.

Each check reads only the slice the run wrote (the export of business day ds, the labels as of ds, the features of
day ds), so the cost does not grow with the table. Results go to
  s3a://lakehouse/_validation/<suite>/ds=<ds>/results.json
and the job always exits 0 when the checks ran: the Airflow task after it publishes the results (and in P8 sends them
to DataHub) and then fails the run if any expectation failed, so the reason is recorded before the pipeline stops.

GE 0.18 legacy API (`SparkDFDataset`, the same expect_* calls as the course's `ge.from_pandas`), run with the
driver Python of /opt/venvs/ge in the Airflow image.

Usage (inside Airflow; by hand from the scheduler container):
  spark-submit --conf spark.pyspark.driver.python=/opt/venvs/ge/bin/python /opt/jobs/validate.py --suite dp1 --ds 2026-10-15
"""
import argparse
import datetime as dt
import json

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUT = "s3a://lakehouse/_validation"
RAW_TXN = ["transaction_id", "card_id", "merchant_id", "terminal_id", "txn_ts", "txn_city", "amount_vnd", "currency",
           "channel", "status", "created_ts", "auth_method", "three_ds_version", "export_date", "_ingested_at", "_source"]


def units(spark, _p):
    """The 34 provincial-level units, from the mapping table the source delivered."""
    return sorted(r[0] for r in spark.table("bronze.raw_ref_province_mapping").select("new_unit_name").distinct().collect())


def distinct_ids(spark, p):
    """Unique transaction ids in the bronze slice: what silver must hold after dedup."""
    return (spark.table("bronze.raw_transactions").where(F.col("export_date") == p["next_ds"])
            .select("transaction_id").distinct().count())


def not_null(*cols):
    """One not-null expectation per column."""
    return [("expect_column_values_to_not_be_null", {"column": c}) for c in cols]


# suite -> [(table, filter or None, [(expectation, kwargs)])]; a kwarg value may be a function of (spark, params).
SUITES = {
    "dp1": [
        ("bronze.raw_transactions", "export_date = '{next_ds}'", [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            *[("expect_column_to_exist", {"column": c}) for c in RAW_TXN],
            *not_null("transaction_id", "card_id", "merchant_id", "txn_ts", "amount_vnd", "created_ts"),
            # synthetic tokens only: a real card number (digits) must never reach the lake
            ("expect_column_values_to_match_regex", {"column": "card_id", "regex": r"^card_[0-9a-f]{8}$"}),
        ]),
        ("bronze.raw_customers", None, [("expect_table_row_count_to_be_between", {"min_value": 1}),
                                        *not_null("customer_id", "city", "updated_ts")]),
        ("bronze.raw_cards", None, [("expect_table_row_count_to_be_between", {"min_value": 1}),
                                    ("expect_column_values_to_be_unique", {"column": "card_id"}), *not_null("account_id")]),
        ("bronze.raw_accounts", None, [("expect_column_values_to_be_unique", {"column": "account_id"}),
                                       *not_null("customer_id")]),
        ("bronze.raw_merchants", None, [("expect_column_values_to_be_unique", {"column": "merchant_id"}),
                                        *not_null("mcc", "country")]),
        ("bronze.raw_disputes", None, [("expect_column_values_to_be_unique", {"column": "dispute_id"}),
                                       *not_null("transaction_id", "reported_ts")]),
        ("bronze.raw_ref_province_mapping", None, [
            ("expect_table_row_count_to_equal", {"value": 63}),     # the 63 provinces before 2025-07-01
            ("expect_column_unique_value_count_to_be_between", {"column": "new_unit_name", "min_value": 34, "max_value": 34}),
        ]),
        ("bronze.raw_card_authorizations", None, [*not_null("auth_id", "card_id", "auth_ts", "received_ts")]),
    ],
    "dp2_silver": [
        ("silver.stg_transactions", "export_date = '{next_ds}'", [
            ("expect_table_row_count_to_equal", {"value": distinct_ids}),   # dedup kept one row per id, lost none
            ("expect_column_values_to_be_unique", {"column": "transaction_id"}),
            ("expect_column_values_to_be_between", {"column": "amount_vnd", "min_value": 1}),
            ("expect_column_values_to_be_in_set", {"column": "channel", "value_set": ["pos", "ecom", "atm"]}),
            ("expect_column_values_to_be_in_set", {"column": "currency", "value_set": ["VND", "USD"]}),
            ("expect_column_values_to_be_in_set", {"column": "status", "value_set": ["settled", "reversed"]}),
            ("expect_column_values_to_be_in_set", {"column": "txn_unit", "value_set": units}),
            *not_null("auth_method", "txn_date", "txn_city_raw"),
        ]),
        ("silver.stg_customers", None, [("expect_column_values_to_be_in_set", {"column": "customer_unit", "value_set": units}),
                                        *not_null("customer_id", "updated_ts")]),
    ],
    "dp2_gold": [
        ("gold.fact_transaction", "export_date = '{next_ds}'", [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "transaction_id"}),
            *not_null("customer_sk", "date_key", "card_id", "merchant_id", "txn_unit"),
            ("expect_column_values_to_be_between", {"column": "amount_vnd", "min_value": 1}),
        ]),
        ("gold.dim_customer", None, [("expect_column_values_to_be_unique", {"column": "customer_sk"}),
                                     ("expect_column_values_to_be_in_set", {"column": "is_current", "value_set": [True, False]}),
                                     *not_null("valid_from_ts", "valid_to_ts")]),
        ("gold.label_transaction", "label_as_of = '{ds}'", [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "transaction_id"}),
            ("expect_column_values_to_be_in_set", {"column": "is_fraud", "value_set": [True, False]}),
            *not_null("event_timestamp", "customer_id"),
        ]),
    ],
    "dp3": [
        ("gold.feat_card_txn_stats_daily", "to_date(event_timestamp) = date_add('{ds}', 1)", [   # rows of activity on ds
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_column_values_to_be_unique", {"column": "card_id"}),   # one row per card and day
            *not_null("card_id", "event_timestamp", "created", "txn_count_30d", "dispute_count_180d"),
            ("expect_column_values_to_be_between", {"column": "txn_count_30d", "min_value": 0}),
            ("expect_column_values_to_be_between", {"column": "distinct_merchants_30d", "min_value": 0}),
            ("expect_column_values_to_be_between", {"column": "ecom_ratio_90d", "min_value": 0, "max_value": 1}),
            ("expect_column_values_to_be_between", {"column": "foreign_txn_ratio_90d", "min_value": 0, "max_value": 1}),
            ("expect_column_values_to_be_between", {"column": "std_amount_90d", "min_value": 0}),
            ("expect_column_values_to_be_between", {"column": "dispute_count_180d", "min_value": 0}),
        ]),
    ],
}


def run(spark: SparkSession, suite: str, ds: dt.date) -> dict:
    """Run every check of the suite; one result row per expectation."""
    p = {"ds": ds.isoformat(), "next_ds": (ds + dt.timedelta(days=1)).isoformat()}
    rows = []
    for table, where, expectations in SUITES[suite]:
        df = spark.table(table)
        df = df.where(where.format(**p)) if where else df
        gdf = SparkDFDataset(df)
        for name, kwargs in expectations:
            kwargs = {k: v(spark, p) if callable(v) else v for k, v in kwargs.items()}
            r = getattr(gdf, name)(**kwargs).to_json_dict()
            res = r.get("result", {})
            rows.append({"table": table, "filter": where.format(**p) if where else None, "expectation": name,
                         "kwargs": kwargs, "success": r["success"], "observed_value": res.get("observed_value"),
                         "element_count": res.get("element_count"), "unexpected_count": res.get("unexpected_count")})
            print(f"{'ok  ' if r['success'] else 'FAIL'} {table} {name} {json.dumps(kwargs, default=str)[:90]}")
    return {"suite": suite, "ds": p["ds"], "success": all(r["success"] for r in rows), "results": rows}


def write_text(spark: SparkSession, path: str, text: str) -> None:
    """Write one small file through the Hadoop S3A client the session already has."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    out = jpath.getFileSystem(spark._jsc.hadoopConfiguration()).create(jpath, True)
    out.write(bytearray(text.encode()))
    out.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suite", choices=SUITES, required=True)
    ap.add_argument("--ds", type=dt.date.fromisoformat, required=True, help="business date of the run (Airflow ds)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"validate_{args.suite}").getOrCreate()
    report = run(spark, args.suite, args.ds)
    path = f"{OUT}/{args.suite}/ds={args.ds}/results.json"
    write_text(spark, path, json.dumps(report, default=str, indent=1))
    failed = sum(not r["success"] for r in report["results"])
    print(f"{args.suite} {args.ds}: {len(report['results']) - failed}/{len(report['results'])} expectations passed -> {path}")
    spark.stop()
