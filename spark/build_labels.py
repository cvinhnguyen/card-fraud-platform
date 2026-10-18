"""Fraud labels for Layer 2: one row per transaction, is_fraud as known on a given date (label delay).

  silver.stg_transactions + bronze.raw_disputes -> gold.label_transaction

A dispute arrives 1-60 days after the payment, so a label written today can still flip later. Each run takes
`--as-of D` (only transactions and disputes up to D exist) and MERGEs into the table:
  - new transactions are inserted, with is_fraud from the disputes known so far
  - older transactions whose fraud dispute arrived since the last run are flipped to is_fraud = true
Every run is one Delta version; its commit carries `label_as_of=D`, so `VERSION AS OF` gives the labels exactly
as they were known on D. Training reads only mature rows: event_timestamp < label_as_of - 60 days.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py --as-of 2026-06-30
  ... build_labels.py                    # as of the last transaction date
  ... build_labels.py --report           # fraud rate of one month as seen by every version (time travel)
"""
import argparse
import datetime as dt

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "gold.label_transaction"
PATH = "s3a://lakehouse/gold/label_transaction"
MATURITY_DAYS = 60   # the longest dispute delay in the source


def build(spark: SparkSession, as_of: dt.date) -> None:
    """Label the transactions up to as_of: insert new ones, flip labels of late disputes (MERGE)."""
    end = as_of + dt.timedelta(days=1)
    exists = spark.catalog.tableExists(TABLE)
    prev = spark.table(TABLE).agg(F.max("label_as_of")).first()[0] if exists else None
    if prev and as_of <= prev:
        print(f"{TABLE}: already labelled as of {prev}")
        return
    start = prev + dt.timedelta(days=1) if prev else dt.date(1900, 1, 1)

    owner = (spark.table("bronze.raw_cards").select("card_id", "account_id")
             .join(spark.table("bronze.raw_accounts").select("account_id", "customer_id"), "account_id").drop("account_id"))
    fraud = (spark.table("bronze.raw_disputes").where(F.col("is_confirmed_fraud") & (F.col("reported_ts") < F.lit(end)))
             .groupBy("transaction_id").agg(F.min("reported_ts").alias("fraud_reported_ts")))
    new_txn = (spark.table("silver.stg_transactions")
               .where((F.col("txn_date") >= F.lit(start)) & (F.col("txn_date") <= F.lit(as_of)))
               .select("transaction_id", "card_id", F.col("txn_ts").alias("event_timestamp"), "amount_vnd"))
    src = (new_txn.join(F.broadcast(owner), "card_id", "left").join(fraud, "transaction_id", "left")
           .withColumn("is_fraud", F.col("fraud_reported_ts").isNotNull())
           .withColumn("label_as_of", F.lit(as_of))
           .withColumn("event_month", F.date_format("event_timestamp", "yyyy-MM")))

    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", f"label_as_of={as_of}")
    if not exists:
        src.repartition("event_month").write.format("delta").partitionBy("event_month").save(PATH)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} USING DELTA LOCATION '{PATH}'")
    else:
        # Older rows only need the label columns; disputes are at most 60 days late, so only the last 3 months change.
        flips = (fraud.where(F.col("fraud_reported_ts") >= F.lit(start))
                 .join(new_txn.select("transaction_id"), "transaction_id", "left_anti")
                 .withColumn("is_fraud", F.lit(True)).withColumn("label_as_of", F.lit(as_of)))
        oldest = (start - dt.timedelta(days=MATURITY_DAYS + 31)).strftime("%Y-%m")
        (DeltaTable.forName(spark, TABLE).alias("t")
         .merge(src.unionByName(flips, allowMissingColumns=True).alias("s"),
                f"t.event_month >= '{oldest}' AND t.transaction_id = s.transaction_id")
         .whenMatchedUpdate(condition="t.is_fraud <> s.is_fraud",
                            set={c: f"s.{c}" for c in ("is_fraud", "fraud_reported_ts", "label_as_of")})
         .whenNotMatchedInsertAll(condition="s.event_timestamp IS NOT NULL")
         .execute())
    m = spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 1").first()["operationMetrics"]
    print(f"{TABLE} as of {as_of}: +{int(m.get('numTargetRowsInserted', m.get('numOutputRows'))):,} new rows, "
          f"{int(m.get('numTargetRowsUpdated', 0)):,} labels flipped")


def report(spark: SparkSession) -> None:
    """Fraud rate as seen by every version: one recent month (still filling up) and the mature rows."""
    hist = (spark.sql(f"DESCRIBE HISTORY {TABLE}").where(F.col("userMetadata").startswith("label_as_of="))
            .select("version", "userMetadata").orderBy("version").collect())
    first = dt.date.fromisoformat(hist[0].userMetadata.split("=")[1])
    month = first.strftime("%Y-%m")
    print(f"{'version':>7} {'label_as_of':<12} {'rows':>11} {month + ' fraud':>14} {'mature rows':>12} {'mature fraud':>13}")
    for h in hist:
        as_of = dt.date.fromisoformat(h.userMetadata.split("=")[1])
        df = spark.read.format("delta").option("versionAsOf", h.version).load(PATH)
        cutoff = as_of - dt.timedelta(days=MATURITY_DAYS)
        r = df.agg(F.count("*").alias("n"),
                   F.avg(F.when(F.col("event_month") == month, F.col("is_fraud").cast("int"))).alias("month"),
                   F.count(F.when(F.col("event_timestamp") < F.lit(cutoff), 1)).alias("mature"),
                   F.avg(F.when(F.col("event_timestamp") < F.lit(cutoff), F.col("is_fraud").cast("int"))).alias("mature_rate")).first()
        print(f"{h.version:>7} {str(as_of):<12} {r.n:>11,} {r.month:>14.4%} {r.mature:>12,} {r.mature_rate:>13.4%}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", type=dt.date.fromisoformat, help="label date (default: the last transaction date)")
    ap.add_argument("--report", action="store_true", help="print the fraud rate seen by every version")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("label_transaction").getOrCreate()
    if args.report:
        report(spark)
    else:
        build(spark, args.as_of or spark.table("silver.stg_transactions").agg(F.max("txn_date")).first()[0])
    spark.stop()
