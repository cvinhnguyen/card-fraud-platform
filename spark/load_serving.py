"""Serving copy for people (dashboards, the analyst's card investigation screen): gold -> Postgres `serving`.

  gold.fact_transaction (last DAYS days) + dim_merchant -> serving.mart.obt_card_txn (one big table, no joins at read time)

Rows are written in txn_ts order, as a daily append would, so a BRIN index on txn_ts stays useful.
Indexes and the EXPLAIN before/after live in sql/serving_indexes.sql.
ponytail: same Postgres instance as core_banking to save RAM on the laptop; a real setup gives serving its own server.

Usage (repo root; the `serving` database must exist, see docs/storage_optimization.md):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/load_serving.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DAYS = 180
URL = "jdbc:postgresql://postgres:5432/serving"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("load_serving").getOrCreate()
    fact = spark.table("gold.fact_transaction")
    since = fact.agg(F.date_sub(F.max(F.to_date("txn_ts")), DAYS - 1)).first()[0]
    obt = (fact.where(F.to_date("txn_ts") >= F.lit(since))
           .join(F.broadcast(spark.table("gold.dim_merchant").select("merchant_id", "merchant_name", "mcc", "country")), "merchant_id")
           .select("transaction_id", "txn_ts", "card_id", "merchant_id", "merchant_name", "mcc", "country", "txn_unit",
                   "channel", "auth_method", "amount_vnd", "currency")
           .orderBy("txn_ts").coalesce(1))   # one writer keeps the insert order = time order
    (obt.write.format("jdbc").mode("overwrite")
     .options(url=URL, dbtable="mart.obt_card_txn", user=os.environ["CORE_DB_USER"], password=os.environ["CORE_DB_PASSWORD"],
              driver="org.postgresql.Driver", batchsize=10000)
     .save())
    print(f"serving.mart.obt_card_txn: {obt.count():,} rows since {since}")
    spark.stop()
