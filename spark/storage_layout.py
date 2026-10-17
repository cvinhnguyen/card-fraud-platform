"""Lakehouse layout: the same fact data stored three ways, then the same queries on each.

  by_city   partitionBy(txn_city_raw) after a 200-way shuffle: up to 200 files per city -> small files
  by_month  partitionBy(txn_month), version 0 naive (small files), 1 after OPTIMIZE (compaction),
            2 after OPTIMIZE ZORDER BY (card_id) (data skipping on card lookups)

Queries: one card's transactions (fraud investigation), one month's spend by unit (reporting), one city's spend.
Each query runs once to warm up, then RUNS times; files and bytes read come from the scan node of the last run
(Spark REST API, SQL metrics). Tables live under s3a://lakehouse/lab/ and are not registered in the metastore.

Usage (repo root, stop spark-thrift first):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/storage_layout.py
"""
import json
import re
import statistics
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LAB = "s3a://lakehouse/lab"
RUNS = 3
SHUFFLE = 200   # Spark's default shuffle partitions: what a job writes after a join or aggregation


def detail(spark: SparkSession, path: str) -> str:
    """Files and size of the current version, from the Delta log."""
    d = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    return f"{d.numFiles:,} files, {d.sizeInBytes / 2**20:,.0f} MB, avg {d.sizeInBytes / d.numFiles / 2**20:.2f} MB"


def scan_metrics(spark: SparkSession, label: str) -> tuple[int, float]:
    """(files read, MB read) summed over the scan nodes of the last SQL execution described as `label`."""
    sc = spark.sparkContext
    api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/sql?details=true&planDescription=false&length=10000"
    runs = [e for e in json.load(urllib.request.urlopen(api, timeout=30)) if e["description"] == label]
    files = mb = 0.0
    for node in runs[-1]["nodes"]:
        if node["nodeName"].startswith("Scan"):
            m = {x["name"]: x["value"] for x in node["metrics"]}
            files += float(m.get("number of files read", "0").replace(",", ""))
            size = re.search(r"([\d.]+) (B|KiB|MiB|GiB)", m.get("size of files read", "0 B").replace(",", ""))
            mb += float(size[1]) * {"B": 2**-20, "KiB": 2**-10, "MiB": 1, "GiB": 2**10}[size[2]]
    return int(files), mb


def bench(spark: SparkSession, name: str, df: DataFrame, queries: dict) -> None:
    for q, run in queries.items():
        label = f"{name} | {q}"
        spark.sparkContext.setJobDescription(label)
        times = []
        for i in range(RUNS + 1):   # run 0 = warm-up
            t0 = time.perf_counter()
            run(df)
            if i:
                times.append(time.perf_counter() - t0)
        time.sleep(1)   # let the listener record the metrics
        files, mb = scan_metrics(spark, label)
        print(f"  {name:<22} {q:<14} {statistics.median(times):>7.2f} s  {files:>7,} files  {mb:>8.1f} MB read")
    spark.sparkContext.setJobDescription(None)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("p5_storage_layout").getOrCreate()
    # ponytail: OPTIMIZE targets 1 GB files; at 1 GB of data that is one file per month and Z-order has nothing to skip.
    # 4 MB keeps the 100 GB shape (a month = many files); at 100 GB use the default.
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 4 * 2**20)
    fact = spark.table("gold.fact_transaction")
    card = fact.groupBy("card_id").count().orderBy(F.desc("count")).first().card_id   # an active card
    queries = {
        "card lookup": lambda df: df.where(F.col("card_id") == card).agg(F.count("*"), F.sum("amount_vnd")).collect(),
        "month by unit": lambda df: df.where(F.col("txn_month") == "2026-06").groupBy("txn_unit").agg(F.sum("amount_vnd")).collect(),
        "one city": lambda df: df.where(F.col("txn_city_raw") == "Da Nang").agg(F.sum("amount_vnd")).collect(),
    }

    by_city, by_month = f"{LAB}/fact_by_city", f"{LAB}/fact_by_month"
    for path, col in ((by_city, "txn_city_raw"), (by_month, "txn_month")):
        t0 = time.perf_counter()
        fact.repartition(SHUFFLE).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .partitionBy(col).save(path)
        print(f"write partitionBy({col}): {time.perf_counter() - t0:.0f} s, {detail(spark, path)}")
    for extra in ("", " ZORDER BY (card_id)"):
        t0 = time.perf_counter()
        spark.sql(f"OPTIMIZE delta.`{by_month}`{extra}").collect()
        print(f"OPTIMIZE{extra}: {time.perf_counter() - t0:.0f} s, {detail(spark, by_month)}")
    last = spark.sql(f"DESCRIBE HISTORY delta.`{by_month}`").agg(F.max("version")).first()[0]
    versions = {"naive": last - 2, "OPTIMIZE": last - 1, "OPTIMIZE ZORDER": last}

    print(f"\ncard {card}; median of {RUNS} runs")
    bench(spark, "by_city", spark.read.format("delta").load(by_city), queries)
    for name, v in versions.items():
        bench(spark, f"by_month {name}", spark.read.format("delta").option("versionAsOf", v).load(by_month), queries)
    spark.stop()
