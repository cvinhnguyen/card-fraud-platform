"""P9: the silver dedup of build_silver.py at scale, on export dates of the 100 GB dataset (docs/scale_100g.md).

Scope, --batch-days:
  0 (global)  every selected export date in one job, as a one-shot backfill would run it
  N (batched) N export dates per job, appended to the same Delta table. Correct because a duplicate comes from
              re-running one nightly export, so both copies share an export_date (generator: gen_day); DP2 relies
              on the same fact when it dedups only the new export dates. Memory and shuffle disk stay those of one
              batch, and the total grows linearly with the number of dates.

How, --method:
  window     row_number over transaction_id ordered by created_ts desc (build_silver.py): every row, every column
             is shuffled and sorted
  broadcast  only 2% of the ids are duplicated: find them from the id column alone, pass the other rows through a
             broadcast anti join (no shuffle, no sort), run the window on the duplicated rows only

Prints wall time, spill and a checksum of the result, so the outputs of all variants can be compared.

Usage (repo root; batch profile up; data from generate_scale.py --every K):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit \
      --conf spark.io.compression.codec=zstd --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=16m \
      /opt/jobs/scale_dedup.py --method broadcast --batch-days 2 [--dates 10]
"""
import argparse
import json
import re
import time
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from build_silver import UNKNOWN_AUTH, unit_lookup

SRC = "s3a://core-banking-export/scale_100g/transactions"
OUT = "s3a://lakehouse/scale_100g/stg_transactions"


def latest(raw, method: str):
    """One row per transaction_id, the one with the latest created_ts."""
    if method == "window":
        w = Window.partitionBy("transaction_id").orderBy(F.col("created_ts").desc())
        return raw.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    # broadcast: only 2% of the ids are duplicated. Find them from the id column alone (column pruning), pass the
    # other rows through a broadcast anti join (no shuffle, no sort) and run the window on the duplicated rows only.
    dup = F.broadcast(raw.groupBy("transaction_id").count().where("count > 1").select("transaction_id"))
    return (raw.join(dup, "transaction_id", "left_anti")
            .unionByName(latest(raw.join(dup, "transaction_id", "left_semi"), "window")))


def clean(spark: SparkSession, dates: list[str], method: str):
    """build_silver.build_transactions on these export dates: latest created_ts per transaction_id, units."""
    raw = (spark.read.option("basePath", SRC).option("mergeSchema", "true")
           .parquet(*[f"{SRC}/export_date={d}" for d in dates]))
    if "auth_method" not in raw.columns:   # every date of the batch before 2024-07-01
        raw = raw.withColumn("auth_method", F.lit(None).cast("string"))
    out = (latest(raw, method)
           .withColumn("auth_method", F.coalesce("auth_method", F.lit(UNKNOWN_AUTH)))
           .withColumn("txn_date", F.to_date("txn_ts")))
    units = unit_lookup(spark)
    return (out.join(F.broadcast(units), out.txn_city == units.name, "left")
            .withColumnRenamed("txn_city", "txn_city_raw").withColumnRenamed("unit", "txn_unit").drop("name"))


def spill_gb(spark: SparkSession) -> tuple[float, float]:
    """(memory, disk) bytes spilled by all stages of this application, from the Spark REST API."""
    sc = spark.sparkContext
    stages = json.load(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"))
    return (sum(s["memoryBytesSpilled"] for s in stages) / 1e9, sum(s["diskBytesSpilled"] for s in stages) / 1e9)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--batch-days", type=int, required=True, help="export dates per batch; 0 = global")
    ap.add_argument("--dates", type=int, help="only the first N export dates present")
    ap.add_argument("--method", choices=["window", "broadcast"], default="window")
    args = ap.parse_args()
    spark = SparkSession.builder.appName(f"p9_scale_dedup_{args.method}_{args.batch_days or 'global'}").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
    files = spark.read.parquet(SRC).inputFiles()   # lists the files, reads nothing
    dates = sorted({re.search(r"export_date=([\d-]+)", f).group(1) for f in files})[:args.dates]
    step = args.batch_days or len(dates)
    t0 = time.perf_counter()
    for i in range(0, len(dates), step):
        (clean(spark, dates[i:i + step], args.method).write.format("delta").mode("append" if i else "overwrite")
         .option("mergeSchema", "true").option("overwriteSchema", "true" if not i else "false").save(OUT))
    took = time.perf_counter() - t0
    mem, disk = spill_gb(spark)
    rows_in = spark.read.option("basePath", SRC).parquet(*[f"{SRC}/export_date={d}" for d in dates]).count()
    out = spark.read.format("delta").load(OUT)
    r = out.agg(F.count("*").alias("n"), F.countDistinct("transaction_id").alias("ids"),
                F.sum(F.xxhash64("transaction_id", "created_ts", "amount_vnd") % 1_000_003).alias("checksum"),
                F.sum(F.col("txn_unit").isNull().cast("int")).alias("unmapped")).first()
    assert r.n == r.ids and r.unmapped == 0, r
    print(f"DEDUP method={args.method} batch_days={args.batch_days} dates={len(dates)} rows_in={rows_in:,} rows_out={r.n:,} "
          f"checksum={r.checksum} took={took:.0f}s ({took / len(dates):.1f} s/date) "
          f"spill_memory={mem:.2f}GB spill_disk={disk:.2f}GB", flush=True)
    spark.stop()
