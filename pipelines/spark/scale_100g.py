"""P9: the merchant join benchmark of skew_join.py (baseline / salting / AQE / broadcast) on the 100 GB offline
dataset (generator/generate_scale.py), one timed run per method (a run is a full pass over the data), AQE skew
threshold back at Spark's 256 MB default.

Reads the raw export, not a deduplicated silver table: the silver dedup (a window over all 4.1B rows, every column
shuffled) measured about 1 h per 1% of the data on the 8 GB Docker VM, so ~4 days, and the 2% duplicates do not
change the skew. Only the join key and one value column are read (Parquet column pruning), so a run shuffles 2
of the export's 13 columns. Kept apart from the 1 GB pipeline: own prefix, nothing written.

Usage (repo root; batch profile up, nothing else running on the worker):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit \
      --conf spark.sql.shuffle.partitions=2000 --conf spark.io.compression.codec=zstd \
      --conf spark.cleaner.periodicGC.interval=5min /opt/jobs/scale_100g.py [--methods salting aqe] [--no-stats]
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import skew_join

SRC = "s3a://core-banking-export/scale_100g"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--methods", nargs="+", default=skew_join.METHODS, choices=skew_join.METHODS)
    ap.add_argument("--no-stats", action="store_true", help="skip the two counting passes (top merchants, skew line)")
    args = ap.parse_args()
    spark = SparkSession.builder.appName("p9_scale_skew").getOrCreate()
    txn = spark.read.parquet(f"{SRC}/transactions").select("merchant_id", "amount_vnd")
    merchants = spark.read.parquet(f"{SRC}/merchants.parquet").select("merchant_id", "merchant_name", "mcc").cache()
    merchants.count()
    if not args.no_stats:
        skew_join.TOP = [r.merchant_id for r in txn.groupBy("merchant_id").count().orderBy(F.desc("count"))
                         .limit(skew_join.TOP_N).collect()]
    # Spark's own AQE defaults at this size (skew threshold 256 MB, target partition 64 MB)
    skew_join.RUNS, skew_join.WARMUP, skew_join.SKEW_THRESHOLD, skew_join.ADVISORY = 1, False, "256m", "64m"
    skew_join.bench(spark, txn, merchants, "100 GB raw export", args.methods, stats=not args.no_stats)
    spark.stop()
