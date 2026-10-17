"""Schema evolution demo 1: reading every export file at once, with and without mergeSchema.

Export files before 2024-07-02 have no auth_method / three_ds_version. Without mergeSchema Spark takes the schema of
one file (the oldest here), so the two columns silently disappear for every row, including the 6.7M rows that have
them. ingest_bronze.py therefore reads with mergeSchema (and Delta's mergeSchema on write: demo 2 in docs).

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/schema_evolution_read.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EXPORTS = "s3a://core-banking-export/transactions/"
AUTH = ["auth_method", "three_ds_version"]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("schema_evolution_read").getOrCreate()
    for merge in (False, True):
        df = spark.read.option("mergeSchema", merge).parquet(EXPORTS)
        have = [c for c in AUTH if c in df.columns]
        filled = df.where(F.col(have[0]).isNotNull()).count() if have else 0
        print(f"mergeSchema={str(merge).lower():<5}  {len(df.columns)} columns, auth columns {have or 'MISSING'}, "
              f"{df.count():,} rows, {filled:,} with auth_method")
    spark.stop()
