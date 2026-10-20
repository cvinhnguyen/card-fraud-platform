"""DP1: copy the core banking sources into bronze Delta tables, unchanged.

  Postgres core_banking.<table>                         -> bronze.raw_<table>       full snapshot each run
  s3a://core-banking-export/transactions/export_date=*  -> bronze.raw_transactions  append the export dates not loaded yet
  s3a://lakehouse/landing/card_authorizations/ (Flink)  -> bronze.raw_card_authorizations  append the files not loaded yet

Bronze keeps every source problem (duplicates, old province names, missing auth columns); silver fixes them.
Every row gets `_ingested_at` and `_source`.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/ingest_bronze.py
  ... ingest_bronze.py --until 2024-07-01        # stop at an export date (incremental / schema evolution demo)
  ... ingest_bronze.py --no-merge-schema         # show the failing append when the auth columns appear
  ... ingest_bronze.py --part landing           # one part only: core_banking | exports | landing (Airflow DP1)

Credentials and the JDBC URL come from the environment (CORE_DB_URL, CORE_DB_USER, CORE_DB_PASSWORD, AWS_*);
Airflow fills them from its connections.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLES = ["customers", "accounts", "cards", "merchants", "disputes", "ref_province_mapping"]
EXPORTS = "s3a://core-banking-export/transactions"
BRONZE = "s3a://lakehouse/bronze"
LANDING = "s3a://lakehouse/landing/card_authorizations"
AUTH_SCHEMA = ("auth_id STRING, card_id STRING, merchant_id STRING, amount_vnd BIGINT, channel STRING, terminal_id STRING, "
               "device_id STRING, geo_city STRING, auth_ts STRING, received_ts STRING, kafka_partition INT, "
               "kafka_offset BIGINT, auth_date STRING")


def save(df, table: str, mode: str, partition_by: str | None = None, **options) -> None:
    """Write a Delta table at BRONZE/<table> and register it as bronze.<table> in the Hive Metastore once.

    The schema lives in the Delta log, so later writes never alter the metastore entry
    (Hive Metastore 3.1 cannot parse timestamp_ntz and would log a warning on every schema change).
    """
    path = f"{BRONZE}/{table}"
    w = df.write.format("delta").mode(mode).options(**options)
    (w.partitionBy(partition_by) if partition_by else w).save(path)
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table} USING DELTA LOCATION '{path}'")


def ingest_core_banking(spark: SparkSession) -> None:
    """Snapshot every Postgres table. Timestamps stay without time zone, as in the source."""
    url = os.environ.get("CORE_DB_URL", "jdbc:postgresql://postgres:5432/core_banking")
    for t in TABLES:
        df = (spark.read.format("jdbc")
              .options(url=url, dbtable=t, user=os.environ["CORE_DB_USER"], password=os.environ["CORE_DB_PASSWORD"],
                       driver="org.postgresql.Driver", preferTimestampNTZ="true").load()
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source", F.lit(f"postgres:core_banking.{t}")))
        # ponytail: full overwrite (459k rows, seconds); Delta time travel keeps the old snapshots.
        # Switch to incremental by updated_ts / reported_ts when the tables grow.
        save(df, f"raw_{t}", "overwrite", overwriteSchema="true")
        print(f"bronze.raw_{t}: {spark.table(f'bronze.raw_{t}').count():,} rows")


def export_dates(spark: SparkSession) -> list[str]:
    """export_date values present in the export bucket, oldest first."""
    path = spark._jvm.org.apache.hadoop.fs.Path(EXPORTS)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return sorted(s.getPath().getName().split("=", 1)[1] for s in fs.listStatus(path) if s.isDirectory())


def ingest_exports(spark: SparkSession, until: str | None, merge_schema: bool) -> None:
    """Append the export dates after the newest one already in bronze (and up to `until`)."""
    table = "bronze.raw_transactions"
    done = str(spark.table(table).agg(F.max("export_date")).first()[0]) if spark.catalog.tableExists(table) else ""
    new = [d for d in export_dates(spark) if d > done and (until is None or d <= until)]
    if not new:
        print(f"{table}: nothing new after {done}")
        return
    # Read only the new folders: files up to 2024-07-01 have no auth_method / three_ds_version columns.
    df = (spark.read.options(basePath=EXPORTS, mergeSchema=str(merge_schema).lower())
          .parquet(*[f"{EXPORTS}/export_date={d}" for d in new])
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source", F.col("_metadata.file_path")))
    save(df.repartition("export_date"), "raw_transactions", "append",
         partition_by="export_date", mergeSchema=str(merge_schema).lower())
    print(f"{table}: +{len(new)} export dates ({new[0]} .. {new[-1]}), {spark.table(table).count():,} rows total")


def ingest_landing(spark: SparkSession) -> None:
    """Raw authorizations written by pipelines/flink/raw_landing.py. A file stream with availableNow reads every landing file
    not seen before (the checkpoint remembers them), including files a late event adds to an older auth_date."""
    fs = spark._jvm.org.apache.hadoop.fs.Path(LANDING).getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(LANDING)):
        print("landing: nothing to load")
        return
    table, path = "raw_card_authorizations", f"{BRONZE}/raw_card_authorizations"
    raw = (spark.readStream.schema(AUTH_SCHEMA).json(LANDING)   # auth_date is the partition folder
           .withColumn("auth_ts", F.to_timestamp_ntz("auth_ts")).withColumn("received_ts", F.to_timestamp_ntz("received_ts"))
           .withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.col("_metadata.file_path")))
    (raw.writeStream.format("delta").partitionBy("auth_date").option("checkpointLocation", f"{BRONZE}/_checkpoints/{table}")
     .trigger(availableNow=True).start(path).awaitTermination())
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table} USING DELTA LOCATION '{path}'")
    print(f"bronze.{table}: {spark.table(f'bronze.{table}').count():,} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--until", help="last export_date to load, YYYY-MM-DD")
    ap.add_argument("--no-merge-schema", action="store_true", help="append without mergeSchema (demo: fails at 2024-07-02)")
    ap.add_argument("--part", choices=["core_banking", "exports", "landing"], help="load only this source (default: all)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("dp1_ingest_bronze").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{BRONZE}'")
    if args.part in (None, "core_banking"):
        ingest_core_banking(spark)
    if args.part in (None, "exports"):
        ingest_exports(spark, args.until, not args.no_merge_schema)
    if args.part in (None, "landing"):
        ingest_landing(spark)
    spark.stop()
