"""DP1: copy the core banking sources into bronze Delta tables, unchanged.

  Postgres core_banking.<table>                         -> bronze.raw_<table>       full snapshot each run
  s3a://core-banking-export/transactions/export_date=*  -> bronze.raw_transactions  append the export dates not loaded yet
                                                           (in lakeFS: s3a://bronze/main/raw_transactions)
  s3a://lakehouse/landing/card_authorizations/ (Flink)  -> bronze.raw_card_authorizations  append the files not loaded yet

Bronze keeps every source problem (duplicates, old province names, missing auth columns); silver fixes them.
Every row gets `_ingested_at` and `_source`.

The nightly export is the one source another department delivers as files, so bronze.raw_transactions lives in the
lakeFS repository `bronze` (docs/novel_lakefs_wap.md): readers (metastore, silver, Trino) see branch main, and DP1
appends a new export with `--txn-root s3a://bronze/<run branch>`, validates that branch and only then merges it.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/ingest_bronze.py \
      --txn-root s3a://lakehouse/bronze          # first load, before infra/init_lakefs.py puts it in lakeFS
  ... ingest_bronze.py --until 2024-07-01        # stop at an export date (incremental / schema evolution demo)
  ... ingest_bronze.py --no-merge-schema         # show the failing append when the auth columns appear
  ... ingest_bronze.py --part landing           # one part only: core_banking | exports | landing (Airflow DP1)
  ... ingest_bronze.py --part exports --txn-root s3a://bronze/dp1_20261015   # append on a lakeFS branch

Credentials and the JDBC URL come from the environment (CORE_DB_URL, CORE_DB_USER, CORE_DB_PASSWORD, AWS_*);
Airflow fills them from its connections.
"""
import argparse
import os

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLES = ["customers", "accounts", "cards", "merchants", "disputes", "ref_province_mapping"]
EXPORTS = "s3a://core-banking-export/transactions"
BRONZE = "s3a://lakehouse/bronze"
TXN_MAIN = "s3a://bronze/main"   # lakeFS repository bronze, branch main: where bronze.raw_transactions points
LANDING = "s3a://lakehouse/landing/card_authorizations"
AUTH_SCHEMA = ("auth_id STRING, card_id STRING, merchant_id STRING, amount_vnd BIGINT, channel STRING, terminal_id STRING, "
               "device_id STRING, geo_city STRING, auth_ts STRING, received_ts STRING, kafka_partition INT, "
               "kafka_offset BIGINT, auth_date STRING")


def save(df, table: str, mode: str, partition_by: str | None = None, root: str = BRONZE, **options) -> None:
    """Write a Delta table at <root>/<table> and register it as bronze.<table> in the Hive Metastore once
    (not for a lakeFS run branch: the metastore points at main).

    The schema lives in the Delta log, so later writes never alter the metastore entry
    (Hive Metastore 3.1 cannot parse timestamp_ntz and would log a warning on every schema change).
    """
    path = f"{root}/{table}"
    w = df.write.format("delta").mode(mode).options(**options)
    (w.partitionBy(partition_by) if partition_by else w).save(path)
    if root in (BRONZE, TXN_MAIN):
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


def ingest_exports(spark: SparkSession, until: str | None, merge_schema: bool, root: str = TXN_MAIN) -> None:
    """Append the export dates after the newest one already in bronze (and up to `until`) to <root>/raw_transactions:
    lakeFS main, or the run's branch in DP1."""
    table, path = "bronze.raw_transactions", f"{root}/raw_transactions"
    try:
        done = str(spark.read.format("delta").load(path).agg(F.max("export_date")).first()[0])
    except AnalysisException:   # first load: no table there yet
        done = ""
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
         partition_by="export_date", root=root, mergeSchema=str(merge_schema).lower())
    total = spark.read.format("delta").load(path).count()
    print(f"{table} at {root}: +{len(new)} export dates ({new[0]} .. {new[-1]}), {total:,} rows total")


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
    ap.add_argument("--txn-root", help="where to append the exports: s3a://bronze/<lakeFS branch> (DP1), or "
                                       "s3a://lakehouse/bronze for the first load before infra/init_lakefs.py")
    args = ap.parse_args()
    if args.part in (None, "exports") and (args.txn_root is None or args.txn_root.rstrip("/") == TXN_MAIN):
        ap.error("bronze.raw_transactions is on lakeFS main, which takes merges only: append on a branch with "
                 "--txn-root s3a://bronze/<branch> and merge it once validated (DP1 does both)")

    spark = SparkSession.builder.appName("dp1_ingest_bronze").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{BRONZE}'")
    if args.part in (None, "core_banking"):
        ingest_core_banking(spark)
    if args.part in (None, "exports"):
        ingest_exports(spark, args.until, not args.no_merge_schema, args.txn_root.rstrip("/"))
    if args.part in (None, "landing"):
        ingest_landing(spark)
    spark.stop()
