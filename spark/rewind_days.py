"""Remove the last business days from the lakehouse, so an Airflow backfill can load them again (docs/airflow.md).

  bronze.raw_transactions, silver.stg_transactions, gold.fact_transaction   DELETE export_date > --from
  gold.label_transaction                                                    RESTORE the last version labelled before --from

The deletes drop whole export_date partitions (bronze, silver: metadata only). Postgres snapshots and features are
rebuilt in full by every run, so they need nothing. Delta keeps the removed files, so `RESTORE` undoes this too.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/rewind_days.py --from 2026-10-13
  then: airflow dags backfill dp1_source_to_bronze -s 2026-10-13 -e 2026-10-15
"""
import argparse
import datetime as dt

from pyspark.sql import SparkSession

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="first", type=dt.date.fromisoformat, required=True, help="first business day to reload")
    first = ap.parse_args().first
    spark = SparkSession.builder.appName("rewind_days").getOrCreate()
    for t in ["bronze.raw_transactions", "silver.stg_transactions", "gold.fact_transaction"]:
        n = spark.sql(f"DELETE FROM {t} WHERE export_date > '{first}'").first()[0]   # export of day D = D + 1
        print(f"{t}: {n:,} rows deleted (export_date > {first})")
    hist = spark.sql("DESCRIBE HISTORY gold.label_transaction").select("version", "userMetadata").collect()
    keep = max(h.version for h in hist if h.userMetadata and h.userMetadata.split("=")[1] < first.isoformat())
    spark.sql(f"RESTORE TABLE gold.label_transaction TO VERSION AS OF {keep}")
    print(f"gold.label_transaction: restored to version {keep} "
          f"({next(h.userMetadata for h in hist if h.version == keep)})")
    spark.stop()
