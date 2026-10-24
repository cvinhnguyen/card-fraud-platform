"""Remove the last business days from the lakehouse, so an Airflow backfill can load them again (docs/airflow.md).

  bronze.raw_transactions, silver.stg_transactions, gold.fact_transaction   DELETE export_date > --from
  gold.label_transaction                                                    RESTORE the last version labelled before --from

The deletes drop whole export_date partitions (bronze, silver: metadata only). Postgres snapshots and features are
rebuilt in full by every run, so they need nothing. Delta keeps the removed files, so `RESTORE` undoes this too.
bronze.raw_transactions is on lakeFS main, which takes only merges (docs/novel_lakefs_wap.md): its delete runs on a
branch `rewind_<date>` that is committed and merged, the same way a DP1 run publishes.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/rewind_days.py --from 2026-10-13
  then: airflow dags backfill dp1_source_to_bronze -s 2026-10-13 -e 2026-10-15
"""
import argparse
import base64
import datetime as dt
import json
import os
import urllib.request

from pyspark.sql import SparkSession


def lakefs(method: str, path: str, body: dict | None = None) -> dict:
    """One call to the lakeFS API of repository bronze with the keys of the container env."""
    auth = base64.b64encode(f"{os.environ['LAKEFS_ACCESS_KEY_ID']}:{os.environ['LAKEFS_SECRET_ACCESS_KEY']}".encode())
    req = urllib.request.Request(f"http://lakefs:8000/api/v1/repositories/bronze{path}", method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Authorization": f"Basic {auth.decode()}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=300) as r:
        data = r.read()
        return json.loads(data) if data else {}

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="first", type=dt.date.fromisoformat, required=True, help="first business day to reload")
    first = ap.parse_args().first
    spark = SparkSession.builder.appName("rewind_days").getOrCreate()
    branch = f"rewind_{first:%Y%m%d}"
    lakefs("POST", "/branches", {"name": branch, "source": "main"})
    for t in [f"delta.`s3a://bronze/{branch}/raw_transactions`", "silver.stg_transactions", "gold.fact_transaction"]:
        n = spark.sql(f"DELETE FROM {t} WHERE export_date > '{first}'").first()[0]   # export of day D = D + 1
        print(f"{t}: {n:,} rows deleted (export_date > {first})")
    lakefs("POST", f"/branches/{branch}/commits", {"message": f"Rewind: export dates after {first} removed"})
    lakefs("POST", f"/refs/{branch}/merge/main", {"message": f"Rewind to {first} for a backfill"})
    lakefs("DELETE", f"/branches/{branch}")
    print(f"bronze.raw_transactions: rewind merged into lakeFS main from {branch}")
    hist = spark.sql("DESCRIBE HISTORY gold.label_transaction").select("version", "userMetadata").collect()
    keep = max(h.version for h in hist if h.userMetadata and h.userMetadata.split("=")[1] < first.isoformat())
    spark.sql(f"RESTORE TABLE gold.label_transaction TO VERSION AS OF {keep}")
    print(f"gold.label_transaction: restored to version {keep} "
          f"({next(h.userMetadata for h in hist if h.version == keep)})")
    spark.stop()
