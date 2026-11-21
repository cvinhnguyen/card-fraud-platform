"""Save the Delta versions of some tables, and restore them later: the data side of a batch rollback.

The batch-pipelines CI check (pipelines/airflow/Jenkinsfile) runs DP2 silver for real on a new release, and that run
writes the silver tables before it checks them. If a check fails, the pipeline puts the old code back and, with this
job, the tables too: RESTORE TABLE ... TO VERSION AS OF the version saved before the run. A table that did not change
is left alone.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/table_versions.py \
    save silver.stg_transactions silver.stg_customers          # prints one JSON line: {"<table>": <version>, ...}
  ... table_versions.py restore '{"silver.stg_transactions": 41, "silver.stg_customers": 17}'
"""
import json
import sys

from pyspark.sql import SparkSession


def version(spark: SparkSession, table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def main(argv: list) -> None:
    spark = SparkSession.builder.appName("table_versions").getOrCreate()
    if argv[0] == "save":
        print("VERSIONS " + json.dumps({t: version(spark, t) for t in argv[1:]}))
    elif argv[0] == "restore":
        for table, v in json.loads(argv[1]).items():
            now = version(spark, table)
            if now == v:
                print(f"{table}: still at version {v}")
                continue
            spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {int(v)}")
            print(f"{table}: restored from version {now} to {v}")
    else:
        sys.exit("usage: table_versions.py save <table>... | restore '<json>'")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
