"""Save the Delta versions of tables, and restore them later: the data side of a batch rollback.

The batch-pipelines CI check (pipelines/airflow/Jenkinsfile) runs DP2 silver for real on a new release, then the gold,
features and materialize runs its publish starts. Those runs write the silver and gold tables before their checks. If
a check fails, the pipeline puts the old code back and, with this job, the tables too: RESTORE TABLE ... TO VERSION
AS OF the version saved before the check. A table that did not change is left alone.

Usage (repo root), with schema names (every Delta table in them) or table names:
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/table_versions.py \
    save silver gold                                     # prints one line: VERSIONS {"<table>": <version>, ...}
  ... table_versions.py restore '{"silver.stg_transactions": 41, "gold.fact_transaction": 17}'
"""
import json
import sys

from pyspark.sql import SparkSession


def version(spark: SparkSession, table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def tables(spark: SparkSession, names: list) -> list:
    """Table names as given, and every table of a schema name; tables that are not Delta are skipped."""
    out = []
    for n in names:
        out += [n] if "." in n else [f"{n}.{t.name}" for t in spark.catalog.listTables(n) if not t.isTemporary]
    return [t for t in out if spark.sql(f"DESCRIBE DETAIL {t}").first()["format"] == "delta"]


def main(argv: list) -> None:
    spark = SparkSession.builder.appName("table_versions").getOrCreate()
    if argv[0] == "save":
        print("VERSIONS " + json.dumps({t: version(spark, t) for t in tables(spark, argv[1:])}))
    elif argv[0] == "restore":
        for table, v in json.loads(argv[1]).items():
            now = version(spark, table)
            if now == v:
                print(f"{table}: still at version {v}")
                continue
            spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {int(v)}")
            print(f"{table}: restored from version {now} to {v}")
    else:
        sys.exit("usage: table_versions.py save <schema or table>... | restore '<json>'")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
