# Spark jobs

Spark 4.1.3 standalone (1 master, 1 worker with 4 cores / 3 GB), Delta Lake 4.4.1, Hive Metastore 3.1.3, Trino 484.
Jobs live in `spark/` and run inside the `spark-master` container (compose profile `batch`).

## DP1: source to bronze (`spark/ingest_bronze.py`)

```bash
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/ingest_bronze.py
```

| Source | Bronze table | Load |
|---|---|---|
| Postgres `core_banking.<table>` (6 tables, JDBC) | `bronze.raw_<table>` | full snapshot, overwrite; Delta time travel keeps earlier snapshots |
| `s3a://core-banking-export/transactions/export_date=*/` | `bronze.raw_transactions`, partitioned by `export_date` | append only the export dates after the newest one already loaded |

Bronze is a faithful copy: duplicates, old province names and missing columns stay as they are. Each row gets
`_ingested_at` and `_source` (the JDBC table or the source file path). Timestamps are read without a time zone
(`preferTimestampNTZ`), as they are in the source. Each table is registered once in the Hive Metastore
(`CREATE TABLE ... USING DELTA LOCATION`); the schema lives in the Delta log, so Trino reads it as `delta.bronze.*`.

Result (2026-10-17, 1 GB data):

| Table | Rows |
|---|---:|
| raw_transactions (1,019 export dates) | 8,159,516 |
| raw_customers | 193,207 |
| raw_cards | 130,120 |
| raw_accounts | 100,000 |
| raw_disputes | 31,357 |
| raw_merchants | 5,000 |
| raw_ref_province_mapping | 63 |

`SELECT count(*) FROM delta.bronze.raw_transactions` on Trino returns 8,159,516, the number of rows the generator wrote.

### Incremental load

The job reads the newest `export_date` in bronze and lists the export bucket, then reads only the newer folders.
A second run with no new files prints `nothing new after 2026-10-16` and appends nothing.
`--until YYYY-MM-DD` stops at a given export date.

### Schema evolution demo

Export files up to 2024-07-01 have no `auth_method` / `three_ds_version` columns; later files have both.

| Step | Command | Result |
|---|---|---|
| 1 | `--until 2024-07-01` | +178 export dates, 1,455,416 rows, table without the auth columns (89 s) |
| 2 | `--until 2024-07-02 --no-merge-schema` | fails: `[DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table`, nothing written |
| 3 | no flags (`mergeSchema` on read and write) | +837 export dates, 8,159,516 rows (132 s) |

After step 3, Trino shows the auth columns filled for every row exported after the cutoff and NULL before it:

| Export date | Rows | With `auth_method` |
|---|---:|---:|
| up to 2024-07-01 | 1,455,416 | 0 |
| after 2024-07-01 | 6,704,100 | 6,704,100 |

Silver (P4) maps the NULLs to `unknown_pre_regulation` instead of guessing a method.

### Notes

- Hive Metastore is 3.1.3 because Spark 4.1's built-in Hive 2.3 client cannot call a 4.x metastore
  (`Invalid method name: 'get_table'`).
- Trino's default per-node memory (~500 MB with a 2 GB container) is too small for an exact
  `count(DISTINCT transaction_id)` over 8M rows; `approx_distinct` works.
