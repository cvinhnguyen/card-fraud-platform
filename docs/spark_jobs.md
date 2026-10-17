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

## DP2: bronze to silver (`spark/build_silver.py`)

```bash
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_silver.py
```

| Silver table | From | Fixes |
|---|---|---|
| `silver.stg_transactions`, partitioned by `txn_date` | `bronze.raw_transactions` | dedup, `txn_unit` (34 units) + `txn_city_raw`, `auth_method` NULL → `unknown_pre_regulation` |
| `silver.stg_customers` | `bronze.raw_customers` | `customer_unit` (34 units) + `city_raw`; input of the SCD2 `dim_customer` |

The job stops if a city name is missing from `ref_province_mapping` (no silent NULL unit).

### Incremental load

Only bronze export dates newer than the newest `export_date` in silver are read. The batch is deduplicated, then
`MERGE`d on `transaction_id`: an id already in silver is replaced only by a row with a later `created_ts`, so a re-run,
or a duplicate that lands in a later export, cannot double count. Test: deleting the last 6 export dates from silver
(47,226 rows) and re-running the job merged exactly 47,226 rows back (7,999,526 total, 69 s).

### Before / after (full build, 148 s)

| Measure | Bronze (before) | Silver (after) |
|---|---:|---:|
| Rows | 8,159,516 | 7,999,526 |
| Distinct `transaction_id` | 7,999,526 | 7,999,526 |
| Settled spend (VND) | 3,682,960,413,000 | 3,611,008,776,000 |
| NULL `auth_method` | 1,455,416 | 0 (1,426,881 rows `unknown_pre_regulation`) |

Without dedup the settled spend is overcounted by 1.99%. 879,837 rows (11.00%) carry a location name that maps to a
different unit (old province names before 2025-07-01, plus stale names after it).

Ho Chi Minh City around the reorganization (settled spend, billion VND; share of rows):

| Month | By raw name | By `txn_unit` | Binh Duong (raw) | Share raw | Share unit |
|---|---:|---:|---:|---:|---:|
| 2025-05 | 52.1 | 65.0 | 8.4 | 48.0% | 59.9% |
| 2025-06 | 50.9 | 63.6 | 8.3 | 47.9% | 59.9% |
| 2025-07 | 64.8 | 65.5 | 0.5 | 59.4% | 60.0% |
| 2025-08 | 64.4 | 65.0 | 0.4 | 59.2% | 59.7% |

By raw name, HCMC "grows" 27% in July 2025 and Binh Duong almost disappears. By `txn_unit` the series is flat: nothing
changed physically, only the labels.

## Gold with dbt-spark (`dbt/`, novel idea 1)

dbt runs SQL models through the Spark Thrift Server (compose profile `dbt`, port 10000), so the gold tables are Delta
tables in `s3a://lakehouse/gold/`, registered in the same Hive Metastore and readable from Trino.

```bash
docker compose -f infra/docker-compose.yaml --profile dbt up -d spark-thrift
.venv/Scripts/dbt build --project-dir dbt --profiles-dir dbt     # models + tests
```

### SCD2 `gold.dim_customer`

The source already holds one row per customer version (`customer_id` + `updated_ts`), from 2024 onwards. A dbt
snapshot only records the state it sees at each run, so it would lose that history. `dim_customer` is therefore a
model over `silver.stg_customers`: `lag()` compares a hash of the tracked attributes (unit, income band, risk tier, KYC
level, name, date of birth) with the previous row, keeps only rows where something changed, and `lead()` gives
`valid_to_ts` (exclusive; open-ended = 9999-12-31) and `is_current`.

The city is compared after normalization (`customer_unit`), so the relabel of old province names on 2025-07-01 is not a
new version:

| Count | Rows |
|---|---:|
| Source rows (`stg_customers`) | 193,207 |
| SCD2 versions comparing the raw city name | 160,802 |
| SCD2 versions comparing `customer_unit` (= `dim_customer`) | 144,167 |
| Spurious versions avoided | 16,635 |
| Current versions (one per customer) | 100,000 |

Tests (`dbt build`, 8/8 pass): unique / not-null keys, exactly one current version per customer, versions of a
customer follow each other without gaps or overlaps.

### Point-in-time join

An online payment's `txn_city` is the card holder's home unit at payment time. For the 15,391 customers who moved,
joining their 944,529 ecom payments to `dim_customer`:

| Join | Home unit matches `txn_unit` |
|---|---:|
| Point in time: `valid_from_ts <= txn_ts < valid_to_ts` | 100.0% |
| Current version only (`is_current`) | 76.7% |

Joining on the current version puts every payment made before a move in the wrong unit.
