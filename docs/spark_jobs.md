# Spark jobs

Spark 4.1.3 standalone (1 master, 1 worker with 8 cores / 3 GB), Delta Lake 4.4.1, Hive Metastore 3.1.3, Trino 484.
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
| raw_disputes | 31,364 |
| raw_merchants | 5,200 |
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
| 1 | `--until 2024-07-01` | +182 export dates, 1,455,416 rows, table without the auth columns (82 s) |
| 2 | `--until 2024-07-02 --no-merge-schema` | fails: `[DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table`, nothing written |
| 3 | no flags (`mergeSchema` on read and write) | +837 export dates, 8,159,516 rows (132 s; 510 s on a run where the machine was short of RAM) |

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
(47,226 rows) and re-running the job merged exactly 47,226 rows back (7,999,526 total, 64 s).

### Before / after (full build, 146 s)

| Measure | Bronze (before) | Silver (after) |
|---|---:|---:|
| Rows | 8,159,516 | 7,999,526 |
| Distinct `transaction_id` | 7,999,526 | 7,999,526 |
| Settled spend (VND) | 3,625,268,094,000 | 3,554,102,498,000 |
| NULL `auth_method` | 1,455,416 | 0 (1,426,881 rows `unknown_pre_regulation`) |

Without dedup the settled spend is overcounted by 2.00%. 878,795 rows (10.99%) carry a location name that maps to a
different unit (old province names before 2025-07-01, plus stale names after it).

Ho Chi Minh City around the reorganization (settled spend, billion VND; share of rows):

| Month | By raw name | By `txn_unit` | Binh Duong (raw) | Share raw | Share unit |
|---|---:|---:|---:|---:|---:|
| 2025-05 | 51.9 | 64.9 | 8.5 | 47.8% | 59.9% |
| 2025-06 | 49.7 | 62.0 | 8.0 | 48.0% | 60.0% |
| 2025-07 | 63.5 | 64.1 | 0.4 | 59.5% | 60.1% |
| 2025-08 | 64.1 | 64.7 | 0.4 | 59.3% | 59.9% |

By raw name, HCMC "grows" 28% in July 2025 and Binh Duong almost disappears. By `txn_unit` the series is flat: nothing
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

An online payment's `txn_city` is the card holder's home unit at payment time. For the 15,393 customers who moved,
joining their 977,652 settled ecom payments to `dim_customer`:

| Join | Home unit matches `txn_unit` |
|---|---:|
| Point in time: `valid_from_ts <= txn_ts < valid_to_ts` | 100.0% |
| Current version only (`is_current`) | 76.7% |

Joining on the current version puts every payment made before a move in the wrong unit.

### Star schema

| Model | Grain | Rows | Built from |
|---|---|---:|---|
| `fact_transaction` (incremental `MERGE`, partitioned by `txn_month`) | one settled transaction | 7,879,773 (315,913 in USD) | `stg_transactions` |
| `dim_customer` (SCD2) | one customer version | 144,167 | `stg_customers` |
| `dim_card` | one card, with its account and owner | 130,120 | `raw_cards` + `raw_accounts` |
| `dim_merchant` | one merchant (`merchant_unit` NULL for the 200 foreign ones) | 5,200 | `raw_merchants` |
| `dim_date` | one day, 2024-01-01 to 2026-10-15 | 1,019 | generated |

Reversed transactions (119,753) are left out: no money moved. Fraud labels are not in the fact because they change
after it is written; they live in `gold.label_transaction` (below). `fact_transaction.customer_sk` is resolved with the
point-in-time join when the row is built, so a query joins `dim_customer` on `customer_sk` without a date range.
The two small reference tables need no cleaning, so `dim_card` and `dim_merchant` read bronze directly.

Tests (`dbt build`, 27/27 pass): unique / not-null keys, every fact key found in its dimension (`relationships`), and
`fact_transaction` has the same row count and settled spend as silver (7,879,773 rows, 3,554,102,498,000 VND).

Incremental load: a full refresh of the fact takes 60 s. Deleting the last 5 export dates (38,752 rows) and running
`dbt run --select fact_transaction` again `MERGE`d exactly 38,752 rows back in 25 s, with no file rewritten.

The Thrift Server runs with `spark.databricks.delta.optimizeMetadataQuery.enabled=false`: Delta answers
`min/max` of a date column from file statistics as `java.sql.Date`, while the Thrift Server uses the Java 8 time API,
so `dim_date` and the incremental filter failed with `ClassCastException: java.time.LocalDate cannot be cast to
java.sql.Date`.

## Labels with label delay (`spark/build_labels.py`)

```bash
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py --as-of 2026-06-30
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py            # up to the last txn date
docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_labels.py --report   # time travel
```

`gold.label_transaction`, one row per transaction, partitioned by `event_month`: `transaction_id`, `card_id`,
`customer_id`, `event_timestamp`, `amount_vnd`, `is_fraud`, `fraud_reported_ts`, `label_as_of`.

A fraud dispute arrives 1 to 60 days after the payment, so a label can change after it was written and the table cannot
be append only. Each run takes `--as-of D` and sees only the transactions and disputes up to D, then `MERGE`s:
new transactions are inserted with the disputes known so far, and older transactions whose dispute arrived since the
last run are flipped to `is_fraud = true`. Because disputes are at most 60 days late, the `MERGE` only scans the last
3 months of partitions. Each run is one Delta version and its commit carries `label_as_of=D`
(`DESCRIBE HISTORY` → `userMetadata`), so `VERSION AS OF` returns the labels exactly as they were known on D.

Replay, one run per month end (about 60 s each):

| Version | `label_as_of` | New rows | Labels flipped | Files removed / added |
|---:|---|---:|---:|---|
| 0 | 2026-06-30 | 7,159,417 | | 0 / 30 |
| 1 | 2026-07-31 | 243,823 | 409 | 2 / 3 |
| 2 | 2026-08-31 | 242,706 | 381 | 2 / 3 |
| 3 | 2026-09-30 | 235,497 | 356 | 2 / 3 |
| 4 | 2026-10-15 | 118,083 | 287 | 2 / 3 |

The final table has 7,999,526 rows (= `stg_transactions`) and 23,594 frauds (= confirmed fraud disputes).

The fraud rate of June 2026 transactions, read from each version, versus the rate of mature rows only
(`event_timestamp < label_as_of - 60 days`):

| Version (`label_as_of`) | June 2026 fraud rate | Mature rows | Mature fraud rate |
|---|---:|---:|---:|
| 0 (2026-06-30) | 0.141% | 6,680,600 | 0.2999% |
| 1 (2026-07-31) | 0.302% | 6,924,346 | 0.2995% |
| 2 (2026-08-31) | 0.311% | 7,167,311 | 0.3000% |
| 3 (2026-09-30) | 0.311% | 7,403,240 | 0.3000% |
| 4 (2026-10-15) | 0.311% | 7,520,544 | 0.2999% |

On the day June ends, more than half of June's frauds are still unknown; after 60 days the rate stops moving. The mature rate is
the same in every version, so a training set built from mature rows does not depend on when it was built. Layer 2
reads `VERSION AS OF` the version its model was trained on and filters to mature rows.

