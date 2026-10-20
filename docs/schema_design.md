# Schema design

All tables are Delta tables in the SeaweedFS bucket `lakehouse`, registered in the Hive Metastore, so Spark, dbt
(through the Spark Thrift Server) and Trino (`delta.<zone>.<table>`) see the same tables.

## Zones

| Zone | Location | Written by | What it holds |
|---|---|---|---|
| bronze | `s3a://lakehouse/bronze/` | `pipelines/spark/ingest_bronze.py` (DP1) | Faithful copies of the sources: duplicates, old province names and missing columns are kept |
| silver | `s3a://lakehouse/silver/` | `pipelines/spark/build_silver.py` (DP2) | Cleaned rows: deduplicated, locations mapped to the 34 units, `auth_method` filled |
| gold | `s3a://lakehouse/gold/` | dbt models, `pipelines/spark/build_labels.py`, `pipelines/spark/build_features.py` (DP3) | Star schema for analysis, the label table and the feature table for Layer 2 |

Bronze and silver are written by PySpark because the dedup, skew and merge demos need DataFrame-level control. Gold
star schema models are SQL in dbt so they are tested (`dbt build`) and documented in one place.

## Naming

| Prefix | Zone | Meaning | Example |
|---|---|---|---|
| `raw_` | bronze | copy of one source table or export | `raw_transactions` |
| `stg_` | silver | cleaned, same grain as the source | `stg_transactions` |
| `dim_` | gold | dimension (who, what, where, when) | `dim_customer` |
| `fact_` | gold | measurable events at a fixed grain | `fact_transaction` |
| `label_` | gold | ML labels that change after they are written | `label_transaction` |
| `feat_` | gold | ML features with `event_timestamp` + `created` (Feast offline source) | `feat_card_txn_stats_daily` |

Columns: `snake_case`; timestamps end in `_ts` (`txn_ts`, `valid_from_ts`); surrogate keys in `_sk`, natural keys in
`_id`; booleans start with `is_`; amounts carry the unit (`amount_vnd`). Pipeline metadata columns start with `_`
(`_ingested_at`, `_source`, `_processed_at`). Timestamps are `timestamp_ntz` in Vietnam local time, as in the source.

## Tables and grain

| Table | Grain (one row is) | Key | Partitioned by | Load |
|---|---|---|---|---|
| `bronze.raw_transactions` | one exported row (duplicates included) | none | `export_date` | append new export dates |
| `bronze.raw_<table>` (6 core banking tables) | one source row | source PK | | full snapshot overwrite |
| `silver.stg_transactions` | one transaction (latest `created_ts`) | `transaction_id` | `txn_date` | `MERGE` new export dates |
| `silver.stg_customers` | one customer version (`customer_id`, `updated_ts`) | both | | overwrite |
| `gold.dim_customer` | one customer version with a change in a tracked attribute (SCD2) | `customer_sk` | | dbt table |
| `gold.dim_card` | one card | `card_id` | | dbt table |
| `gold.dim_merchant` | one merchant | `merchant_id` | | dbt table |
| `gold.dim_date` | one calendar day | `date_key` (`yyyyMMdd`) | | dbt table |
| `gold.fact_transaction` | one **settled** transaction | `transaction_id` | `txn_month` | dbt incremental `MERGE` |
| `gold.label_transaction` | one transaction (settled or reversed) as labelled on `label_as_of` | `transaction_id` | `event_month` | `MERGE` per `--as-of` run |
| `gold.feat_card_txn_stats_daily` | one card after one day with activity (a settled transaction or a reported dispute) | `card_id`, `event_timestamp` | | overwrite |

## Gold ER diagram

```mermaid
erDiagram
    dim_card ||--o{ feat_card_txn_stats_daily : card_id
    dim_card ||--o{ fact_transaction : card_id
    dim_customer ||--o{ fact_transaction : "customer_sk (version valid at txn_ts)"
    dim_merchant ||--o{ fact_transaction : merchant_id
    dim_date ||--o{ fact_transaction : date_key
    label_transaction ||--o| fact_transaction : "transaction_id (reversed txns have no fact row)"

    fact_transaction {
        string transaction_id PK
        int date_key FK
        timestamp txn_ts
        string card_id FK
        string customer_sk FK
        string merchant_id FK
        string terminal_id
        string txn_unit "one of 34 units"
        string txn_city_raw "name as sent by the source"
        string channel "ecom, pos, atm"
        string auth_method
        string three_ds_version
        bigint amount_vnd
        string currency "VND, or USD for foreign merchants"
        date export_date
        string txn_month
    }
    dim_customer {
        string customer_sk PK
        string customer_id
        string full_name
        date dob
        string customer_unit
        string city_raw
        string income_band
        string risk_tier
        int kyc_level
        timestamp valid_from_ts
        timestamp valid_to_ts "exclusive, 9999-12-31 if current"
        boolean is_current
    }
    dim_card {
        string card_id PK
        string account_id
        string customer_id
        string card_type
        string network
        string account_type
        bigint credit_limit
        timestamp issue_ts
        string expiry_month
        string status
    }
    dim_merchant {
        string merchant_id PK
        string merchant_name
        int mcc
        boolean is_online
        string merchant_unit "NULL for foreign merchants"
        string country
    }
    dim_date {
        int date_key PK
        date full_date
        int year
        int month
        string year_month
        int day_of_week
        boolean is_weekend
        boolean is_after_reorg
    }
    label_transaction {
        string transaction_id PK
        string card_id
        string customer_id
        timestamp event_timestamp
        bigint amount_vnd
        boolean is_fraud
        timestamp fraud_reported_ts
        date label_as_of
        string event_month
    }
    feat_card_txn_stats_daily {
        string card_id PK
        timestamp event_timestamp PK "active day L + 1, 00:00"
        int txn_count_30d
        int distinct_merchants_30d
        double avg_amount_90d
        double std_amount_90d
        double ecom_ratio_90d
        double foreign_txn_ratio_90d
        int dispute_count_180d
        timestamp stats_ts "copy of event_timestamp, served as a feature"
        timestamp created
    }
```

## Design notes

- In the SCD2 `dim_customer`, `valid_from_ts` is inclusive, `valid_to_ts` exclusive, and the open version ends at
  9999-12-31, so a point-in-time join is always `valid_from_ts <= ts < valid_to_ts`. Versions are compared on
  `customer_unit`, so relabelling an old province name on 2025-07-01 is not a new version (16,635 avoided).
- `fact_transaction.customer_sk` is resolved once, at build time, against the customer version valid at `txn_ts`.
  Queries join on `customer_sk` without a date condition.
- Every location keeps two columns: the 34-unit value used for grouping (`txn_unit`, `customer_unit`,
  `merchant_unit`) and the raw string from the source (`txn_city_raw`, `city_raw`) for lineage and audits.
- A fraud label can flip up to 60 days after the payment, so labels live in their own table, apart from the fact,
  updated with `MERGE`; each run is a Delta version tagged `label_as_of=<date>` (see [spark_jobs.md](spark_jobs.md)).
- A feature row describes an active day L and carries `event_timestamp` = L + 1 day, the night it is written, so a
  point-in-time join in Feast sees the card's last active day before the payment, never the payment being scored.
  `created` is the processing time.
- Facts are partitioned by month (`txn_month`, `event_month`), so a `MERGE` of recent data rewrites only the last few
  partitions. Silver is partitioned by day (`txn_date`) to match the nightly export.
