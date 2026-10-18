"""The gold star schema written by hand in PySpark, the way the course teaches: the baseline for novel idea 1 (dbt).

Builds the same five tables as dbt/models/gold (same logic, same columns) into the database `gold_pyspark`, then
runs the same 22 checks as `dbt build` (unique / not_null / relationships / SCD2 / reconciliation) as assertions.
docs/novel_dbt.md compares the two: rows equal to dbt's tables, lines of code, checks, run time, and the work needed
to add one more dimension.

Usage (repo root; same resources as the Thrift Server dbt runs on):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit \
      --conf spark.cores.max=2 --conf spark.executor.memory=1g /opt/jobs/build_gold_pyspark.py
"""
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

DB, ROOT = "gold_pyspark", "s3a://lakehouse/gold_pyspark"
OPEN_END = "9999-12-31 00:00:00"   # valid_to_ts of the current version (exclusive end)


def save(df: DataFrame, name: str, partition_by: str | None = None) -> None:
    """Overwrite a Delta table under ROOT and register it in gold_pyspark."""
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    (w.partitionBy(partition_by) if partition_by else w).save(f"{ROOT}/{name}")
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS {DB}.{name} USING DELTA LOCATION '{ROOT}/{name}'")


def dim_customer(spark: SparkSession) -> DataFrame:
    """SCD2: a version starts when a tracked attribute changes (city compared after normalization)."""
    by_cust = Window.partitionBy("customer_id").orderBy("updated_ts")
    attrs = F.struct(F.col("full_name"), F.col("dob"), F.col("customer_unit").alias("unit"), "income_band",
                     "risk_tier", "kyc_level")
    v = (spark.table("silver.stg_customers").withColumn("attr_hash", F.md5(F.to_json(attrs)))
         .withColumn("prev_hash", F.lag("attr_hash").over(by_cust))
         .where(F.col("prev_hash").isNull() | (F.col("prev_hash") != F.col("attr_hash"))))
    nxt = F.lead("updated_ts").over(by_cust)
    return v.select(F.md5(F.concat("customer_id", F.lit("|"), F.col("updated_ts").cast("string"))).alias("customer_sk"),
                    "customer_id", "full_name", "dob", "customer_unit", "city_raw", "income_band", "risk_tier",
                    "kyc_level", F.col("updated_ts").alias("valid_from_ts"),
                    F.coalesce(nxt, F.lit(OPEN_END).cast("timestamp_ntz")).alias("valid_to_ts"), nxt.isNull().alias("is_current"))


def dim_card(spark: SparkSession) -> DataFrame:
    """One row per card with its account and owner."""
    a = spark.table("bronze.raw_accounts")
    return (spark.table("bronze.raw_cards").alias("c").join(a.alias("a"), "account_id")
            .select("c.card_id", "c.account_id", "a.customer_id", "c.card_type", "c.network", "a.account_type",
                    "a.credit_limit", "c.issue_ts", "c.expiry_month", "c.status"))


def dim_merchant(spark: SparkSession) -> DataFrame:
    """One row per merchant; city is already a unit (NULL abroad)."""
    return spark.table("bronze.raw_merchants").select("merchant_id", "merchant_name", "mcc", "is_online",
                                                       F.col("city").alias("merchant_unit"), "country")


def dim_date(spark: SparkSession) -> DataFrame:
    """One row per day of the transaction period."""
    lo, hi = spark.table("silver.stg_transactions").agg(F.min("txn_date"), F.max("txn_date")).first()
    d = F.col("d")
    return (spark.sql(f"SELECT explode(sequence(DATE'{lo}', DATE'{hi}', INTERVAL 1 DAY)) AS d")
            .select(F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"), d.alias("full_date"),
                    F.year(d).alias("year"), F.month(d).alias("month"), F.date_format(d, "yyyy-MM").alias("year_month"),
                    F.dayofweek(d).alias("day_of_week"), F.dayofweek(d).isin(1, 7).alias("is_weekend"),
                    (d >= F.lit("2025-07-01").cast("date")).alias("is_after_reorg")))


def fact_transaction(spark: SparkSession) -> DataFrame:
    """Settled transactions with the customer version valid at txn_ts (point in time). Full rebuild: an incremental
    MERGE like dbt's would add a max(export_date) lookup and a DeltaTable.merge here."""
    t = spark.table("silver.stg_transactions").where("status = 'settled'").alias("t")
    c, d = spark.table(f"{DB}.dim_card").alias("c"), spark.table(f"{DB}.dim_customer").alias("d")
    pit = (F.col("d.customer_id") == F.col("c.customer_id")) & (F.col("t.txn_ts") >= F.col("d.valid_from_ts")) \
        & (F.col("t.txn_ts") < F.col("d.valid_to_ts"))
    return (t.join(c, F.col("c.card_id") == F.col("t.card_id")).join(d, pit, "left")
            .select("t.transaction_id", F.date_format("t.txn_date", "yyyyMMdd").cast("int").alias("date_key"),
                    "t.txn_ts", "t.card_id", "d.customer_sk", "t.merchant_id", "t.terminal_id", "t.txn_unit",
                    "t.txn_city_raw", "t.channel", "t.auth_method", "t.three_ds_version", "t.amount_vnd",
                    "t.currency", "t.export_date", F.date_format("t.txn_date", "yyyy-MM").alias("txn_month")))


def checks(spark: SparkSession) -> int:
    """The 22 tests of `dbt build`, one assertion each."""
    t = lambda n: spark.table(f"{DB}.{n}")   # noqa: E731
    keys = {"dim_customer": "customer_sk", "dim_card": "card_id", "dim_merchant": "merchant_id",
            "dim_date": "date_key", "fact_transaction": "transaction_id"}
    n = 0
    for table, key in keys.items():   # unique + not_null on every key: 10
        assert t(table).where(F.col(key).isNull()).count() == 0, f"{table}.{key} null"
        assert t(table).groupBy(key).count().where("count > 1").count() == 0, f"{table}.{key} not unique"
        n += 2
    for table, col in [("dim_customer", "customer_id"), ("dim_customer", "customer_unit"),
                       ("dim_customer", "valid_from_ts"), ("dim_card", "customer_id"), ("fact_transaction", "customer_sk")]:
        assert t(table).where(F.col(col).isNull()).count() == 0, f"{table}.{col} null"
        n += 1
    for col, dim, key in [("customer_sk", "dim_customer", "customer_sk"), ("card_id", "dim_card", "card_id"),
                          ("merchant_id", "dim_merchant", "merchant_id"), ("date_key", "dim_date", "date_key")]:
        keys_ = t(dim).select(F.col(key).alias("_k"))
        orphans = t("fact_transaction").join(keys_, F.col(col) == F.col("_k"), "left_anti").where(F.col(col).isNotNull())
        assert orphans.count() == 0, f"fact_transaction.{col} not in {dim}"
        n += 1
    cur = t("dim_customer").groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("k"))
    assert cur.where("k != 1").count() == 0, "customer without exactly one current version"
    nxt = F.lead("valid_from_ts").over(Window.partitionBy("customer_id").orderBy("valid_from_ts"))
    gaps = t("dim_customer").withColumn("nxt", nxt).where("nxt IS NOT NULL AND nxt != valid_to_ts")
    assert gaps.count() == 0, "customer versions overlap or leave gaps"
    s = spark.table("silver.stg_transactions").where("status = 'settled'").agg(F.count("*"), F.sum("amount_vnd")).first()
    f = t("fact_transaction").agg(F.count("*"), F.sum("amount_vnd")).first()
    assert tuple(s) == tuple(f), f"fact {tuple(f)} != silver {tuple(s)}"
    return n + 3


def compare_with_dbt(spark: SparkSession) -> None:
    """Row-for-row comparison with the tables dbt built in `gold` (both directions of EXCEPT ALL)."""
    for name in ["dim_customer", "dim_card", "dim_merchant", "dim_date", "fact_transaction"]:
        mine, dbt = spark.table(f"{DB}.{name}"), spark.table(f"gold.{name}")
        cols = [c for c in dbt.columns if c in mine.columns]
        only_mine, only_dbt = mine.select(cols).exceptAll(dbt.select(cols)).count(), dbt.select(cols).exceptAll(mine.select(cols)).count()
        print(f"{name}: {mine.count():,} rows here, {dbt.count():,} in dbt gold, {len(cols)} columns compared, "
              f"only here {only_mine}, only in dbt {only_dbt}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_pyspark").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB} LOCATION '{ROOT}'")
    t0 = time.perf_counter()
    for name, build, part in [("dim_customer", dim_customer, None), ("dim_card", dim_card, None),
                              ("dim_merchant", dim_merchant, None), ("dim_date", dim_date, None),
                              ("fact_transaction", fact_transaction, "txn_month")]:
        save(build(spark), name, part)
        print(f"{DB}.{name}: {spark.table(f'{DB}.{name}').count():,} rows")
    built = time.perf_counter() - t0
    n = checks(spark)
    print(f"build {built:.0f}s, {n} checks passed in {time.perf_counter() - t0 - built:.0f}s")
    compare_with_dbt(spark)
    spark.stop()
