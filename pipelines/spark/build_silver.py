"""DP2 (silver): clean the bronze copies. Each fix prints its before/after numbers.

  bronze.raw_transactions -> silver.stg_transactions  dedup, 63 -> 34 unit normalization, auth_method for old rows
  bronze.raw_customers    -> silver.stg_customers     unit normalization (input of the SCD2 dim_customer)

stg_transactions is incremental: only bronze export dates newer than the newest one in silver are read, and they are
MERGEd on transaction_id (keep the latest created_ts), so a re-run or a duplicate in a later export cannot double count.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_silver.py
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SILVER = "s3a://lakehouse/silver"
UNKNOWN_AUTH = "unknown_pre_regulation"   # column did not exist before 2024-07-01; not "no authentication"


def unit_lookup(spark: SparkSession) -> DataFrame:
    """name -> unit for every old province name and every new unit name (new names map to themselves)."""
    m = spark.table("bronze.raw_ref_province_mapping")
    return (m.select(F.col("old_province_name").alias("name"), F.col("new_unit_name").alias("unit"))
            .union(m.select(F.col("new_unit_name").alias("name"), F.col("new_unit_name").alias("unit")))
            .distinct())


def normalize(df: DataFrame, units: DataFrame, col: str, unit_col: str) -> DataFrame:
    """Add unit_col (one of the 34 units) and rename col to <col>_raw. Fails if a name is not in the mapping."""
    out = (df.join(F.broadcast(units), df[col] == units["name"], "left")
           .withColumnRenamed(col, f"{col}_raw").withColumnRenamed("unit", unit_col).drop("name"))
    # ponytail: assert as the quality gate until Great Expectations runs in Airflow (P7)
    unmapped = out.where(F.col(unit_col).isNull()).count()
    assert unmapped == 0, f"{unmapped} rows with a {col} not in ref_province_mapping"
    return out


def save(df: DataFrame, table: str, partition_by: str | None = None) -> None:
    """Overwrite a Delta table at SILVER/<table>, registered once as silver.<table>."""
    path = f"{SILVER}/{table}"
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    (w.partitionBy(partition_by) if partition_by else w).save(path)
    df.sparkSession.sql(f"CREATE TABLE IF NOT EXISTS silver.{table} USING DELTA LOCATION '{path}'")


def report(name: str, before: dict, after: dict) -> None:
    """Print one before/after line of the cleaning report."""
    print(f"  {name:<34} before {before[name]:>22,}  after {after[name]:>22,}")


def build_transactions(spark: SparkSession, units: DataFrame) -> None:
    """Clean the new bronze export dates and MERGE them into silver.stg_transactions."""
    table = "silver.stg_transactions"
    exists = spark.catalog.tableExists(table)
    done = str(spark.table(table).agg(F.max("export_date")).first()[0]) if exists else ""
    raw = spark.table("bronze.raw_transactions").where(F.col("export_date") > F.lit(done or "1900-01-01").cast("date"))

    latest = Window.partitionBy("transaction_id").orderBy(F.col("created_ts").desc())
    clean = (raw.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
             .withColumn("auth_method", F.coalesce("auth_method", F.lit(UNKNOWN_AUTH)))
             .withColumn("txn_date", F.to_date("txn_ts"))
             .withColumn("_processed_at", F.current_timestamp()))
    clean = normalize(clean, units, "txn_city", "txn_unit").cache()

    def stats(df: DataFrame) -> dict:
        """Row count, unique ids, settled spend and NULL auth_method of a DataFrame."""
        settled = F.col("status") == "settled"
        return df.agg(F.count("*").alias("rows"), F.countDistinct("transaction_id").alias("unique ids"),
                      F.sum(F.when(settled, F.col("amount_vnd"))).alias("settled spend (VND)"),
                      F.sum(F.when(F.col("auth_method").isNull(), 1).otherwise(0)).alias("null auth_method")).first().asDict()

    before, after = stats(raw), stats(clean)
    if not before["rows"]:
        print(f"{table}: nothing new after {done}")
        return
    print(f"{table}: export dates after {done or 'start'}")
    for name in before:
        report(name, before, after)
    mapped = clean.where(F.col("txn_city_raw") != F.col("txn_unit")).count()
    print(f"  txn_city mapped to a different unit: {mapped:,} rows ({mapped / after['rows']:.2%}); "
          f"{UNKNOWN_AUTH}: {clean.where(F.col('auth_method') == UNKNOWN_AUTH).count():,} rows")

    if not exists:
        save(clean.repartition("txn_date"), "stg_transactions", "txn_date")
    else:   # a later export may repeat an id: keep the row with the newest created_ts
        first_day = clean.agg(F.min("txn_date")).first()[0]
        (DeltaTable.forName(spark, table).alias("t")
         .merge(clean.alias("s"), f"t.txn_date >= '{first_day}' AND t.transaction_id = s.transaction_id")
         .whenMatchedUpdateAll(condition="s.created_ts > t.created_ts")
         .whenNotMatchedInsertAll()
         .execute())
    print(f"{table}: {spark.table(table).count():,} rows total")
    clean.unpersist()


def build_customers(spark: SparkSession, units: DataFrame) -> None:
    """One row per source version, city normalized so a relabel is not mistaken for a move (SCD2 input)."""
    cust = normalize(spark.table("bronze.raw_customers"), units, "city", "customer_unit")
    save(cust.withColumn("_processed_at", F.current_timestamp()), "stg_customers")
    print(f"silver.stg_customers: {spark.table('silver.stg_customers').count():,} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dp2_build_silver").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{SILVER}'")
    units = unit_lookup(spark).cache()
    assert units.select("unit").distinct().count() == 34, "ref_province_mapping must cover 34 units"
    build_transactions(spark, units)
    build_customers(spark, units)
    spark.stop()
