"""DP3: gold -> feat_card_txn_stats_daily, the batch feature view `card_txn_stats_daily` of Layer 2 (Feast).

One row per card and day L on which the card had activity (a settled transaction or a dispute reported), with
windows ending at L (L included) and event_timestamp = L + 1 day 00:00: the nightly run after day L writes it, and
from then on it is the card's latest value. A point-in-time join for a payment on day D therefore sees the row of
the card's last active day before D, exactly the row the online store holds when the payment is scored.
(Layer 2 found the earlier layout, one row on each *payment* day D with windows ending at D-1, gave training a
row that only existed because of the payment being scored: serving never has it. Same values, different rows.)
Same-day activity is the stream features' job (Flink, 1 hour).

  txn_count_30d, distinct_merchants_30d         settled transactions in the 30 days up to L
  avg_amount_90d, std_amount_90d                amount_vnd over the 90 days up to L (NULL if none)
  ecom_ratio_90d, foreign_txn_ratio_90d         share of online / cross-border (currency != VND) payments
  dispute_count_180d                            disputes *reported* in the 180 days up to L (any reason)

Disputes are counted by reported_ts, not by the transaction date: a dispute reported after L was unknown on L + 1.

Usage (repo root):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/build_features.py
"""
from pyspark.sql import Column, SparkSession, Window
from pyspark.sql import functions as F

TABLE = "gold.feat_card_txn_stats_daily"
PATH = "s3a://lakehouse/gold/feat_card_txn_stats_daily"
EPOCH = "2024-01-01"
SAMPLE = 500   # rows re-computed by brute force as the leakage check
FEATURES = ["txn_count_30d", "distinct_merchants_30d", "avg_amount_90d", "std_amount_90d", "ecom_ratio_90d",
            "foreign_txn_ratio_90d", "dispute_count_180d"]


def build(spark: SparkSession):
    """Build feat_card_txn_stats_daily from the fact and the disputes (a row the day after each active day)."""
    fact = spark.table("gold.fact_transaction")
    day = F.to_date("txn_ts")
    txn = (fact.groupBy("card_id", day.alias("d"))
           .agg(F.count("*").alias("n"), F.sum(F.col("amount_vnd").cast("double")).alias("s"),
                F.sum(F.pow(F.col("amount_vnd").cast("double"), 2)).alias("s2"),
                F.sum((F.col("channel") == "ecom").cast("int")).alias("ecom"),
                F.sum((F.col("currency") != "VND").cast("int")).alias("foreign"),
                F.collect_set("merchant_id").alias("merchants")))
    disputes = (spark.table("bronze.raw_disputes").join(fact.select("transaction_id", "card_id"), "transaction_id")
                .groupBy("card_id", F.to_date("reported_ts").alias("d")).agg(F.count("*").alias("k")))
    days = (txn.join(disputes, ["card_id", "d"], "full")
            .fillna(0, ["n", "s", "s2", "ecom", "foreign", "k"])
            .withColumn("dn", F.datediff("d", F.lit(EPOCH))))

    def past(days_back: int) -> Window:   # the `days_back` days up to L, L included (= before the row's day L + 1)
        """Window over the `days_back` days ending at L for the same card."""
        return Window.partitionBy("card_id").orderBy("dn").rangeBetween(-(days_back - 1), 0)

    def total(c: str, days_back: int) -> Column:
        """Sum of column c over the `days_back` days ending at L."""
        return F.coalesce(F.sum(c).over(past(days_back)), F.lit(0))

    n90 = total("n", 90)
    avg = total("s", 90) / n90
    feats = (days.select(
        "card_id", "d", "n", "k",
        total("n", 30).cast("int").alias("txn_count_30d"),
        F.size(F.array_distinct(F.flatten(F.collect_list(F.coalesce("merchants", F.array())).over(past(30)))))
         .alias("distinct_merchants_30d"),
        F.when(n90 > 0, avg).alias("avg_amount_90d"),
        F.when(n90 > 0, F.sqrt(F.greatest(total("s2", 90) / n90 - avg * avg, F.lit(0.0)))).alias("std_amount_90d"),
        F.when(n90 > 0, total("ecom", 90) / n90).alias("ecom_ratio_90d"),
        F.when(n90 > 0, total("foreign", 90) / n90).alias("foreign_txn_ratio_90d"),
        total("k", 180).cast("int").alias("dispute_count_180d"))
        .where("n > 0 OR k > 0").drop("n", "k")
        .withColumn("event_timestamp", F.date_add("d", 1).cast("timestamp_ntz"))
        .withColumn("stats_ts", F.col("event_timestamp"))   # as a feature: Layer 2 derives days_since_last_active
        .withColumn("created", F.current_timestamp())
        .drop("d"))
    # ponytail: full rebuild (~7.6M rows); incremental for Airflow = recompute the new days from 180 days of history
    feats.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(PATH)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} USING DELTA LOCATION '{PATH}'")


def check(spark: SparkSession) -> None:
    """Re-compute SAMPLE random rows straight from the fact with a plain date filter over [E - 30, E - 1], E the
    row's event day; any difference means a window includes day E or later (leakage) or misses a day."""
    f = spark.table(TABLE).orderBy(F.rand(1)).limit(SAMPLE).withColumn("d", F.to_date("event_timestamp"))
    fact = spark.table("gold.fact_transaction")
    t = fact.select("card_id", F.to_date("txn_ts").alias("td"), "merchant_id")
    dsp = (spark.table("bronze.raw_disputes").join(fact.select("transaction_id", "card_id"), "transaction_id")
           .select("card_id", F.to_date("reported_ts").alias("rd")))
    in30 = (F.col("td") >= F.date_sub("d", 30)) & (F.col("td") < F.col("d"))
    txn_side = (f.join(t, "card_id", "left").groupBy("card_id", "d")
                .agg(F.count(F.when(in30, 1)).alias("n30"), F.count_distinct(F.when(in30, F.col("merchant_id"))).alias("m30")))
    dsp_side = (f.join(dsp, "card_id", "left").groupBy("card_id", "d")
                .agg(F.count(F.when((F.col("rd") >= F.date_sub("d", 180)) & (F.col("rd") < F.col("d")), 1)).alias("k180")))
    bad = (f.join(txn_side, ["card_id", "d"]).join(dsp_side, ["card_id", "d"])
           .where("n30 != txn_count_30d OR m30 != distinct_merchants_30d OR k180 != dispute_count_180d").count())
    assert bad == 0, f"{bad} of {SAMPLE} sampled rows differ from the brute-force recount"
    print(f"check: {SAMPLE} sampled rows match a brute-force recount (no same-day leakage)")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dp3_build_features").getOrCreate()
    build(spark)
    out = spark.table(TABLE)
    print(f"{TABLE}: {out.count():,} rows, {out.select('card_id').distinct().count():,} cards")
    out.select([F.round(F.avg(c), 4).alias(c) for c in FEATURES]).show(vertical=True)   # means, a sanity look
    check(spark)
    spark.stop()
