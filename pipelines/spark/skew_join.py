"""Merchant join skew: the same join four ways, timed, with the task skew of each run read from the Spark REST API.

  transactions JOIN merchants ON merchant_id   (the biggest merchant has ~24% of the rows)

  baseline   sort-merge join, AQE off, broadcast off: one task gets the whole hot key
  salting    every row gets a random salt in 0..SALTS-1, merchants copied once per salt: the hot key spreads over SALTS tasks
  aqe        AQE skew-join handling splits the oversized partitions at run time
  broadcast  merchants (5k rows) sent to every executor: no shuffle of transactions at all

Full transaction rows are joined, as a fact build would. `--replicate N` unions the data N times to get closer to
the 100 GB target: skew only costs wall time when the hot partition is bigger than total / cores and tasks are long
enough to hide Spark's fixed overhead. The join result goes to the `noop` sink (fully computed, nothing written).
Each method runs once to warm up, then RUNS times; the median is reported.

Usage (repo root, stop spark-thrift first so it does not hold worker cores):
  docker compose -f infra/docker-compose.yaml exec spark-master /opt/spark/bin/spark-submit /opt/jobs/skew_join.py
  ... skew_join.py --replicate 4                 # 32M rows
  ... skew_join.py --top-shares 0.3,0.6,0.8     # also re-assign merchant_id to other skew levels (in memory)
"""
import argparse
import json
import statistics
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RUNS = 3
WARMUP = True          # one extra, untimed run per method first
SKEW_THRESHOLD = "8m"  # AQE skew partition threshold; the 256 MB default targets big data, at 1 GB the hot partition is ~50 MB
ADVISORY = None        # AQE target partition size; None = same as SKEW_THRESHOLD (1 GB run), "64m" = Spark's default
SALTS = 32
TOP_N = 10
METHODS = ["baseline", "salting", "aqe", "broadcast"]


def configure(spark: SparkSession, method: str) -> None:
    """Session settings per method; everything else is identical across runs."""
    conf = {
        "spark.sql.adaptive.enabled": method == "aqe",
        "spark.sql.autoBroadcastJoinThreshold": -1,            # no automatic broadcast: it would hide the skew
        "spark.sql.adaptive.autoBroadcastJoinThreshold": -1,
        "spark.sql.adaptive.skewJoin.enabled": True,
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": SKEW_THRESHOLD,
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": ADVISORY or SKEW_THRESHOLD,
    }
    for k, v in conf.items():
        spark.conf.set(k, str(v).lower() if isinstance(v, bool) else str(v))


def join(txn: DataFrame, merchants: DataFrame, method: str) -> DataFrame:
    """transactions JOIN merchants on merchant_id with one of the four methods."""
    if method == "salting":
        txn = txn.withColumn("salt", (F.rand(7) * SALTS).cast("int"))
        merchants = merchants.crossJoin(txn.sparkSession.range(SALTS).withColumnRenamed("id", "salt"))
        return txn.join(merchants, ["merchant_id", "salt"]).drop("salt")
    if method == "broadcast":
        return txn.join(F.broadcast(merchants), "merchant_id")
    return txn.join(merchants, "merchant_id")


def task_skew(spark: SparkSession, group: str) -> tuple[float, float, float]:
    """(max task s, median task s, max shuffle read MB) of the slowest stage of the jobs in `group`."""
    sc = spark.sparkContext
    api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    get = lambda path: json.load(urllib.request.urlopen(api + path, timeout=30))
    stages = [s for j in get("/jobs") if j.get("jobGroup") == group for s in j["stageIds"]]
    best = (0.0, 0.0, 0.0)
    for sid in stages:
        for attempt in get(f"/stages/{sid}"):
            if attempt["status"] != "COMPLETE":
                continue
            q = get(f"/stages/{sid}/{attempt['attemptId']}/taskSummary?quantiles=0.5,1.0")
            run = q["executorRunTime"]
            read = q.get("shuffleReadMetrics", {}).get("readBytes", [0, 0])
            if run[1] / 1000 > best[0]:
                best = (run[1] / 1000, run[0] / 1000, read[1] / 2**20)
    return best


def with_top_share(txn: DataFrame, top: list[str], others: list[str], share: float) -> DataFrame:
    """Re-assign merchant_id so the top-N merchants get `share` of the rows (their relative sizes are kept)."""
    is_top = F.col("merchant_id").isin(top)
    pick_top = F.element_at(F.array(*map(F.lit, top)), (F.rand(1) * len(top)).cast("int") + 1)
    pick_other = F.element_at(F.array(*map(F.lit, others)), (F.rand(2) * len(others)).cast("int") + 1)
    # ponytail: rows moved into the top group are spread evenly over the 10; fine for a skew-level sweep
    return txn.withColumn("merchant_id", F.when(F.rand(3) < share, F.when(is_top, F.col("merchant_id")).otherwise(pick_top))
                          .otherwise(F.when(is_top, pick_other).otherwise(F.col("merchant_id"))))


def bench(spark: SparkSession, txn: DataFrame, merchants: DataFrame, label: str, methods: list[str] = METHODS,
          stats: bool = True) -> None:
    """Time each method and print its task skew (max vs median task time, max shuffle read). A method whose job
    fails (at 100 GB the baseline's hot partition runs out of memory) is reported as failed and the next one runs.
    stats=False skips the full counting pass behind the header line."""
    if stats:
        counts = txn.groupBy("merchant_id").count()
        n, hot = counts.agg(F.sum("count"), F.max("count")).first()
        top = counts.where(F.col("merchant_id").isin(TOP)).agg(F.sum("count")).first()[0]
        print(f"\n== {label}: {n:,} rows, biggest merchant {hot / n:.1%}, top-{TOP_N} {top / n:.1%}, "
              f"{spark.sparkContext.defaultParallelism} cores")
    print(f"{'method':<10} {'median s':>9} {'runs s':<22} {'max task s':>10} {'median task s':>13} {'max read MB':>11}")
    for method in methods:
        configure(spark, method)
        times = []
        for i in range(0 if WARMUP else 1, RUNS + 1):   # run 0 = warm-up, not counted
            group = f"{label}-{method}-{i}"
            spark.sparkContext.setJobGroup(group, group)
            t0 = time.perf_counter()
            try:
                join(txn, merchants, method).write.format("noop").mode("overwrite").save()
            except Exception as e:   # noqa: BLE001 - Py4JJavaError; the cause is the first line of the Java error
                cause = next((l.strip() for l in str(e).splitlines() if "Error" in l or "Exception" in l), repr(e))
                print(f"{method:<10} FAILED after {time.perf_counter() - t0:.0f} s: {cause[:200]}", flush=True)
                break
            if i:
                times.append(time.perf_counter() - t0)
        if not times:
            continue
        time.sleep(2)   # let the listener finish before reading the REST API
        mx, med, read = task_skew(spark, group)
        print(f"{method:<10} {statistics.median(times):>9.1f} {', '.join(f'{t:.1f}' for t in times):<22} "
              f"{mx:>10.1f} {med:>13.2f} {read:>11.1f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--replicate", type=int, default=1, help="union the transactions N times")
    ap.add_argument("--top-shares", help="extra skew levels, e.g. 0.3,0.6,0.8 (share of rows in the top-10 merchants)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("p4_skew_join").getOrCreate()
    txn = spark.table("silver.stg_transactions")
    for _ in range(args.replicate - 1):
        txn = txn.unionByName(spark.table("silver.stg_transactions"))
    merchants = spark.table("bronze.raw_merchants").select("merchant_id", "merchant_name", "mcc").cache()
    merchants.count()
    TOP = [r.merchant_id for r in txn.groupBy("merchant_id").count().orderBy(F.desc("count")).limit(TOP_N).collect()]
    bench(spark, txn, merchants, f"as generated x{args.replicate}")
    if args.top_shares:
        others = [r.merchant_id for r in merchants.where(~F.col("merchant_id").isin(TOP)).collect()]
        for share in map(float, args.top_shares.split(",")):
            bench(spark, with_top_share(txn, TOP, others, share), merchants, f"top-{TOP_N} {share:.0%}")
    spark.stop()
