"""Training data: labels as known on a date -> entity rows -> Feast point-in-time features -> a versioned training set.

  labels      gold.label_transaction, the Delta version written as of `label_as_of` (time travel), mature rows only
  entities    + the payment's request fields (silver) and device (bronze authorizations); negatives of the train
              period downsampled by a hash of transaction_id (the same rows in every run), validation / test kept whole
  features    Feast get_historical_features(feature_service=fraud_model_v1) on the Spark offline store
  versioning  MERGE into the Delta table ml/training_set_<service>: a run whose data barely changed rewrites only the
              files holding changed rows, and `VERSION AS OF` rebuilds any run's training set exactly
"""
import datetime as dt
from dataclasses import dataclass

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fraud_ml.split import TimeSplitter

LAKE = "s3a://lakehouse"
LABELS = f"{LAKE}/gold/label_transaction"
TRANSACTIONS = f"{LAKE}/silver/stg_transactions"
AUTHORIZATIONS = f"{LAKE}/bronze/raw_card_authorizations"
FEATURE_TABLES = ["feat_card_txn_stats_daily", "dim_card", "feat_card_auth_rt", "feat_card_device"]
NO_DEVICE = "-"   # same as feature_repo: non-ecom payments have no device
KEY = "transaction_id"
LABEL = "is_fraud"
WEIGHT = "sample_weight"
ROW_HASH = "row_hash"   # training-set table only: hash of every column but the key (incremental MERGE)


def delta_version(spark: SparkSession, path: str) -> int:
    """Current version of a Delta table."""
    return DeltaTable.forPath(spark, path).history(1).first()["version"]


@dataclass
class TrainingDataService:
    """Builds the training set of one pipeline run. Every input is pinned to a Delta version, recorded for MLflow."""

    spark: SparkSession
    label_as_of: dt.date
    negative_rate: float = 0.02
    feature_service: str = "fraud_model_v1"

    def label_version(self) -> int:
        """Version of gold.label_transaction written by the build_labels run as of `label_as_of` (or the last one
        before it). build_labels tags each commit with userMetadata label_as_of=D."""
        hist = (DeltaTable.forPath(self.spark, LABELS).history().select("version", "userMetadata")
                .where(F.col("userMetadata").startswith("label_as_of=")).collect())
        ok = [(r.version, r.userMetadata) for r in hist if dt.date.fromisoformat(r.userMetadata[12:]) <= self.label_as_of]
        if not ok:
            raise ValueError(f"no label version as of {self.label_as_of}")
        return max(ok)[0]

    def data_versions(self) -> dict[str, int]:
        """Delta versions of every input table, logged as MLflow params."""
        out = {"label_transaction": self.label_version()}
        for t in FEATURE_TABLES:
            out[t] = delta_version(self.spark, f"{LAKE}/gold/{t}")
        return out

    def labels(self) -> DataFrame:
        """The 2-column label table (transaction_id, is_fraud) of mature transactions, as known on label_as_of."""
        split = TimeSplitter(self.label_as_of)
        lab = self.spark.read.format("delta").option("versionAsOf", self.label_version()).load(LABELS)
        return lab.where(F.col("event_timestamp") < F.lit(split.mature_end)).select(KEY, F.col(LABEL).cast("int").alias(LABEL))

    def entities(self) -> DataFrame:
        """Entity rows for Feast: keys, timestamps, request fields, label, split and sample weight.

        event_timestamp = txn_ts - 1 ms: the point-in-time join then sees only feature rows written before this
        payment, exactly what the online store holds when the API scores it (Flink updates arrive after)."""
        split = TimeSplitter(self.label_as_of)
        txn = (self.spark.read.format("delta").load(TRANSACTIONS)
               .select(KEY, "card_id", "txn_ts", "amount_vnd", "channel", "currency", "auth_method"))
        dev = (self.spark.read.format("delta").load(AUTHORIZATIONS)
               .select(F.regexp_replace("auth_id", "^auth_", "txn_").alias(KEY), "device_id")
               .dropDuplicates([KEY]))
        rows = (self.labels().join(txn, KEY).join(dev, KEY, "left")
                .withColumn("device_id", F.coalesce("device_id", F.lit(NO_DEVICE)))
                .withColumn("split", split.column(F.to_date("txn_ts")))
                .where(F.col("split").isNotNull()))   # embargo days belong to no split
        # Hash, not rand(): the same negatives are kept by every run, so the versioned set changes only where data did.
        keep = (F.col(LABEL) == 1) | (F.col("split") != "train") | \
               ((F.abs(F.xxhash64(KEY)) % 1_000_000) < int(self.negative_rate * 1_000_000))
        weight = F.when((F.col("split") == "train") & (F.col(LABEL) == 0), 1 / self.negative_rate).otherwise(1.0)
        return (rows.where(keep).withColumn(WEIGHT, weight)
                .withColumn("request_ts", F.col("txn_ts"))
                .withColumn("event_timestamp", F.col("txn_ts") - F.expr("INTERVAL 1 MILLISECOND"))
                .drop("txn_ts"))

    def historical_features(self, entities: DataFrame, store) -> pd.DataFrame:
        """Feast point-in-time join of every feature of the feature service (on-demand features included)."""
        service = store.get_feature_service(self.feature_service)
        return store.get_historical_features(entity_df=entities, features=service).to_df()

    def save_versioned(self, df: pd.DataFrame, path: str) -> int:
        """MERGE the run's rows into the training-set table; returns the Delta version holding exactly them.

        Rows match on the key AND a hash of every other column: an unchanged row matches and stays where it is; a
        new or changed row is inserted, a row gone (or the old copy of a changed one) is deleted. With deletion
        vectors a delete only marks the row, so a version stores the inserted rows plus small DV files. Without
        them Delta rewrote every file of the table on each MERGE (all 108 files when the windows moved by 18 days;
        all 3 for a 1-day shift with 15k rows changed)."""
        cols = [c for c in df.columns if c != KEY]
        # ponytail: 64-bit hash; a collision between a row's old and new values would miss that one update
        sdf = self.spark.createDataFrame(df).withColumn(ROW_HASH, F.xxhash64(*cols))
        if not DeltaTable.isDeltaTable(self.spark, path):
            sdf.write.format("delta").option("delta.enableDeletionVectors", "true").save(path)
        else:
            (DeltaTable.forPath(self.spark, path).alias("t")
             .merge(sdf.alias("s"), f"t.{KEY} = s.{KEY} AND t.{ROW_HASH} = s.{ROW_HASH}")
             .whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute())
        return delta_version(self.spark, path)
