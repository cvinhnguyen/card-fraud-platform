"""Feast definitions of the card fraud model (Layer 2): entities, sources, feature views, feature service.

Every source is a Delta table of the lakehouse, read by the Spark offline store:
  card_txn_stats_daily  gold.feat_card_txn_stats_daily  DP3 (Spark), a row after each active day  TTL none
  card_profile          gold.dim_card                   dbt, static card attributes                TTL none
  card_auth_realtime    gold.feat_card_auth_rt          Flink rt_features.py -> push jobs A / B    TTL 1 hour
  card_device           gold.feat_card_device           Flink device_seen.py -> push jobs A / B    TTL none
  fraud_request_features  on demand: the payment being scored (request) + the views above

TTL = how old a value may be and still describe the card (docs/feature_store.md). One fact shapes the choices:
Feast's Redis lookup returns the stored value however old it is (TTL bounds the point-in-time join and the
materialization window, not online reads). Any ageing the model must see is therefore done in the on-demand view,
from the value's own timestamp, so training and serving age it the same way:
  - daily stats: none. DP3 writes a card's row the night after each day it was active; however old, it is the
    card's latest history. Its age is a feature (days_since_last_active) instead of a cut-off
  - realtime: 1 hour, the longest window. The point-in-time join drops older rows (NULL); online, the on-demand
    view turns a value older than its own window into 0 ("no authorization in the last 10 min / hour"), and NULL
    into 0 the same way, so both paths give the model the same number
  - card attributes and "first time this card used this device" never expire
Timestamps of every source are naive UTC wall clock (feature_store.yaml sets the Spark session to UTC).
Non-ecom payments have no device: they use device_id "-", which never has a card_device row.
"""
from datetime import timedelta

import pandas as pd
from feast import Entity, FeatureService, FeatureView, Field, PushSource, RequestSource
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.table_format import DeltaFormat
from feast.types import Float64, Int64, String, UnixTimestamp

GOLD = "s3a://lakehouse/gold"
NO_DEVICE = "-"

card = Entity(name="card", join_keys=["card_id"], description="A payment card")
device = Entity(name="device", join_keys=["device_id"], description="Phone used for online payments")


def delta(table: str, timestamp: str = "event_timestamp", created: str | None = None) -> SparkSource:
    """Delta table gold.<table>, read by path (no Hive Metastore needed). file_format is the file type under the
    Delta log; Feast reads through the log because of table_format."""
    return SparkSource(name=table, path=f"{GOLD}/{table}", file_format="parquet", table_format=DeltaFormat(),
                       timestamp_field=timestamp, created_timestamp_column=created)


card_txn_stats_daily = FeatureView(
    name="card_txn_stats_daily", entities=[card], ttl=timedelta(0),
    schema=[Field(name="txn_count_30d", dtype=Int64), Field(name="distinct_merchants_30d", dtype=Int64),
            Field(name="avg_amount_90d", dtype=Float64), Field(name="std_amount_90d", dtype=Float64),
            Field(name="ecom_ratio_90d", dtype=Float64), Field(name="foreign_txn_ratio_90d", dtype=Float64),
            Field(name="dispute_count_180d", dtype=Int64), Field(name="stats_ts", dtype=UnixTimestamp)],
    source=delta("feat_card_txn_stats_daily", created="created"))

card_profile = FeatureView(
    name="card_profile", entities=[card], ttl=timedelta(0),   # 0 = never expires
    schema=[Field(name="card_type", dtype=String), Field(name="network", dtype=String),
            Field(name="card_issue_ts", dtype=UnixTimestamp)],
    source=SparkSource(name="dim_card", timestamp_field="event_timestamp",
                       query=f"SELECT card_id, card_type, network, issue_ts AS card_issue_ts, issue_ts AS event_timestamp "
                             f"FROM delta.`{GOLD}/dim_card`"))

card_auth_realtime = FeatureView(
    name="card_auth_realtime", entities=[card], ttl=timedelta(hours=1),
    schema=[Field(name="auth_count_10m", dtype=Int64), Field(name="amount_sum_1h", dtype=Int64),
            Field(name="distinct_merchants_1h", dtype=Int64), Field(name="distinct_units_1h", dtype=Int64),
            Field(name="rt_ts", dtype=UnixTimestamp)],   # = event_timestamp, readable by the on-demand view
    source=PushSource(name="card_auth_rt_push", batch_source=delta("feat_card_auth_rt")))

card_device = FeatureView(
    name="card_device", entities=[card, device], ttl=timedelta(0),
    schema=[Field(name="first_seen_ts", dtype=UnixTimestamp)],
    source=PushSource(name="card_device_push", batch_source=delta("feat_card_device")))

# What the authorization switch sends with the payment being scored.
auth_request = RequestSource(name="auth_request", schema=[
    Field(name="amount_vnd", dtype=Int64), Field(name="channel", dtype=String), Field(name="currency", dtype=String),
    Field(name="auth_method", dtype=String), Field(name="request_ts", dtype=UnixTimestamp)])


@on_demand_feature_view(
    sources=[card_txn_stats_daily, card_profile, card_auth_realtime, card_device, auth_request],
    schema=[Field(name="auth_count_10m_now", dtype=Int64), Field(name="amount_sum_1h_now", dtype=Int64),
            Field(name="distinct_merchants_1h_now", dtype=Int64), Field(name="distinct_units_1h_now", dtype=Int64),
            Field(name="days_since_last_active", dtype=Float64),
            Field(name="amount", dtype=Float64), Field(name="amount_to_avg_ratio", dtype=Float64),
            Field(name="days_since_card_issue", dtype=Float64), Field(name="is_new_device", dtype=Int64),
            Field(name="is_ecom", dtype=Int64), Field(name="is_foreign", dtype=Int64),
            Field(name="is_night", dtype=Int64), Field(name="is_otp", dtype=Int64)],
    mode="pandas")
def fraud_request_features(df: pd.DataFrame) -> pd.DataFrame:
    """Features that need the request: anything relative to now or to this payment is computed here, per request,
    from stored timestamps (a stored "days since" is wrong the next day).
    Self-contained on purpose: Feast stores this function's source in the registry and runs it from there, so
    module-level helpers and imports are not available to it."""
    import numpy as np
    import pandas as pd
    utc = lambda s: pd.to_datetime(s, utc=True)   # noqa: E731  offline: tz-aware, online: naive or epoch
    out = pd.DataFrame(index=df.index)
    amount = df["amount_vnd"].astype("float64")
    avg = df["avg_amount_90d"].astype("float64")
    now = utc(df["request_ts"])
    ecom = df["channel"] == "ecom"
    # Realtime values were computed at the card's previous authorization: "now" they still hold only within their
    # window; past it there was no authorization in that window, i.e. 0. (Inside it, an upper bound: the oldest
    # authorizations may have slid out since.)
    age_s = (now - utc(df["rt_ts"])).dt.total_seconds()
    for name, window_s in (("auth_count_10m", 600), ("amount_sum_1h", 3600), ("distinct_merchants_1h", 3600),
                           ("distinct_units_1h", 3600)):
        fresh = age_s.notna() & (age_s <= window_s)
        out[f"{name}_now"] = df[name].where(fresh, 0).fillna(0).astype("int64")
    out["days_since_last_active"] = ((now - utc(df["stats_ts"])).dt.total_seconds() / 86400).astype("float64")
    out["amount"] = amount
    out["amount_to_avg_ratio"] = (amount / avg.where(avg > 0)).astype("float64")   # NaN for a card with no history
    out["days_since_card_issue"] = ((now - utc(df["card_issue_ts"])).dt.total_seconds() / 86400).astype("float64")
    out["is_new_device"] = (ecom & df["first_seen_ts"].isna()).astype("int64")
    out["is_ecom"] = ecom.astype("int64")
    out["is_foreign"] = (df["currency"] != "VND").astype("int64")
    out["is_night"] = (now.dt.hour < 6).astype("int64")
    out["is_otp"] = (df["auth_method"] == "3ds_otp").astype("int64")
    return out.replace([np.inf, -np.inf], np.nan)


# The exact feature list of the model. A model with other features gets fraud_model_v2; the deployed model and the
# features the API fetches for it can never drift apart.
fraud_model_v1 = FeatureService(name="fraud_model_v1", features=[
    card_txn_stats_daily[["txn_count_30d", "distinct_merchants_30d", "avg_amount_90d", "std_amount_90d",
                          "ecom_ratio_90d", "foreign_txn_ratio_90d", "dispute_count_180d"]],
    fraud_request_features])
