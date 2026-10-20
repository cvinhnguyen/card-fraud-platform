"""Feast push jobs: Flink's streaming features (Kafka) -> Feast online store (job A) or offline store (job B).

  card_features_rt  (pipelines/flink/rt_features.py)  -> push source card_auth_rt_push  -> view card_auth_realtime
  card_device_seen  (pipelines/flink/device_seen.py)  -> push source card_device_push   -> view card_device

Job A (--to online) writes Redis through `FeatureStore.push`, within seconds. Job B (--to offline) appends to the
Delta table behind the push source (the offline store), so training reads the values serving saw. Job B writes
with the `deltalake` library: Feast's Spark offline write appends plain Parquet next to the Delta log, which Delta
readers ignore, and a JVM per push job is heavy anyway. Separate consumer groups and processes: a slow Delta append
never delays online freshness. At-least-once: offsets are committed after the write, so a crash re-writes the last
batch (Redis keeps the newest timestamp per key; a duplicate offline row has identical values).

Usage (ml image, compose network or kind pod; env ML_DB_PASSWORD, REDIS_PASSWORD, AWS_* for offline):
  python ml/jobs/push_job.py --to online
  python ml/jobs/push_job.py --to offline --batch 200000 --until-idle 60     # backfill: stop after 60 s without data
  python ml/jobs/push_job.py --to offline --until-idle 1                      # first run: creates the empty tables
"""
import argparse
import json
import os
import time

import pandas as pd
import pyarrow as pa
from confluent_kafka import Consumer

KAFKA = "kafka:19092"
TS = pa.timestamp("us", tz="UTC")
TOPICS = {   # topic -> (push source, schema of its Delta batch source = the columns written, in this order)
    "card_features_rt": ("card_auth_rt_push", pa.schema([
        ("card_id", pa.string()), ("event_timestamp", TS), ("rt_ts", TS), ("auth_count_10m", pa.int64()),
        ("amount_sum_1h", pa.int64()), ("distinct_merchants_1h", pa.int64()), ("distinct_units_1h", pa.int64())])),
    "card_device_seen": ("card_device_push", pa.schema([
        ("card_id", pa.string()), ("device_id", pa.string()), ("event_timestamp", TS), ("first_seen_ts", TS)])),
}


def to_frame(values: list[str], schema: pa.Schema) -> pd.DataFrame:
    """JSON messages -> the push source's columns; epoch-ms timestamps -> UTC datetimes."""
    df = pd.DataFrame.from_records([json.loads(v) for v in values])
    if "rt_ts" in schema.names:   # the feature's own timestamp, so on-demand features can age it at request time
        df["rt_ts"] = df["event_timestamp"]
    df = df[schema.names]
    for f in schema:
        if f.type == TS:
            df[f.name] = pd.to_datetime(df[f.name], unit="ms", utc=True)
    return df


class OnlineSink:
    """Job A: Feast push to the online store (Redis)."""

    def __init__(self, repo: str):
        """Open the feature store of `repo`."""
        from feast import FeatureStore
        from feast.data_source import PushMode
        self.store, self.mode = FeatureStore(repo_path=repo), PushMode.ONLINE

    def write(self, source: str, df: pd.DataFrame, schema: pa.Schema) -> None:
        """Push one batch; Redis keeps the newest event_timestamp per entity."""
        self.store.push(source, df, to=self.mode)


class OfflineSink:
    """Job B: append to the Delta table behind each push source (Feast's offline store reads it)."""

    def __init__(self, repo: str):
        """Resolve the Delta path of every push source; create the empty tables on the first run."""
        import sys
        from deltalake import DeltaTable, write_deltalake
        sys.path.insert(0, repo)
        import fraud_features   # the definitions, not the registry: the tables must exist before `feast apply`
        sources = {fv.stream_source.name: fv.batch_source for fv in
                   (fraud_features.card_auth_realtime, fraud_features.card_device)}
        self.write_deltalake = write_deltalake
        self.options = {"AWS_ENDPOINT_URL": "http://lakehouse-s3:8333", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
                        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                        # S3 has no atomic rename: safe here because this job is the only writer of these tables
                        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
        self.paths = {}
        for source, schema in TOPICS.values():
            path = sources[source].path.replace("s3a://", "s3://")
            self.paths[source] = path
            if not DeltaTable.is_deltatable(path, storage_options=self.options):
                write_deltalake(path, pa.Table.from_pylist([], schema=schema), storage_options=self.options)
                print(f"created {path}", flush=True)

    def write(self, source: str, df: pd.DataFrame, schema: pa.Schema) -> None:
        """Append one batch as one Delta commit."""
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        self.write_deltalake(self.paths[source], table, mode="append", storage_options=self.options)


def run(sink, group: str, batch: int, max_wait_s: float, until_idle_s: float | None) -> None:
    """Consume both topics forever (or until idle); write one batch per topic at most every max_wait_s."""
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": group, "auto.offset.reset": "earliest",
                         "enable.auto.commit": False})
    consumer.subscribe(list(TOPICS))
    pending: dict[str, list[str]] = {t: [] for t in TOPICS}
    last_flush = last_data = time.monotonic()
    written = 0
    while True:
        msgs = consumer.consume(num_messages=min(batch, 10_000), timeout=1.0)
        for m in msgs:
            if m.error():
                raise RuntimeError(m.error())
            pending[m.topic()].append(m.value().decode())
        now = time.monotonic()
        if msgs:
            last_data = now
        full = any(len(v) >= batch for v in pending.values())
        if full or (now - last_flush >= max_wait_s and any(pending.values())):
            for topic, values in pending.items():
                if values:
                    source, schema = TOPICS[topic]
                    sink.write(source, to_frame(values, schema), schema)
                    written += len(values)
            consumer.commit(asynchronous=False)
            print(f"{group}: {written:,} rows written", flush=True)
            pending = {t: [] for t in TOPICS}
            last_flush = now
        if until_idle_s and now - last_data > until_idle_s and not any(pending.values()):
            break
    consumer.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--to", choices=["online", "offline"], required=True)
    ap.add_argument("--repo", default="feature_repo")
    ap.add_argument("--batch", type=int, default=5_000, help="rows per write and topic")
    ap.add_argument("--max-wait", type=float, default=2.0, help="seconds before a partial batch is written")
    ap.add_argument("--until-idle", type=float, help="stop after this many seconds without messages (backfill)")
    a = ap.parse_args()
    sink = OnlineSink(a.repo) if a.to == "online" else OfflineSink(a.repo)
    run(sink, f"feast_push_{a.to}", a.batch, a.max_wait, a.until_idle)
