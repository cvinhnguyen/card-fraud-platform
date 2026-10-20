"""Smoke test of the deployed push jobs: one authorization summary sent to Kafka must reach the online store (Redis,
read through Feast) and the offline store (the Delta table behind the push source) within the timeout.

The card id is synthetic (ci_smoke_<random>): one marked row per deployment, never a real card.
Usage (ml image on the compose network; env ML_DB_PASSWORD, REDIS_PASSWORD, AWS_*):
  python ml/tools/stream_smoke.py --repo /opt/ml/feature_repo [--timeout 120]
"""
import argparse
import json
import os
import sys
import time
import uuid

import pyarrow.compute as pc
from confluent_kafka import Producer
from deltalake import DeltaTable
from feast import FeatureStore

TOPIC = "card_features_rt"


def main(repo: str, timeout_s: float) -> int:
    """Send one message, then poll both stores; 0 when both have it in time."""
    sys.path.insert(0, repo)
    import fraud_features
    path = fraud_features.card_auth_realtime.batch_source.path.replace("s3a://", "s3://")
    opts = {"AWS_ENDPOINT_URL": "http://lakehouse-s3:8333", "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"]}
    store = FeatureStore(repo_path=repo)
    card = f"ci_smoke_{uuid.uuid4().hex[:8]}"
    msg = {"card_id": card, "event_timestamp": int(time.time() * 1000), "auth_count_10m": 7, "amount_sum_1h": 123_456,
           "distinct_merchants_1h": 1, "distinct_units_1h": 1}
    producer = Producer({"bootstrap.servers": "kafka:19092"})
    producer.produce(TOPIC, json.dumps(msg).encode())
    producer.flush(10)
    start, online_s, offline_s = time.monotonic(), None, None
    while time.monotonic() - start < timeout_s and (online_s is None or offline_s is None):
        if online_s is None:
            got = store.get_online_features(features=["card_auth_realtime:auth_count_10m"],
                                            entity_rows=[{"card_id": card}]).to_dict()["auth_count_10m"][0]
            online_s = time.monotonic() - start if got == 7 else None
        if offline_s is None:
            rows = DeltaTable(path, storage_options=opts).to_pyarrow_dataset().to_table(
                filter=pc.field("card_id") == card).num_rows
            offline_s = time.monotonic() - start if rows else None
        time.sleep(2)
    print(f"{card}: online {'%.1f s' % online_s if online_s is not None else 'MISSING'}, "
          f"offline {'%.1f s' % offline_s if offline_s is not None else 'MISSING'} ({path})")
    return 0 if online_s is not None and offline_s is not None else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", default="/opt/ml/feature_repo")
    ap.add_argument("--timeout", type=float, default=120)
    a = ap.parse_args()
    sys.exit(main(a.repo, a.timeout))
