"""Create the S3 buckets and Kafka topics the platform needs. Idempotent.

Usage: python infra/init_infra.py   (after `docker compose --profile core up -d`)
"""
import boto3
from confluent_kafka.admin import AdminClient, NewTopic

from make_env import read_env

BUCKETS = ["core-banking-export", "lakehouse"]   # another department's exports | our bronze/silver/gold
TOPICS = {"card_authorizations": 6, "app_events": 6}   # name -> partitions
# DataHub (gov profile): its system-update creates the v1 topics, but GMS still consumes these legacy ones, and
# auto-create is off on our broker. Without them the MCE consumer loops on UNKNOWN_TOPIC_OR_PARTITION.
DATAHUB_TOPICS = ["MetadataChangeEvent_v4", "FailedMetadataChangeEvent_v4", "MetadataAuditEvent_v4"]


def create_buckets(env: dict[str, str]) -> None:
    """Create each bucket in SeaweedFS unless it already exists."""
    s3 = boto3.client("s3", endpoint_url="http://localhost:8333", region_name="us-east-1",
                      aws_access_key_id=env["S3_ACCESS_KEY"], aws_secret_access_key=env["S3_SECRET_KEY"])
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for name in BUCKETS:
        if name not in existing:
            s3.create_bucket(Bucket=name)
        print(f"bucket {name}: ok")


def create_topics() -> None:
    """Create each Kafka topic unless it already exists."""
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    # Messages carry their event time (2024-2026) as Kafka timestamp: time-based retention would delete them at once.
    existing = admin.list_topics(timeout=10).topics
    missing = [NewTopic(t, num_partitions=p, replication_factor=1, config={"retention.ms": "-1"})
               for t, p in TOPICS.items() if t not in existing]
    missing += [NewTopic(t, num_partitions=1, replication_factor=1) for t in DATAHUB_TOPICS if t not in existing]
    for future in (admin.create_topics(missing).values() if missing else []):
        future.result()   # raises on failure
    for t in [*TOPICS, *DATAHUB_TOPICS]:
        print(f"topic {t}: ok")


if __name__ == "__main__":
    create_buckets(read_env())
    create_topics()
