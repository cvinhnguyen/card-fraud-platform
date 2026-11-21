"""Raw authorizations, exactly as received (duplicates and late events included), -> S3 landing zone.

  Kafka card_authorizations -> s3://lakehouse/landing/card_authorizations/auth_date=YYYY-MM-DD/  (JSON lines)

Nothing is cleaned here: the raw copy is what replays, audits and lineage go back to. Kafka partition and offset are
kept so any row can be traced to its message. pipelines/spark/ingest_bronze.py then loads the landing files into the Delta
table bronze.raw_card_authorizations.
ponytail: JSON, not Parquet. Flink's Parquet writer needs the Hadoop client jars on the classpath; JSON needs none,
and Spark converts it to Delta in the next step anyway.

A part file is closed at 64 MB or 30 minutes after it opened (the rollover default) and becomes visible on the next
checkpoint (every 10 s). Until then it is an unfinished S3 multipart upload, so a reader never sees a half-written file.

Usage (stream profile up):
  docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/raw_landing.py --topic card_authorizations --live   # live
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/raw_landing.py              # backfill: whole topic, then stops
  ... raw_landing.py --from-ms 1792430000000   # backfill: only messages appended since then (Kafka time), then stops
--live is unbounded and resumes from the group's committed offsets (the whole topic on the first start). Run a
backfill only while the live job is stopped: both would land the same messages.
"""
import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--topic", default="card_authorizations")
    ap.add_argument("--live", action="store_true", help="unbounded, resume from the group's committed offsets")
    ap.add_argument("--from-ms", type=int, help="start at this Kafka timestamp (epoch ms) instead of the earliest offset")
    ap.add_argument("--release", default="", help="code version, added to the job name (pipelines/flink/Jenkinsfile)")
    args = ap.parse_args()
    if args.from_ms:
        start = f"'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '{args.from_ms}'"
    elif args.live:   # no committed offsets yet (first start): from the earliest offset
        start = "'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest'"
    else:
        start = "'scan.startup.mode' = 'earliest-offset'"
    end = "" if args.live else ", 'scan.bounded.mode' = 'latest-offset'"
    t = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t.get_config().set("execution.checkpointing.interval", "10 s")   # files and Kafka offsets are committed on checkpoints
    t.get_config().set("parallelism.default", "1")
    if args.live:   # a backfill keeps Flink's default name, so the Jenkinsfile never upgrades it
        t.get_config().set("pipeline.name", f"raw_landing {args.release}".strip())   # the running job shows its version
    t.execute_sql(f"""
        CREATE TABLE kafka_auth (
            auth_id STRING, card_id STRING, merchant_id STRING, amount_vnd BIGINT, channel STRING,
            terminal_id STRING, device_id STRING, geo_city STRING, auth_ts STRING, received_ts STRING,
            kafka_partition INT METADATA FROM 'partition' VIRTUAL,
            kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
        ) WITH (
            'connector' = 'kafka', 'topic' = '{args.topic}',
            'properties.bootstrap.servers' = 'kafka:19092', 'properties.group.id' = 'raw_landing',
            {start}{end},
            'format' = 'json'
        )""")
    t.execute_sql("""
        CREATE TABLE landing (
            auth_id STRING, card_id STRING, merchant_id STRING, amount_vnd BIGINT, channel STRING,
            terminal_id STRING, device_id STRING, geo_city STRING, auth_ts STRING, received_ts STRING,
            kafka_partition INT, kafka_offset BIGINT, auth_date STRING
        ) PARTITIONED BY (auth_date) WITH (
            'connector' = 'filesystem', 'path' = 's3://lakehouse/landing/card_authorizations',
            'format' = 'json', 'sink.rolling-policy.file-size' = '64MB'
        )""")
    job = t.execute_sql("""
        INSERT INTO landing
        SELECT auth_id, card_id, merchant_id, amount_vnd, channel, terminal_id, device_id, geo_city, auth_ts,
               received_ts, kafka_partition, kafka_offset, SUBSTRING(auth_ts, 1, 10)
        FROM kafka_auth""")
    if not args.live:   # a live job never ends: `flink run -d` returns once it is submitted
        job.wait()
