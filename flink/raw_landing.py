"""Raw authorizations, exactly as received (duplicates and late events included), -> S3 landing zone.

  Kafka card_authorizations -> s3://lakehouse/landing/card_authorizations/auth_date=YYYY-MM-DD/  (JSON lines)

Nothing is cleaned here: the raw copy is what replays, audits and lineage go back to. Kafka partition and offset are
kept so any row can be traced to its message. spark/ingest_bronze.py then loads the landing files into the Delta
table bronze.raw_card_authorizations.
ponytail: JSON, not Parquet. Flink's Parquet writer needs the Hadoop client jars on the classpath; JSON needs none,
and Spark converts it to Delta in the next step anyway.

Usage (stream profile up):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/raw_landing.py              # whole topic, then stops
"""
from pyflink.table import EnvironmentSettings, TableEnvironment

if __name__ == "__main__":
    t = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t.get_config().set("execution.checkpointing.interval", "10 s")   # the file sink commits files on checkpoints
    t.get_config().set("parallelism.default", "1")
    t.execute_sql("""
        CREATE TABLE kafka_auth (
            auth_id STRING, card_id STRING, merchant_id STRING, amount_vnd BIGINT, channel STRING,
            terminal_id STRING, device_id STRING, geo_city STRING, auth_ts STRING, received_ts STRING,
            kafka_partition INT METADATA FROM 'partition' VIRTUAL,
            kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
        ) WITH (
            'connector' = 'kafka', 'topic' = 'card_authorizations',
            'properties.bootstrap.servers' = 'kafka:19092', 'properties.group.id' = 'raw_landing',
            'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset',
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
    t.execute_sql("""
        INSERT INTO landing
        SELECT auth_id, card_id, merchant_id, amount_vnd, channel, terminal_id, device_id, geo_city, auth_ts,
               received_ts, kafka_partition, kafka_offset, SUBSTRING(auth_ts, 1, 10)
        FROM kafka_auth""").wait()
