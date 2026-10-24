"""Account takeover patterns on app events with Flink CEP (SQL MATCH_RECOGNIZE), one job, three patterns:

  takeover          password_reset -> add_payee -> transfer within 10 minutes (what ato_sequence.py does by hand)
  blocked_takeover  password_reset -> a failed biometric_verify within 10 minutes, at the new-device check after the
                    login or at the amount check before a transfer: the identity check stopped a takeover attempt
                    (from 2024-07-01, Decision 2345)
  split_transfers   add_payee -> two transfers of 9,000,000-9,999,999 VND to that payee within 30 minutes: amounts
                    kept just below the 10M identity check, as malware on the victim's own phone does (no new device,
                    no password reset)

MATCH_RECOGNIZE orders each session's rows by event time and waits for the watermark (30 s behind the newest event)
before matching, so the 15% of events that reach Kafka late need no hand-written buffer. Writes one message per match
to `ato_alerts_cep`. pipelines/flink/check_ato_cep.py compares with the generator truth and with ato_sequence.py.

Usage (stream profile up; replay loaded into app_events_replay):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/ato_cep.py
"""
from pyflink.table import EnvironmentSettings, TableEnvironment

PATTERNS = {
    "takeover": """
        MEASURES R.ts AS first_ts, T.ts AS last_ts, T.payee_id AS payee_id, T.amount_vnd AS amount_vnd
        ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
        PATTERN (R X*? P Y*? T) WITHIN INTERVAL '10' MINUTE
        DEFINE R AS R.event_type = 'password_reset', P AS P.event_type = 'add_payee', T AS T.event_type = 'transfer'""",
    "blocked_takeover": """
        MEASURES R.ts AS first_ts, V.ts AS last_ts, CAST(NULL AS STRING) AS payee_id, CAST(NULL AS BIGINT) AS amount_vnd
        ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
        PATTERN (R X*? V) WITHIN INTERVAL '10' MINUTE
        DEFINE R AS R.event_type = 'password_reset',
               V AS V.event_type = 'biometric_verify' AND V.`result` = 'failed' AND V.reason IN ('new_device', 'amount')""",
    "split_transfers": """
        MEASURES A.ts AS first_ts, S2.ts AS last_ts, A.payee_id AS payee_id, S1.amount_vnd + S2.amount_vnd AS amount_vnd
        ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A X*? S1 Y*? S2) WITHIN INTERVAL '30' MINUTE
        DEFINE A AS A.event_type = 'add_payee',
               S1 AS S1.event_type = 'transfer' AND S1.payee_id = A.payee_id AND S1.amount_vnd BETWEEN 9000000 AND 9999999,
               S2 AS S2.event_type = 'transfer' AND S2.payee_id = A.payee_id AND S2.amount_vnd BETWEEN 9000000 AND 9999999""",
}

if __name__ == "__main__":
    t = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t.get_config().set("parallelism.default", "1")
    t.get_config().set("pipeline.name", "ato_cep")
    t.execute_sql("""
        CREATE TABLE app_events (
            session_id STRING, customer_id STRING, event_type STRING, device_id STRING, event_ts STRING,
            amount_vnd BIGINT, payee_id STRING, `result` STRING, reason STRING,   -- RESULT is a reserved word
            ts AS TO_TIMESTAMP(event_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
            WATERMARK FOR ts AS ts - INTERVAL '30' SECOND   -- events are at most 20 s out of order
        ) WITH (
            'connector' = 'kafka', 'topic' = 'app_events_replay',
            'properties.bootstrap.servers' = 'kafka:19092', 'properties.group.id' = 'ato_cep',
            'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset',
            'format' = 'json'
        )""")
    t.execute_sql("""
        CREATE TABLE ato_alerts_cep (
            session_id STRING, `pattern` STRING, first_ts TIMESTAMP(3), last_ts TIMESTAMP(3), payee_id STRING,
            amount_vnd BIGINT
        ) WITH (
            'connector' = 'kafka', 'topic' = 'ato_alerts_cep',
            'properties.bootstrap.servers' = 'kafka:19092', 'format' = 'json'
        )""")
    jobs = t.create_statement_set()   # one job: the source is read once and feeds the three pattern operators
    for name, body in PATTERNS.items():
        jobs.add_insert_sql(f"""
            INSERT INTO ato_alerts_cep
            SELECT session_id, '{name}', first_ts, last_ts, payee_id, amount_vnd
            FROM app_events MATCH_RECOGNIZE (PARTITION BY session_id ORDER BY ts {body})""")
    jobs.execute().wait()
