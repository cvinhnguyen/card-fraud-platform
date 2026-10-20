"""Push jobs (Kafka -> Feast online / offline store): message parsing, at-least-once batching, both sinks."""
import json

import pandas as pd
import pyarrow as pa
import pytest

import push_job

RT_SOURCE, RT_SCHEMA = push_job.TOPICS["card_features_rt"]
DEV_SOURCE, DEV_SCHEMA = push_job.TOPICS["card_device_seen"]
RT = {"card_id": "card_1", "event_timestamp": 1_792_500_000_000, "auth_count_10m": 2, "amount_sum_1h": 900_000,
      "distinct_merchants_1h": 2, "distinct_units_1h": 1, "extra": "dropped"}
DEV = {"card_id": "card_1", "device_id": "dev_9", "event_timestamp": 1_792_500_000_000, "first_seen_ts": 0}


def test_to_frame_realtime_takes_the_schema_columns_and_its_own_timestamp():
    df = push_job.to_frame([json.dumps(RT)], RT_SCHEMA)
    assert list(df.columns) == RT_SCHEMA.names               # schema order, unknown fields dropped
    assert df.rt_ts.iloc[0] == df.event_timestamp.iloc[0]    # rt_ts = event time, for on-demand ageing
    assert str(df.event_timestamp.dt.tz) == "UTC"
    assert df.event_timestamp.iloc[0] == pd.Timestamp(1_792_500_000_000, unit="ms", tz="UTC")


def test_to_frame_device_epoch_zero_boundary():
    df = push_job.to_frame([json.dumps(DEV)], DEV_SCHEMA)
    assert list(df.columns) == DEV_SCHEMA.names
    assert df.first_seen_ts.iloc[0] == pd.Timestamp(0, unit="ms", tz="UTC")


def test_to_frame_missing_field_fails():
    with pytest.raises(KeyError):
        push_job.to_frame([json.dumps({k: v for k, v in RT.items() if k != "auth_count_10m"})], RT_SCHEMA)


class Msg:
    def __init__(self, topic, value, error=None):
        self._t, self._v, self._e = topic, value, error

    def topic(self): return self._t
    def value(self): return json.dumps(self._v).encode()
    def error(self): return self._e


class FakeConsumer:
    """Hands out the given batches, then nothing; records subscribe / commit in order."""

    def __init__(self, conf, batches, log):
        self.batches, self.log = list(batches), log

    def subscribe(self, topics): self.log.append(("subscribe", sorted(topics)))
    def consume(self, num_messages, timeout): return self.batches.pop(0) if self.batches else []
    def commit(self, asynchronous): self.log.append(("commit",))
    def close(self): self.log.append(("close",))


class FakeSink:
    def __init__(self, log): self.log = log
    def write(self, source, df, schema): self.log.append(("write", source, len(df)))


def run(monkeypatch, batches, **kw):
    log = []
    monkeypatch.setattr(push_job, "Consumer", lambda conf: FakeConsumer(conf, batches, log))
    push_job.run(FakeSink(log), "g", kw.get("batch", 1000), kw.get("max_wait", 0.0), kw.get("until_idle", 0.01))
    return log


def test_run_writes_each_topic_then_commits_at_least_once(monkeypatch):
    log = run(monkeypatch, [[Msg("card_features_rt", RT), Msg("card_features_rt", RT), Msg("card_device_seen", DEV)]])
    assert log[0] == ("subscribe", ["card_device_seen", "card_features_rt"])
    assert log[1:4] == [("write", RT_SOURCE, 2), ("write", DEV_SOURCE, 1), ("commit",)]   # offsets after the write
    assert log[-1] == ("close",)


def test_run_full_batch_is_written_before_max_wait(monkeypatch):
    log = run(monkeypatch, [[Msg("card_features_rt", RT)] * 3], batch=3, max_wait=3600)
    assert ("write", RT_SOURCE, 3) in log and log.count(("commit",)) == 1


def test_run_stops_when_idle_without_writing_nothing(monkeypatch):
    assert run(monkeypatch, []) == [("subscribe", ["card_device_seen", "card_features_rt"]), ("close",)]


def test_run_kafka_error_stops_the_job(monkeypatch):
    with pytest.raises(RuntimeError):
        run(monkeypatch, [[Msg("card_features_rt", RT, error="broker down")]])


def test_online_sink_pushes_online():
    calls = []
    sink = object.__new__(push_job.OnlineSink)
    sink.store = type("S", (), {"push": lambda self, src, df, to: calls.append((src, len(df), to))})()
    sink.mode = "ONLINE"
    sink.write(RT_SOURCE, push_job.to_frame([json.dumps(RT)], RT_SCHEMA), RT_SCHEMA)
    assert calls == [(RT_SOURCE, 1, "ONLINE")]


def test_offline_sink_appends_one_delta_commit_in_the_schema():
    calls = []
    sink = object.__new__(push_job.OfflineSink)
    sink.paths, sink.options = {RT_SOURCE: "s3://lakehouse/x"}, {"k": "v"}
    sink.write_deltalake = lambda path, table, mode, storage_options: calls.append((path, table, mode))
    sink.write(RT_SOURCE, push_job.to_frame([json.dumps(RT)], RT_SCHEMA), RT_SCHEMA)
    (path, table, mode), = calls
    assert (path, mode, table.num_rows) == ("s3://lakehouse/x", "append", 1)
    assert table.schema.equals(RT_SCHEMA) and table.schema.field("rt_ts").type == pa.timestamp("us", tz="UTC")
