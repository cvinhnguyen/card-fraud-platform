"""Realtime card features (feature view `card_auth_realtime` of Layer 2) -> Kafka `card_features_rt`.

One output per authorization, computed when it arrives, over the same card's earlier authorizations:
  auth_count_10m, amount_sum_1h, distinct_merchants_1h, distinct_units_1h
Event-driven (KeyedProcessFunction), not windowed: the scoring model needs the features of the payment being
authorized now, not the result of a window that closes later. Duplicates are dropped first (keyed state on auth_id),
and locations are mapped to the 34 units, so "Binh Duong" then "Ho Chi Minh" within the hour is one place, not an
impossible journey. distinct_cities_raw_1h is the same count on raw names, kept only to measure that difference.

Usage (stream profile up; replay loaded into card_auth_replay):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/rt_features.py
  ... rt_features.py --topic card_authorizations            # Layer 2: whole topic (history backfill), then stops
  ... rt_features.py --topic card_authorizations --live     # Layer 2: unbounded, from the committed offsets
Layer 2's push jobs read `card_features_rt` into Feast (online and offline store), so training and serving see the
values of this one job.
"""
import argparse
import csv
import json
from datetime import datetime, timezone

from pyflink.common import Configuration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetResetStrategy, KafkaOffsetsInitializer,
                                                 KafkaRecordSerializationSchema, KafkaSink, KafkaSource)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

KAFKA = "kafka:19092"
MAPPING = "/opt/flink/ref/province_mapping.csv"
HOUR, TEN_MIN = 3_600_000, 600_000
KEEP = HOUR + 30 * 60_000   # + the longest POS delay, so a late event still sees its hour
AUTH = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING(), Types.LONG()])


def epoch_ms(s: str) -> int:
    """'YYYY-MM-DD HH:MM:SS.fff' (UTC) -> epoch milliseconds."""
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp() * 1000)


def parse(v: str):
    """JSON message -> (auth_id, card_id, merchant_id, amount_vnd, geo_city, auth_ts ms)."""
    d = json.loads(v)
    return d["auth_id"], d["card_id"], d["merchant_id"], d["amount_vnd"], d["geo_city"], epoch_ms(d["auth_ts"])


class FirstSeen(KeyedProcessFunction):
    """Pass only the first event of each auth_id (network retries arrive 1-10 s later)."""

    def open(self, ctx: RuntimeContext):
        """Per-auth_id state: a flag set by the first event (TTL 1 h, longer than any retry)."""
        desc = ValueStateDescriptor("seen", Types.BOOLEAN())
        desc.enable_time_to_live(StateTtlConfig.new_builder(Time.hours(1)).build())
        self.seen = ctx.get_state(desc)

    def process_element(self, value, ctx):
        """Emit the event only if this auth_id has not been seen yet."""
        if not self.seen.value():
            self.seen.update(True)
            yield value


class CardFeatures(KeyedProcessFunction):
    """Per card: the last KEEP ms of authorizations in state; features over [ts - window, ts] of the new one."""

    def open(self, ctx: RuntimeContext):
        # ponytail: static map loaded once; a broadcast stream of ref_province_mapping if it changes at runtime
        """Per-card state: the authorizations of the last KEEP ms (pickled list, TTL 2 h)."""
        with open(MAPPING, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        self.unit = {r["old_province_name"]: r["new_unit_name"] for r in rows}
        self.unit.update({r["new_unit_name"]: r["new_unit_name"] for r in rows})
        recent = ValueStateDescriptor("recent", Types.PICKLED_BYTE_ARRAY())
        recent.enable_time_to_live(StateTtlConfig.new_builder(Time.hours(2)).build())   # drop idle cards
        self.recent = ctx.get_state(recent)

    def process_element(self, e, ctx):
        """Add the authorization to the card's history and emit its features over that history."""
        auth_id, card_id, merchant, amount, city, ts = e
        recent = [r for r in (self.recent.value() or []) if r[0] >= ts - KEEP] + [(ts, merchant, amount, city)]
        self.recent.update(recent)
        hour = [r for r in recent if ts - HOUR <= r[0] <= ts]
        yield json.dumps({
            "auth_id": auth_id, "card_id": card_id, "event_timestamp": ts,
            "auth_count_10m": sum(1 for r in hour if r[0] >= ts - TEN_MIN),
            "amount_sum_1h": sum(r[2] for r in hour),
            "distinct_merchants_1h": len({r[1] for r in hour}),
            "distinct_units_1h": len({self.unit[r[3]] for r in hour}),
            "distinct_cities_raw_1h": len({r[3] for r in hour}),
        })


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="card_auth_replay")
    ap.add_argument("--live", action="store_true", help="unbounded, resume from the group's committed offsets")
    ap.add_argument("--parallelism", type=int, default=1, help="> 1 for the history backfill (state is keyed by card)")
    args = ap.parse_args()
    # Python keeps up to this many keys' state; the default (1,000) makes most of 130k cards a JVM round trip
    conf = Configuration()
    conf.set_integer("python.state.cache-size", 30_000)
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(args.parallelism)
    builder = (KafkaSource.builder().set_bootstrap_servers(KAFKA).set_topics(args.topic).set_group_id("rt_features")
               .set_value_only_deserializer(SimpleStringSchema()))
    if args.live:
        env.enable_checkpointing(10_000)   # offsets are committed on checkpoints
        builder = builder.set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
    else:
        builder = builder.set_starting_offsets(KafkaOffsetsInitializer.earliest()).set_bounded(KafkaOffsetsInitializer.latest())
    source = builder.build()
    sink = (KafkaSink.builder().set_bootstrap_servers(KAFKA)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic("card_features_rt")
                                   .set_value_serialization_schema(SimpleStringSchema()).build()).build())
    (env.from_source(source, WatermarkStrategy.no_watermarks(), f"kafka {args.topic}")   # arrival order, no windows
     .map(parse, output_type=AUTH)
     .key_by(lambda e: e[0], key_type=Types.STRING()).process(FirstSeen(), output_type=AUTH)
     .key_by(lambda e: e[1], key_type=Types.STRING()).process(CardFeatures(), output_type=Types.STRING())
     .sink_to(sink).name("card_features_rt"))
    env.execute("rt_features")
