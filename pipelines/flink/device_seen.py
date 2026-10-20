"""First time a card is used from a device (feature view `card_device` of Layer 2) -> Kafka `card_device_seen`.

Fraudsters pay online from their own phone, so a device the card never used is the strongest realtime signal.
Keyed state per (card_id, device_id): one boolean, no TTL (a known device stays known). The first authorization of
a pair emits {card_id, device_id, first_seen_ts}; later ones and network retries emit nothing. Arrival order, like
rt_features.py. The scoring API asks Feast for the pair: no row means "new device".

Usage (stream profile up):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/device_seen.py --topic card_authorizations          # backfill
  ... device_seen.py --topic card_authorizations --live                                                           # live
"""
import argparse
import json
from datetime import datetime, timezone

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetResetStrategy, KafkaOffsetsInitializer,
                                                 KafkaRecordSerializationSchema, KafkaSink, KafkaSource)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

KAFKA = "kafka:19092"
PAIR = Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])   # card_id, device_id, auth_ts ms


def epoch_ms(s: str) -> int:
    """'YYYY-MM-DD HH:MM:SS.fff' (UTC) -> epoch milliseconds (same as rt_features.py; the Python workers on the
    TaskManager only get this file, so it is not imported from there)."""
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp() * 1000)


def parse(v: str):
    """JSON authorization -> (card_id, device_id, auth_ts ms); device_id is null for non-ecom payments."""
    d = json.loads(v)
    return d["card_id"], d.get("device_id"), epoch_ms(d["auth_ts"])


class FirstSeen(KeyedProcessFunction):
    """Emit a (card, device) pair the first time it arrives."""

    def open(self, ctx: RuntimeContext):
        """One flag per pair, kept forever."""
        self.seen = ctx.get_state(ValueStateDescriptor("seen", Types.BOOLEAN()))

    def process_element(self, e, ctx):
        """First sight of the pair -> one JSON message; anything later -> nothing."""
        if not self.seen.value():
            self.seen.update(True)
            yield json.dumps({"card_id": e[0], "device_id": e[1], "first_seen_ts": e[2], "event_timestamp": e[2]})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="card_authorizations")
    ap.add_argument("--live", action="store_true", help="unbounded, resume from the group's committed offsets")
    ap.add_argument("--parallelism", type=int, default=1, help="> 1 for the history backfill (state is keyed by pair)")
    args = ap.parse_args()
    conf = Configuration()
    conf.set_integer("python.state.cache-size", 30_000)   # as rt_features.py
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(args.parallelism)
    builder = (KafkaSource.builder().set_bootstrap_servers(KAFKA).set_topics(args.topic).set_group_id("device_seen")
               .set_value_only_deserializer(SimpleStringSchema()))
    if args.live:
        env.enable_checkpointing(10_000)
        builder = builder.set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
    else:
        builder = builder.set_starting_offsets(KafkaOffsetsInitializer.earliest()).set_bounded(KafkaOffsetsInitializer.latest())
    sink = (KafkaSink.builder().set_bootstrap_servers(KAFKA)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic("card_device_seen")
                                   .set_value_serialization_schema(SimpleStringSchema()).build()).build())
    (env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), f"kafka {args.topic}")
     .map(parse, output_type=PAIR)
     .filter(lambda e: e[1] is not None)   # only online payments carry a device
     .key_by(lambda e: f"{e[0]}|{e[1]}", key_type=Types.STRING())
     .process(FirstSeen(), output_type=Types.STRING())
     .sink_to(sink).name("card_device_seen"))
    env.execute("device_seen")
