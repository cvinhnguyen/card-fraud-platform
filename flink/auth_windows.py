"""Authorizations per card per 5-minute event-time window, in three steps that each fix one stream problem.

  baseline  watermark 5 s, nothing else: late POS events (5-30 min, offline terminals) are dropped silently,
            network-retry duplicates are counted twice
  lateness  + allowed_lateness 30 min: a window that already fired is updated when a late event arrives;
            anything later still goes to a side output instead of disappearing
  dedup     + keyed state on auth_id (TTL 1 h) before the window: each authorization counts once

Event time is the Kafka message timestamp (the producer sets it to auth_ts), so watermarks are generated inside the
Kafka source. Reads the replay topic `card_auth_replay` (1 partition) from the earliest to the latest offset
(bounded), so a run over a fixed replay is reproducible. Two replay effects are neutralized, see docs/flink_jobs.md:
  - a replay packs days of event time into seconds; watermarks every 10 ms and a Python bundle of 1 element keep one
    watermark step to a few events, as with live traffic
  - with 6 partitions read at uneven speed the watermark (minimum over partitions) lagged days behind, so the replay
    topic has one partition and the job runs with parallelism 1
Writes JSON results to `auth_counts_<mode>` and late events to `auth_late_<mode>`;
flink/check_windows.py compares them with counts computed from the Parquet files.

Usage (stream profile up, replay loaded):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/auth_windows.py --mode dedup
"""
import argparse
import json

from pyflink.common import Configuration, Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
                                                 KafkaSource)
from pyflink.datastream.functions import AggregateFunction, KeyedProcessFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA = "kafka:19092"
TOPIC = "card_auth_replay"
WINDOW = Time.minutes(5)                  # PyFlink 2.2 windows and TTL still take Time; watermarks take Duration
OUT_OF_ORDER = Duration.of_seconds(5)     # normal network delay is at most 2 s
LATENESS = Time.minutes(30)               # offline POS terminals send up to 30 min late
EVENT = Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING()])   # auth_id, card_id, amount_vnd, auth_ts


class FirstSeen(KeyedProcessFunction):
    """Pass only the first event of each auth_id. Retries arrive 1-10 s later, so 1 h of state is plenty."""

    def open(self, ctx: RuntimeContext):
        """Keyed flag per auth_id with a 1 h TTL."""
        desc = ValueStateDescriptor("seen", Types.BOOLEAN())
        desc.enable_time_to_live(StateTtlConfig.new_builder(Time.hours(1)).build())
        self.seen = ctx.get_state(desc)

    def process_element(self, value, ctx):
        """Emit the event only the first time its auth_id is seen."""
        if not self.seen.value():
            self.seen.update(True)
            yield value


class Count(AggregateFunction):
    """Counts the events of a window."""
    def create_accumulator(self):
        """Start at 0."""
        return 0

    def add(self, value, acc):
        """One more event."""
        return acc + 1

    def get_result(self, acc):
        """The count."""
        return acc

    def merge(self, a, b):
        """Add two partial counts (session-window merges)."""
        return a + b


class ToJson(ProcessWindowFunction):
    """Window result as JSON: card_id, window_start, count."""
    def process(self, key, ctx, counts):
        """Attach the card and window start to the count."""
        yield json.dumps({"card_id": key, "window_start": ctx.window().start, "count": next(iter(counts))})


def sink(topic: str) -> KafkaSink:
    """Kafka sink writing strings to `topic`."""
    return (KafkaSink.builder().set_bootstrap_servers(KAFKA)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(topic)
                                   .set_value_serialization_schema(SimpleStringSchema()).build())
            .build())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["baseline", "lateness", "dedup"], required=True)
    mode = ap.parse_args().mode

    # Bundle of 1: the Python map otherwise takes 1,000 events at once and the source runs hours of event time ahead.
    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.size", "1")
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.get_config().set_auto_watermark_interval(10)
    env.set_parallelism(1)
    source = (KafkaSource.builder().set_bootstrap_servers(KAFKA).set_topics(TOPIC)
              .set_group_id(f"auth_windows_{mode}")
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_bounded(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema()).build())
    # No timestamp assigner: event time = Kafka message timestamp, watermarks generated in the source.
    # (A Python TimestampAssigner passed to from_source is ignored in PyFlink 2.2: windows fell on processing time.)
    wm = WatermarkStrategy.for_bounded_out_of_orderness(OUT_OF_ORDER)

    def parse(v: str):
        """JSON message -> (auth_id, card_id, amount_vnd, auth_ts)."""
        d = json.loads(v)
        return d["auth_id"], d["card_id"], d["amount_vnd"], d["auth_ts"]

    events = env.from_source(source, wm, f"kafka {TOPIC}").map(parse, output_type=EVENT)
    if mode == "dedup":
        events = events.key_by(lambda e: e[0], key_type=Types.STRING()).process(FirstSeen(), output_type=EVENT)

    # ponytail: untyped (pickled) tag; a TUPLE-typed tag fails to pickle in PyFlink 2.2 (nested Java type info)
    late = OutputTag("late")
    windows = events.key_by(lambda e: e[1], key_type=Types.STRING()).window(TumblingEventTimeWindows.of(WINDOW))
    if mode != "baseline":
        windows = windows.allowed_lateness(LATENESS.to_milliseconds())
    counts = windows.side_output_late_data(late).aggregate(Count(), ToJson(), accumulator_type=Types.LONG(),
                                                           output_type=Types.STRING())
    counts.sink_to(sink(f"auth_counts_{mode}")).name("counts")
    (counts.get_side_output(late).map(lambda e: json.dumps({"auth_id": e[0], "card_id": e[1], "auth_ts": e[3]}), output_type=Types.STRING())
     .sink_to(sink(f"auth_late_{mode}")).name("late events"))
    env.execute(f"auth_windows_{mode}")
