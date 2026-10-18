"""Authorizations per unit per day, with parallelism 4: the hot-key problem and its fix.

  hot        key_by(unit): every Ho Chi Minh authorization (60% of all) goes to the same subtask, which is busy all
             the time while the other three mostly wait; it backpressures the subtasks feeding it
  two_phase  key_by(unit + salt) for the hot unit (8 salts from auth_id), count per window, then key_by(unit) and add
             the partial counts: the hot unit's counting is spread over all subtasks, phase 2 gets <= 8 records per
             window. Only hot units are salted: in PyFlink every new (key, window) pair costs state + a timer through
             the Python runtime, so salting all 34 units multiplied that cost and the job got slower (--salt-all)

Same result in both modes. Event time is the Kafka timestamp; the watermark waits 31 minutes, longer than the latest
POS event (30 min), so no window fires before all its events are in and phase 2 never receives a partial count twice
(allowed_lateness would re-fire phase-1 windows and double count). Reads the 1-partition replay topic
`card_auth_replay` with one source subtask, bounded; flink/check_hot_key.py loads the replay, runs both modes and
reads per-subtask records and busy / backpressured time from the Flink REST API.

Usage (stream profile up, replay loaded):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/hot_key.py --mode hot
"""
import argparse
import csv
import json

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
                                                 KafkaSource)
from pyflink.datastream.functions import AggregateFunction, MapFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA = "kafka:19092"
TOPIC = "card_auth_replay"
MAPPING = "/opt/flink/ref/province_mapping.csv"
WINDOW = Time.days(1)
SALTS = 8
HOT = {"Ho Chi Minh"}   # 60% of authorizations (docs/data_characteristics.md); ponytail: fixed, from daily stats if it moves
PARALLELISM = 4
AUTH = Types.TUPLE([Types.STRING(), Types.STRING()])                  # auth_id, unit
PARTIAL = Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG()])   # unit, window_start, count


class ToUnit(MapFunction):
    """JSON message -> (auth_id, unit): old and new province names map to the 34 units."""

    def open(self, ctx: RuntimeContext):
        """Load the 63 -> 34 province mapping once per Python worker."""
        with open(MAPPING, encoding="utf-8") as f:
            self.unit = {r["old_province_name"]: r["new_unit_name"] for r in csv.DictReader(f)}
        self.unit.update({u: u for u in set(self.unit.values())})

    def map(self, v: str):
        """Parse the message and map its location to one of the 34 units."""
        d = json.loads(v)
        return d["auth_id"], self.unit[d["geo_city"]]


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
        """Add two partial counts."""
        return a + b


class Partial(ProcessWindowFunction):
    """Phase-1 result: (unit, window_start, count); the key may carry a salt suffix, the output does not."""

    def process(self, key, ctx, counts):
        """Emit (unit, window_start, count), dropping the salt suffix of the key."""
        yield key.split("#")[0], ctx.window().start, next(iter(counts))


def sink(topic: str) -> KafkaSink:
    """Kafka sink writing strings to `topic`."""
    return (KafkaSink.builder().set_bootstrap_servers(KAFKA)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(topic)
                                   .set_value_serialization_schema(SimpleStringSchema()).build())
            .build())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["hot", "two_phase"], required=True)
    ap.add_argument("--salts", type=int, default=SALTS, help="two_phase: partial keys per unit")
    ap.add_argument("--salt-all", action="store_true", help="salt every unit, not only the hot ones")
    args = ap.parse_args()
    mode, salts, hot = args.mode, args.salts, None if args.salt_all else HOT

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    source = (KafkaSource.builder().set_bootstrap_servers(KAFKA).set_topics(TOPIC)
              .set_group_id(f"hot_key_{mode}")
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_bounded(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema()).build())
    wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(31))
    # One partition -> one source subtask (idle readers would hold the watermark back); rebalance spreads the parsing.
    auths = (env.from_source(source, wm, f"kafka {TOPIC}").set_parallelism(1).rebalance()
             .map(ToUnit(), output_type=AUTH).name("to unit"))

    late = OutputTag("late")
    if mode == "hot":
        windows = auths.key_by(lambda a: a[1], key_type=Types.STRING())
    else:
        # The salt comes from auth_id, so a retry lands in the same partial count as its original.
        windows = auths.key_by(lambda a: f"{a[1]}#{int(a[0][5:], 16) % salts}" if hot is None or a[1] in hot else a[1],
                               key_type=Types.STRING())
    partials = (windows.window(TumblingEventTimeWindows.of(WINDOW)).side_output_late_data(late)
                .aggregate(Count(), Partial(), accumulator_type=Types.LONG(), output_type=PARTIAL).name("count per key"))
    counts = partials
    if mode == "two_phase":
        counts = (partials.key_by(lambda p: p[0], key_type=Types.STRING()).window(TumblingEventTimeWindows.of(WINDOW))
                  .reduce(lambda a, b: (a[0], a[1], a[2] + b[2]), output_type=PARTIAL).name("add partial counts"))

    (counts.map(lambda p: json.dumps({"unit": p[0], "window_start": p[1], "count": p[2]}), output_type=Types.STRING())
     .sink_to(sink(f"unit_counts_{mode}")).name("unit counts"))
    (partials.get_side_output(late).map(lambda a: a[0], output_type=Types.STRING())   # expected empty: 31 min > 30
     .sink_to(sink(f"unit_late_{mode}")).name("late events"))
    env.execute(f"hot_key_{mode}")
