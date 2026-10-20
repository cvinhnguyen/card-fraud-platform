"""Account takeover (new_device) pattern on app events: password_reset -> add_payee -> transfer within 10 minutes,
in the same session. About 15% of app events reach Kafka after the next event of their session (parallel uploads).

  arrival  match events in the order they arrive: a reordered pair breaks the sequence and the session is missed
  event    buffer each session's events and release them in event_ts order when the watermark (30 s behind the
           newest event) passes them, via event-time timers; then match

Writes one message per matched session to `ato_alerts_<order>`. pipelines/flink/check_ato.py compares with the generator truth.

Usage (stream profile up; replay loaded into app_events_replay):
  docker exec flink-jobmanager flink run -py /opt/flink/jobs/ato_sequence.py --order event
"""
import argparse
import json
from datetime import datetime, timezone

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
                                                 KafkaSource)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

KAFKA = "kafka:19092"
STEPS = ["password_reset", "add_payee", "transfer"]
WITHIN_MS = 10 * 60_000
EVENT = Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])   # session_id, event_type, event_ts ms


def parse(v: str):
    """JSON app event -> (session_id, event_type, event_ts in epoch ms, UTC)."""
    d = json.loads(v)
    ts = datetime.strptime(d["event_ts"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()
    return d["session_id"], d["event_type"], int(ts * 1000)


class Sequence(KeyedProcessFunction):
    """Per session: progress through STEPS; in event mode, events wait in a buffer until the watermark passes them."""

    def __init__(self, by_event_time: bool):
        """by_event_time: buffer and reorder each session's events before matching."""
        self.by_event_time = by_event_time

    def open(self, ctx: RuntimeContext):
        """Per-session state: progress through STEPS and, in event mode, the buffered events (TTL 1 h)."""
        ttl = StateTtlConfig.new_builder(Time.hours(1)).build()   # sessions last minutes; drop finished ones
        state, buffer = (ValueStateDescriptor(n, Types.PICKLED_BYTE_ARRAY()) for n in ("state", "buffer"))
        for d in (state, buffer):
            d.enable_time_to_live(ttl)
        self.state, self.buffer = ctx.get_state(state), ctx.get_state(buffer)   # (step, reset_ts, done), [events]

    def advance(self, session, event_type, ts):
        """Feed one event to the state machine; yield an alert when the third step lands within WITHIN_MS."""
        step, reset_ts, done = self.state.value() or (0, None, False)
        if done or event_type != STEPS[step]:
            return
        if step == 0:
            reset_ts = ts
        elif ts - reset_ts > WITHIN_MS:
            return
        step += 1
        self.state.update((step, reset_ts, step == len(STEPS)))
        if step == len(STEPS):
            yield json.dumps({"session_id": session, "reset_ts": reset_ts, "transfer_ts": ts})

    def process_element(self, e, ctx):
        """Arrival mode: match at once. Event mode: buffer and set a timer at the event time."""
        if not self.by_event_time:
            yield from self.advance(*e)
            return
        self.buffer.update((self.buffer.value() or []) + [e])
        ctx.timer_service().register_event_time_timer(e[2])

    def on_timer(self, ts, ctx):
        """Release the buffered events up to the watermark in event_ts order and match them."""
        ready = sorted((e for e in self.buffer.value() if e[2] <= ts), key=lambda e: e[2])
        self.buffer.update([e for e in self.buffer.value() if e[2] > ts])
        for e in ready:
            yield from self.advance(*e)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--order", choices=["arrival", "event"], required=True)
    order = ap.parse_args().order

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    source = (KafkaSource.builder().set_bootstrap_servers(KAFKA).set_topics("app_events_replay").set_group_id(f"ato_{order}")
              .set_starting_offsets(KafkaOffsetsInitializer.earliest()).set_bounded(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema()).build())
    # Event time = Kafka timestamp (event_ts); events are at most 20 s out of order, the watermark waits 30 s.
    wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
    sink = (KafkaSink.builder().set_bootstrap_servers(KAFKA)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(f"ato_alerts_{order}")
                                   .set_value_serialization_schema(SimpleStringSchema()).build()).build())
    (env.from_source(source, wm, "kafka app_events_replay")
     .map(parse, output_type=EVENT)
     .key_by(lambda e: e[0], key_type=Types.STRING())
     .process(Sequence(order == "event"), output_type=Types.STRING())
     .sink_to(sink).name(f"ato_alerts_{order}"))
    env.execute(f"ato_sequence_{order}")
