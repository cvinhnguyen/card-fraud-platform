"""Replay a generated stream (card_authorizations or app_events) into Kafka in arrival order (received_ts).

Each event is one JSON message keyed by card_id / customer_id, so one card's or customer's events stay in
one partition. The Kafka message timestamp is the event time (auth_ts / event_ts), as a card network or app
would set it, so Flink can take event time and per-partition watermarks straight from the Kafka source.
Late, duplicate and out-of-order events are already in the data; the producer is idempotent, so it adds no
duplicates of its own. Timestamps are 'YYYY-MM-DD HH:MM:SS.fff' (Flink SQL JSON format).

Usage: python generator/producer.py [--topic app_events] [--speed 200] [--replay-from "2025-06-30 23:00"] [--limit 1000]
       python generator/producer.py --stream-dir data/drift/stream --speed 0     # Layer 2: the live period
       (after generate.py and infra/init_infra.py; Ctrl-C stops and prints where to resume)
"""
import argparse
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import yaml
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parent.parent
KEYS = {"card_authorizations": "card_id", "app_events": "customer_id"}   # topic -> message key
EVENT_TS = {"card_authorizations": "auth_ts", "app_events": "event_ts"}   # topic -> Kafka message timestamp


def load(src: Path, replay_from: pd.Timestamp | None, limit: int | None) -> pd.DataFrame:
    """Events with received_ts >= replay_from, in arrival order.
    ponytail: loads the whole stream into memory; read day by day if it outgrows RAM."""
    df = pq.read_table(src, partitioning=None).to_pandas()   # no partition column
    if replay_from is not None:
        df = df[df.received_ts >= replay_from]
    return df.sort_values("received_ts", kind="stable", ignore_index=True).head(limit)


def main(args: argparse.Namespace) -> None:
    """Send every event, paced to --speed events/s (0 = as fast as possible)."""
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    df = load((args.stream_dir or ROOT / cfg["output_dir"] / "stream") / args.topic, args.replay_from, args.limit)
    ts_cols = df.select_dtypes("datetime").columns
    out = df.assign(**{c: df[c].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3] for c in ts_cols})
    values = out.to_json(orient="records", lines=True).splitlines()   # nulls stay null
    stamps = ((df[EVENT_TS[args.topic]] - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)).tolist()   # naive = UTC, as Flink

    errors, target = [], args.target or args.topic
    producer = Producer({"bootstrap.servers": "127.0.0.1:9092", "enable.idempotence": True, "linger.ms": 20,
                         "on_delivery": lambda err, _: errors.append(err) if err else None})
    sent, t0 = 0, time.perf_counter()
    try:
        for key, value, ts in zip(df[KEYS[args.topic]], values, stamps):
            if args.speed and (ahead := t0 + sent / args.speed - time.perf_counter()) > 0:
                time.sleep(ahead)
            while True:
                try:
                    producer.produce(target, key=key, value=value, timestamp=ts)
                    break
                except BufferError:   # local queue full: let it drain
                    producer.poll(0.5)
            producer.poll(0)
            sent += 1
    except KeyboardInterrupt:
        pass
    finally:
        left = producer.flush(30)
    print(f"sent {sent - left:,} of {len(df):,} events to {target} in {time.perf_counter() - t0:.1f}s")
    if sent < len(df):
        print(f'resume with --replay-from "{df.received_ts.iloc[sent]}"')
    if errors or left:
        raise SystemExit(f"{len(errors)} delivery errors, {left} undelivered, first: {errors[:1]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    ap.add_argument("--topic", choices=KEYS, default="card_authorizations")
    ap.add_argument("--speed", type=float, default=200, help="events per second, 0 = no limit")
    ap.add_argument("--replay-from", type=pd.Timestamp, help="start at this received_ts")
    ap.add_argument("--limit", type=int, help="stop after this many events")
    ap.add_argument("--target", help="send to this topic instead of --topic (e.g. a 1-partition replay topic)")
    ap.add_argument("--stream-dir", type=Path, help="read from here instead of <output_dir>/stream "
                    "(Layer 2 live period: data/drift/stream)")
    main(ap.parse_args())
