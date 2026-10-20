"""Replay the live period (generator/generate_drift.py) as the authorization switch would: every authorization goes
to Kafka `card_authorizations` (Flink -> realtime features -> Feast), and the first copy of each is scored by the
Fraud Scoring API, in arrival order, at --rate requests per second.

Duplicates (network retries) reach Kafka only: the switch scores a payment once. A day of event time passes in
~ 8,000 / rate seconds (80 s at 100/s, ~1,000x real time). Known limit of the replay: the path Kafka -> Flink ->
push job A -> Redis takes a few seconds, i.e. hours of replayed event time, so a card's authorization from the
last minutes or hours is often not online yet when its next payment is scored. The realtime features
(`*_now`) are then lower than in production and than in training; their PSI in the drift API is partly this
artifact. The request features (amount, channel, ...) and the daily stats are not affected.

One API worker scores ~45 requests/s (Feast computes the on-demand features in pandas, in the event loop): above
that, requests queue past their time budget and fall back. --score-days scores only some days; the others still go
to Kafka (every day needs its realtime features, device history and raw landing) at --stream-rate.
With --max-lag a day starts only once the rt_features and device_seen jobs are within that many events of the topic
end, so a scored day reads current online features. Without it, a stalled Flink job (it once looped through
restarts on failed checkpoints) left a scored day 24k events behind: `is_new_device` flagged every device of the
last 3 days as new.

Usage (host; compose profile ml up, the API on 127.0.0.1:8000):
  python ml/tools/replay_scoring.py --rate 30 [--days 3] [--from-day 2026-11-09] [--score-days 2026-11-09,2026-11-10]
                               [--max-lag 300]
"""
import argparse
import asyncio
import time
from pathlib import Path

import httpx
import pandas as pd
import pyarrow.parquet as pq
from confluent_kafka import Consumer, Producer, TopicPartition

ROOT = Path(__file__).resolve().parents[2]
LIVE = ROOT / "data" / "drift"
STREAM_COLS = ["auth_id", "card_id", "merchant_id", "amount_vnd", "channel", "terminal_id", "device_id", "geo_city",
               "auth_ts", "received_ts"]


def days(from_day: str | None, n: int | None) -> list[str]:
    """auth_date folders of the live period to replay."""
    all_days = sorted(p.name.split("=")[1] for p in (LIVE / "requests").iterdir())
    start = all_days.index(from_day) if from_day else 0
    return all_days[start:start + n] if n else all_days[start:]


def flink_lag(groups=("rt_features", "device_seen"), topic: str = "card_authorizations") -> int:
    """Events of `topic` the Flink jobs have not committed yet (read only: the groups are not joined)."""
    lag = 0
    for g in groups:
        c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": g, "enable.auto.commit": False})
        parts = [TopicPartition(topic, p) for p in c.list_topics(topic, timeout=10).topics[topic].partitions]
        for tp in c.committed(parts, timeout=10):
            lag += c.get_watermark_offsets(tp, timeout=10)[1] - max(tp.offset, 0)
        c.close()
    return lag


def ts(s: pd.Series) -> pd.Series:
    """Timestamps as the producer writes them ('YYYY-MM-DD HH:MM:SS.fff')."""
    return s.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]


async def replay(api: str, rate: float, day_list: list[str], concurrency: int, score_days: set[str] | None = None,
                 stream_rate: float = 150, max_lag: int | None = None) -> None:
    """Send each day's stream to Kafka and its first copies to the API, paced to `rate` scores per second; days not in
    `score_days` (when given) only go to Kafka, at `stream_rate` events per second."""
    producer = Producer({"bootstrap.servers": "127.0.0.1:9092", "linger.ms": 20, "enable.idempotence": True})
    stats = {"sent": 0, "scored": 0, "errors": 0, "fallback": 0, "decisions": {}}
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(base_url=api, timeout=5.0) as client:
        async def score(body: dict) -> None:
            async with sem:
                try:
                    r = await client.post("/v1/score", json=body)
                    r.raise_for_status()
                    out = r.json()
                    stats["scored"] += 1
                    stats["fallback"] += out["fallback"]
                    stats["decisions"][out["decision"]] = stats["decisions"].get(out["decision"], 0) + 1
                except httpx.HTTPError:
                    stats["errors"] += 1

        t0, tasks = time.perf_counter(), []
        t_next = t0
        for day in day_list:
            scored = score_days is None or day in score_days
            while max_lag is not None and (lag := flink_lag()) > max_lag:
                print(f"{day}: waiting for Flink, {lag:,} events behind", flush=True)
                await asyncio.sleep(10)
            t_next = max(t_next, time.perf_counter())   # no catch-up burst after loading the day / flushing Kafka
            stream = pq.read_table(LIVE / "stream" / "card_authorizations" / f"auth_date={day}").to_pandas()
            req = pq.read_table(LIVE / "requests" / f"auth_date={day}").to_pandas().set_index("auth_id")
            stream = stream.sort_values("received_ts", kind="stable", ignore_index=True)
            first = ~stream.auth_id.duplicated()
            msgs = stream[STREAM_COLS].assign(auth_ts=ts(stream.auth_ts), received_ts=ts(stream.received_ts))
            values = msgs.to_json(orient="records", lines=True).splitlines()
            stamps = ((stream.auth_ts - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)).tolist()
            for i, (key, value, stamp) in enumerate(zip(stream.card_id, values, stamps)):
                producer.produce("card_authorizations", key=key, value=value, timestamp=stamp)
                producer.poll(0)
                stats["sent"] += 1
                if not scored:
                    t_next += 1 / stream_rate
                    if (ahead := t_next - time.perf_counter()) > 0:
                        await asyncio.sleep(ahead)
                    continue
                if not first.iat[i]:
                    continue
                r = req.loc[stream.auth_id.iat[i]]
                body = {"auth_id": stream.auth_id.iat[i], "card_id": key, "customer_id": r.customer_id,
                        "merchant_id": r.merchant_id, "amount_vnd": int(r.amount_vnd), "channel": r.channel,
                        "currency": r.currency, "auth_method": r.auth_method,
                        "device_id": None if pd.isna(r.device_id) else r.device_id,
                        "auth_ts": r.auth_ts.isoformat()}
                tasks.append(asyncio.create_task(score(body)))
                n = len(tasks)
                t_next += 1 / rate
                if (ahead := t_next - time.perf_counter()) > 0:
                    await asyncio.sleep(ahead)
                if n % 5000 == 0:
                    print(f"{day}: {stats}", flush=True)
            producer.flush(30)
            print(f"day {day} done ({'scored' if scored else 'Kafka only'}, {time.perf_counter() - t0:.0f}s): {stats}",
                  flush=True)
        await asyncio.gather(*tasks)
    print(f"done in {time.perf_counter() - t0:.0f}s: {stats}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--api", default="http://127.0.0.1:8000")
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--from-day")
    ap.add_argument("--days", type=int)
    ap.add_argument("--score-days", help="comma-separated days to score (default: all); the others go to Kafka only")
    ap.add_argument("--stream-rate", type=float, default=150, help="events/s of the days that are not scored")
    ap.add_argument("--max-lag", type=int, help="start a day only when Flink is at most this many events behind")
    a = ap.parse_args()
    asyncio.run(replay(a.api, a.rate, days(a.from_day, a.days), a.concurrency,
                       set(a.score_days.split(",")) if a.score_days else None, a.stream_rate, a.max_lag))
