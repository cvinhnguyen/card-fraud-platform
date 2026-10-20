"""Kafka `fraud_predictions` -> Drift API POST /v1/events, in batches of up to 1,000 events.

The scoring API logs every decision to Kafka; this forwarder feeds the drift API from that log, so scoring never
waits on drift detection. At-least-once: offsets are committed after the API accepted the batch.

Usage (ml image, compose network): python ml/jobs/drift_forwarder.py --api http://drift-api:8000
"""
import argparse
import json
import time
import urllib.request

from confluent_kafka import Consumer


def post(api: str, events: list[dict]) -> None:
    """One batch to the drift API; raises on a non-2xx answer (the batch is retried after a restart)."""
    body = json.dumps({"events": events}).encode()
    req = urllib.request.Request(f"{api}/v1/events", body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        r.read()


def main(api: str, max_wait_s: float) -> None:
    """Consume forever; post whenever 1,000 events or max_wait_s seconds have accumulated."""
    c = Consumer({"bootstrap.servers": "kafka:19092", "group.id": "drift_forwarder", "auto.offset.reset": "latest",
                  "enable.auto.commit": False})
    c.subscribe(["fraud_predictions"])
    batch, last, sent = [], time.monotonic(), 0
    while True:
        for m in c.consume(num_messages=1000 - len(batch), timeout=1.0):
            if not m.error():
                e = json.loads(m.value())
                batch.append({"auth_id": e["auth_id"], "score": e.get("score"), "fallback": e.get("fallback", False),
                              "features": e.get("features")})
        if batch and (len(batch) >= 1000 or time.monotonic() - last >= max_wait_s):
            post(api, batch)
            c.commit(asynchronous=False)
            sent += len(batch)
            print(f"forwarded {sent:,} events", flush=True)
            batch, last = [], time.monotonic()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--api", default="http://drift-api:8000")
    ap.add_argument("--max-wait", type=float, default=5.0)
    a = ap.parse_args()
    main(a.api, a.max_wait)
