"""Load test of POST /v1/score (SLA report: locust --headless ... --html docs/images/locust_report.html).

Card ids come from CARDS_FILE (one per line, real cards so Redis lookups hit) or are random.
Usage: locust -f services/scoring_api/locustfile.py --host http://127.0.0.1:8000 --headless -u 50 -r 10 -t 2m
"""
import os
import random
from datetime import datetime, timezone

from locust import HttpUser, between, task

CARDS = open(os.environ["CARDS_FILE"]).read().split() if os.environ.get("CARDS_FILE") else []


class Switch(HttpUser):
    """The authorization switch: one score request per payment."""

    wait_time = between(0.01, 0.1)

    @task
    def score(self):
        ecom = random.random() < 0.6
        self.client.post("/v1/score", name="/v1/score", json={
            "auth_id": f"auth_{random.getrandbits(48):012x}",
            "card_id": random.choice(CARDS) if CARDS else f"card_{random.randrange(130_000):06d}",
            "customer_id": "cus_load", "merchant_id": "mer_load",
            "amount_vnd": random.choice([50_000, 250_000, 1_200_000, 6_000_000]),
            "channel": "ecom" if ecom else random.choice(["pos", "atm"]),
            "currency": "USD" if ecom and random.random() < 0.05 else "VND",
            "auth_method": "3ds_otp" if ecom else "chip_pin",
            "device_id": f"dev_{random.randrange(200_000):06x}" if ecom else None,
            "auth_ts": datetime.now(timezone.utc).isoformat()})
