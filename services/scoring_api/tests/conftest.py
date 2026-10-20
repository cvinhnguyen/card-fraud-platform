"""Fixtures: fake feature store / model / Kafka log, a scorer wired with them, and a test client of the real app."""
import asyncio
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from app.main import create_app
from app.policy import FallbackPolicy, ThresholdPolicy
from app.scorer import FraudScorer
from app.settings import Settings

FEATURES = {"txn_count_30d": 3.0, "amount": 250_000.0, "is_new_device": 0.0, "is_ecom": 1.0, "auth_count_10m": None}


class FakeFeatures:
    """Returns fixed features; can be made slow or failing."""

    def __init__(self, features=None, delay_s: float = 0.0, error: Exception | None = None):
        self.value, self.delay_s, self.error, self.calls = features or dict(FEATURES), delay_s, error, 0
        self.requests = []

    async def features(self, req):
        self.calls += 1
        self.requests.append(req)
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.error:
            raise self.error
        return dict(self.value)

    async def ready(self):
        if self.error:
            raise self.error
        return True


class FakeModel:
    """Deterministic model: score is a pure function of the features (or fixed); can be slow or failing."""

    def __init__(self, score: float | None = None, delay_s: float = 0.0, error: Exception | None = None):
        self.score, self.delay_s, self.error, self.calls = score, delay_s, error, 0

    async def predict(self, features):
        self.calls += 1
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.error:
            raise self.error
        if self.score is not None:
            return self.score, "7"
        return min(1.0, (features.get("amount") or 0) / 10_000_000), "7"

    async def ready(self):
        return self.error is None


class FakeLog:
    """Collects what the scorer would send to Kafka."""

    def __init__(self):
        self.records, self.started, self.stopped = [], False, False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send(self, record):
        self.records.append(record)


def request_body(**over) -> dict:
    """A valid authorization; keyword arguments override fields."""
    body = {"auth_id": "auth_000000000001", "card_id": "card_00001", "customer_id": "cus_00001",
            "merchant_id": "mer_00001", "amount_vnd": 250_000, "channel": "ecom", "currency": "VND",
            "auth_method": "3ds_otp", "device_id": "dev_0001", "auth_ts": datetime(2026, 10, 16, 9, 30).isoformat()}
    return {**body, **over}


def make_scorer(features=None, model=None, log=None, t_low=0.05, t_high=0.5, timeout_s=0.05) -> FraudScorer:
    """Scorer with fakes; thresholds and timeouts as given."""
    return FraudScorer(features or FakeFeatures(), model or FakeModel(), ThresholdPolicy(t_low, t_high),
                       FallbackPolicy(2_000_000), log, timeout_s, timeout_s)


@pytest.fixture
def fake_log():
    return FakeLog()


@pytest.fixture
def scorer(fake_log):
    return make_scorer(log=fake_log)


@pytest.fixture
def client(scorer):
    """The real FastAPI app (routes, validation, metrics) around the fake scorer; lifespan runs."""
    app = create_app(lambda s: scorer, Settings(), registry=CollectorRegistry())
    with TestClient(app) as c:
        yield c
