"""Fixtures: fake stores / embedder / scoring API (each can be made to fail), a service wired with them at a fixed
clock, and a test client of the real app (REST + MCP) with its own metrics registry."""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from app.main import create_app
from app.service import CardSupportService
from app.settings import Settings

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
CARD = {"card_type": "credit", "network": "visa", "card_issue_ts": datetime(2024, 3, 1),
        "txn_count_30d": 12, "distinct_merchants_30d": 7, "avg_amount_90d": 450_000.0, "std_amount_90d": 120_000.0,
        "ecom_ratio_90d": 0.5, "foreign_txn_ratio_90d": 0.1, "dispute_count_180d": 0, "stats_ts": datetime(2026, 10, 19),
        "auth_count_10m": 3, "amount_sum_1h": 9_000_000, "distinct_merchants_1h": 2, "distinct_units_1h": 2,
        "rt_ts": NOW - timedelta(minutes=5)}
CHUNKS = {   # 2-d embeddings keep the cosine arithmetic readable
    "card_terms#4.2": {"document": "card_terms", "section": "4.2", "text": "Not liable within 60 days.", "embedding": [1.0, 0.0]},
    "fee_schedule#2": {"document": "fee_schedule", "section": "2", "text": "Foreign fee 2.5%.", "embedding": [0.0, 1.0]},
    "faq#3": {"document": "faq", "section": "3", "text": "Lock your card.", "embedding": [0.7, 0.7]},
}


class FakeFeatures:
    """One fixed card; any other id is unknown (all None)."""

    def __init__(self, card_id="card_00001", values=None, error=None):
        self.card_id, self.values, self.error = card_id, dict(values or CARD), error

    async def card(self, card_id):
        if self.error:
            raise self.error
        return dict(self.values) if card_id == self.card_id else {k: None for k in self.values}

    async def ready(self):
        if self.error:
            raise self.error
        return True


class FakeKnowledge:
    """Chunks by id; candidates returned in a deliberately wrong order, with an unknown id mixed in."""

    def __init__(self, chunks=None, error=None):
        self.data, self.error, self.asked = dict(CHUNKS if chunks is None else chunks), error, []

    async def chunks(self, ids):
        if self.error:
            raise self.error
        return {i: self.data.get(i) for i in ids}

    async def candidates(self, vector, n):
        self.asked.append((vector, n))
        return sorted(self.data)[::-1][:n] + ["gone#9"]

    async def ready(self):
        if self.error:
            raise self.error
        return True


class FakeEmbedder:
    """Every question maps to a fixed vector."""

    def __init__(self, vector=(1.0, 0.1), error=None):
        self.vector, self.error, self.asked = list(vector), error, []

    async def embed_query(self, question):
        self.asked.append(question)
        if self.error:
            raise self.error
        return list(self.vector)


class FakeScoring:
    """Records the bodies it gets; answers a fixed response."""

    def __init__(self, response=None, error=None):
        self.response = response or {"auth_id": "x", "score": 0.94, "decision": "decline", "model_version": "1",
                                     "fallback": False}
        self.error, self.bodies = error, []

    async def score(self, body):
        self.bodies.append(body)
        if self.error:
            raise self.error
        return dict(self.response)


def make_service(**over) -> CardSupportService:
    """A service with fakes; keyword arguments replace one fake."""
    parts = {"features": FakeFeatures(), "knowledge": FakeKnowledge(), "embedder": FakeEmbedder(),
             "scoring": FakeScoring()} | over
    return CardSupportService(parts["features"], parts["knowledge"], parts["embedder"], parts["scoring"],
                              clock=lambda: NOW)   # candidate_factor: the default (3)


@pytest.fixture
def service():
    return make_service()


@pytest.fixture
def make_client():
    """Factory: a TestClient of the real app over a given service (lifespan runs: MCP session manager starts)."""
    clients = []

    def _make(svc=None):
        app = create_app(lambda s: svc or make_service(), Settings(), registry=CollectorRegistry())
        c = TestClient(app)
        c.__enter__()
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.__exit__(None, None, None)


@pytest.fixture
def client(make_client):
    return make_client()
