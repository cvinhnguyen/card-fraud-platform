"""Fixtures: an in-memory async Redis (just the commands the store uses), a reference profile, a test client."""
import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from app.main import create_app
from app.settings import Settings

REFERENCE = {"model_version": "7", "inputs": {
    "amount": {"edges": [100_000.0, 500_000.0], "shares": {"0": 0.3, "1": 0.5, "2": 0.2, "missing": 0.0}},
    "score": {"edges": [0.01, 0.1], "shares": {"0": 0.9, "1": 0.08, "2": 0.02, "missing": 0.0}},
}}


class FakePipeline:
    def __init__(self, r):
        self.r, self.ops = r, []

    def hincrby(self, key, field, n):
        self.ops.append(("hincrby", key, field, n))

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))

    def hgetall(self, key):
        self.ops.append(("hgetall", key))

    async def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "hincrby":
                h = self.r.data.setdefault(op[1], {})
                h[op[2].encode()] = h.get(op[2].encode(), 0) + op[3]
                out.append(h[op[2].encode()])
            elif op[0] == "expire":
                self.r.ttl[op[1]] = op[2]
                out.append(True)
            else:
                out.append(dict(self.r.data.get(op[1], {})))
        return out


class FakeRedis:
    def __init__(self, down: bool = False):
        self.data, self.ttl, self.down, self.closed = {}, {}, down, False

    def pipeline(self, transaction=False):
        return FakePipeline(self)

    async def ping(self):
        if self.down:
            raise ConnectionError("redis down")
        return True

    async def aclose(self):
        self.closed = True


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def client(redis):
    app = create_app(Settings(window_minutes=5), lambda: redis, REFERENCE, registry=CollectorRegistry())
    with TestClient(app) as c:
        yield c


def event(amount=250_000.0, score=0.005, fallback=False, auth_id="a1"):
    return {"auth_id": auth_id, "score": score, "fallback": fallback, "features": {"amount": amount}}
