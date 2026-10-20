"""Endpoints, the minute-bucket store, probes, metrics, reference loading."""
import json

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st
from prometheus_client import CollectorRegistry

from app.main import create_app, load_reference
from app.settings import Settings
from app.store import HistogramStore
from tests.conftest import REFERENCE, FakeRedis, event


def test_batch_is_counted(client, redis):
    r = client.post("/v1/events", json={"events": [event(), event(auth_id="a2")]})
    assert r.json() == {"counted": 2}
    keys = sorted(redis.data)
    assert any(k.endswith(":amount") for k in keys) and any(k.endswith(":score") for k in keys)
    assert all(ttl == Settings().bucket_ttl_s for ttl in redis.ttl.values())
    assert set(redis.ttl) == set(redis.data)   # every bucket written gets the TTL


def test_single_event_body_is_counted(client):
    assert client.post("/v1/events", json=event()).json() == {"counted": 1}


def test_fallback_event_does_not_stop_the_batch(client):
    batch = [event(fallback=True, score=None), event(auth_id="a2")]
    assert client.post("/v1/events", json={"events": batch}).json() == {"counted": 1}


def test_fallback_events_are_not_counted(client):
    assert client.post("/v1/events", json={"events": [event(fallback=True, score=None)]}).json() == {"counted": 0}


@pytest.mark.parametrize("n, status", [(0, 422), (1, 200), (1000, 200), (1001, 422)])
def test_batch_size_boundaries(client, n, status):
    body = {"events": [event(auth_id=f"a{i}") for i in range(n)]}
    assert client.post("/v1/events", json=body).status_code == status


@pytest.mark.parametrize("score, status", [(-0.01, 422), (0.0, 200), (1.0, 200), (1.01, 422)])
def test_score_boundaries(client, score, status):
    assert client.post("/v1/events", json=event(score=score)).status_code == status


def test_drift_report_stable_when_live_matches_reference(client):
    # amounts in the reference shares 30/50/20, scores 90/8/2
    evs = ([event(50_000, 0.005)] * 27 + [event(50_000, 0.05)] * 3 + [event(250_000, 0.005)] * 45
           + [event(250_000, 0.05)] * 3 + [event(250_000, 0.5)] * 2 + [event(900_000, 0.005)] * 18
           + [event(900_000, 0.05)] * 2)
    client.post("/v1/events", json={"events": evs})
    rep = client.get("/v1/drift").json()
    assert rep["drift"] is False and rep["model_version"] == "7" and rep["window_minutes"] == 5
    assert {i["name"]: i["level"] for i in rep["inputs"]} == {"amount": "stable", "score": "stable"}
    assert all(i["live_count"] == 100 for i in rep["inputs"])


def test_drift_report_major_when_amounts_shift(client):
    client.post("/v1/events", json={"events": [event(5_000_000, 0.005) for _ in range(100)]})
    rep = client.get("/v1/drift").json()
    assert rep["drift"] is True and rep["inputs"][0]["name"] == "amount" and rep["inputs"][0]["level"] == "major"


@pytest.mark.parametrize("n, level, drift", [(99, "too_few_events", False), (100, "major", True)])
def test_small_window_never_drifts(client, n, level, drift):
    client.post("/v1/events", json={"events": [event(5_000_000, 0.005) for _ in range(n)]})
    rep = client.get("/v1/drift").json()
    amount = next(i for i in rep["inputs"] if i["name"] == "amount")
    assert (amount["level"], rep["drift"], amount["live_count"]) == (level, drift, n) and amount["psi"] > 0.25


def test_drift_report_without_data(client):
    rep = client.get("/v1/drift").json()
    assert rep["drift"] is False and all(i["level"] == "no_data" for i in rep["inputs"])


def test_psi_gauge_follows_the_report(client):
    from prometheus_client import REGISTRY   # the app's own gauges live in the default registry
    client.post("/v1/events", json={"events": [event(5_000_000)]})
    psi = next(i["psi"] for i in client.get("/v1/drift").json()["inputs"] if i["name"] == "amount")
    assert REGISTRY.get_sample_value("drift_psi", {"input": "amount"}) == pytest.approx(psi, abs=1e-4)
    assert "http_requests_total" in client.get("/metrics").text


@settings(max_examples=30, deadline=None)
@given(amounts=st.lists(st.floats(1, 1e9), min_size=1, max_size=50))
def test_drift_report_is_idempotent(amounts):
    r = FakeRedis()
    app = create_app(Settings(), lambda: r, REFERENCE, registry=CollectorRegistry())
    with TestClient(app) as c:
        c.post("/v1/events", json={"events": [event(a) for a in amounts]})
        first = c.get("/v1/drift").json()
        assert all(c.get("/v1/drift").json() == first for _ in range(3))


async def test_buckets_are_named_by_minute():
    r = FakeRedis()
    ev = type("E", (), {"fallback": False, "score": 0.1, "features": {"amount": 50.0}})()
    await HistogramStore(r, {"amount": [100.0]}, 3600, clock=lambda: 3659.9).add([ev])
    assert list(r.data) == ["drift:60:amount"]   # second 3659.9 is minute 60


async def test_window_only_sums_recent_minutes():
    now = [600.0]   # minute 10
    store = HistogramStore(FakeRedis(), {"amount": [100.0]}, 3600, clock=lambda: now[0])
    ev = type("E", (), {"fallback": False, "score": 0.1, "features": {"amount": 50.0}})()
    await store.add([ev])
    now[0] = 600.0 + 60 * 4             # minute 14
    await store.add([ev, ev])
    assert (await store.window(5))["amount"] == {"0": 3}    # minutes 10..14
    assert (await store.window(4))["amount"] == {"0": 2}    # minutes 11..14


def test_healthz_and_readyz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_readyz_503_when_redis_down():
    app = create_app(Settings(), lambda: FakeRedis(down=True), REFERENCE, registry=CollectorRegistry())
    with TestClient(app) as c:
        assert c.get("/readyz").status_code == 503


def test_redis_closed_on_shutdown(redis):
    with TestClient(create_app(Settings(), lambda: redis, REFERENCE, registry=CollectorRegistry())):
        pass
    assert redis.closed


def test_reference_loaded_from_file(tmp_path, monkeypatch):
    p = tmp_path / "ref.json"
    p.write_text(json.dumps(REFERENCE))
    assert load_reference(str(p)) == REFERENCE
    import redis.asyncio
    monkeypatch.setattr(redis.asyncio.Redis, "from_url", staticmethod(lambda url, password=None: FakeRedis()))
    with TestClient(create_app(Settings(reference_path=str(p)), registry=CollectorRegistry())) as c:
        assert c.get("/v1/drift").json()["model_version"] == "7"
