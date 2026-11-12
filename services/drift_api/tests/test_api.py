"""Endpoints, the minute-bucket store, probes, metrics, reference loading.

  POST /v1/inference-events   request event (dict or positional instances) | response event | non-inference call
                              (a GET and its answer: ignored) | unknown ce-type | missing ce-type |
                              malformed body (bad JSON, not an object, wrong shape, wrong length, non-numeric)
"""
import json

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st
from prometheus_client import REGISTRY, CollectorRegistry

from app.main import create_app, load_reference
from app.settings import Settings
from app.store import HistogramStore
from tests.conftest import REFERENCE, REQUEST, RESPONSE, FakeRedis, cloudevent, scored


def bins(redis, name):
    """bin -> count of one input, summed over the minute buckets written."""
    out = {}
    for key, h in redis.data.items():
        if key.endswith(f":{name}"):
            for b, c in h.items():
                out[b.decode()] = out.get(b.decode(), 0) + c
    return out


def test_request_event_counts_features_only(client, redis):
    r = cloudevent(client, REQUEST, {"instances": [{"amount": 50_000.0}, {"amount": 900_000.0, "unknown": 1}]})
    assert r.status_code == 202 and r.content == b""   # any body would be read by the broker as a reply event
    assert bins(redis, "amount") == {"0": 1, "2": 1} and bins(redis, "score") == {}
    assert all(ttl == Settings().bucket_ttl_s for ttl in redis.ttl.values())
    assert set(redis.ttl) == set(redis.data)   # every bucket written gets the TTL


def test_response_event_counts_score_only(client, redis):
    r = cloudevent(client, RESPONSE, {"predictions": [0.005, 0.05], "model_version": "7"})
    assert r.status_code == 202 and r.content == b""
    assert bins(redis, "score") == {"0": 1, "1": 1} and bins(redis, "amount") == {}


def test_missing_feature_lands_in_the_missing_bin(client, redis):
    cloudevent(client, REQUEST, {"instances": [{}, {"amount": None}]})
    assert bins(redis, "amount") == {"missing": 2}


def test_positional_instances_follow_the_profile_order():
    ref = {"model_version": "7", "inputs": {"a": {"edges": [10.0], "shares": {}},
                                            "b": {"edges": [10.0], "shares": {}},
                                            "score": {"edges": [0.5], "shares": {}}}}
    r = FakeRedis()
    with TestClient(create_app(Settings(), lambda: r, ref, registry=CollectorRegistry())) as c:
        assert cloudevent(c, REQUEST, {"instances": [[5, 50]]}).status_code == 202
    assert (bins(r, "a"), bins(r, "b")) == ({"0": 1}, {"1": 1})


def test_events_counter_by_type(client):
    def count(kind):
        return REGISTRY.get_sample_value("drift_events_total", {"type": kind}) or 0
    before = count("request"), count("response")
    scored(client, [(1.0, 0.1)] * 3)
    assert (count("request") - before[0], count("response") - before[1]) == (3, 3)


def test_unknown_type_is_rejected(client, redis, caplog):
    r = cloudevent(client, "org.kubeflow.serving.inference.outlier", {"instances": [{"amount": 1.0}]}, ce_id="x9")
    assert r.status_code == 400 and "unknown ce-type" in r.json()["detail"] and redis.data == {}
    assert "rejected event x9" in caplog.text


def test_missing_type_is_rejected(client):
    r = client.post("/v1/inference-events", json={"instances": [{"amount": 1.0}]})
    assert r.status_code == 422


def test_non_inference_calls_are_ignored(client, redis, caplog):
    # the Scoring API's readiness check, GET /v1/models/card-fraud, logged by the agent like a predict
    assert cloudevent(client, REQUEST, b"", ce_id="g1").status_code == 202
    assert cloudevent(client, RESPONSE, {"name": "card-fraud", "ready": True}, ce_id="g1").status_code == 202
    assert redis.data == {} and "rejected" not in caplog.text


@pytest.mark.parametrize("ce_type, body", [
    (REQUEST, b"not json"),
    (REQUEST, {"instances": [[1.0, 2.0, 3.0]]}),       # positional, wrong length
    (REQUEST, {"instances": [{"amount": "abc"}]}),     # non-numeric value
    (REQUEST, {"instances": ["x"]}),                   # neither dict nor list
    (REQUEST, {"instances": 5}),                       # not a list of instances
    (REQUEST, [1, 2]),                                 # not an object
    (RESPONSE, {"predictions": [{"score": 0.1}]}),
])
def test_malformed_body_is_rejected(client, redis, caplog, ce_type, body):
    r = cloudevent(client, ce_type, body)
    assert r.status_code == 400 and "malformed" in r.json()["detail"] and redis.data == {}
    assert "rejected" in caplog.text and " event e1: " in caplog.text   # the ce-id, to find the pair


def test_drift_report_stable_when_live_matches_reference(client):
    # amounts in the reference shares 30/50/20, scores 90/8/2
    pairs = ([(50_000, 0.005)] * 27 + [(50_000, 0.05)] * 3 + [(250_000, 0.005)] * 45 + [(250_000, 0.05)] * 3
             + [(250_000, 0.5)] * 2 + [(900_000, 0.005)] * 18 + [(900_000, 0.05)] * 2)
    scored(client, pairs)
    rep = client.get("/v1/drift").json()
    assert rep["drift"] is False and rep["model_version"] == "7" and rep["window_minutes"] == 5
    assert {i["name"]: i["level"] for i in rep["inputs"]} == {"amount": "stable", "score": "stable"}
    assert all(i["live_count"] == 100 for i in rep["inputs"])


def test_drift_report_major_when_amounts_shift(client):
    scored(client, [(5_000_000, 0.005)] * 100)
    rep = client.get("/v1/drift").json()
    assert rep["drift"] is True and rep["inputs"][0]["name"] == "amount" and rep["inputs"][0]["level"] == "major"


@pytest.mark.parametrize("n, level, drift", [(99, "too_few_events", False), (100, "major", True)])
def test_small_window_never_drifts(client, n, level, drift):
    scored(client, [(5_000_000, 0.005)] * n)
    rep = client.get("/v1/drift").json()
    amount = next(i for i in rep["inputs"] if i["name"] == "amount")
    assert (amount["level"], rep["drift"], amount["live_count"]) == (level, drift, n) and amount["psi"] > 0.25


def test_drift_report_without_data(client):
    rep = client.get("/v1/drift").json()
    assert rep["drift"] is False and all(i["level"] == "no_data" for i in rep["inputs"])


def test_psi_gauge_follows_the_report(client):
    scored(client, [(5_000_000, 0.005)])
    psi = next(i["psi"] for i in client.get("/v1/drift").json()["inputs"] if i["name"] == "amount")
    assert REGISTRY.get_sample_value("drift_psi", {"input": "amount"}) == pytest.approx(psi, abs=1e-4)
    assert "http_requests_total" in client.get("/metrics").text


@settings(max_examples=30, deadline=None)
@given(amounts=st.lists(st.floats(1, 1e9), min_size=1, max_size=50))
def test_drift_report_is_idempotent(amounts):
    r = FakeRedis()
    app = create_app(Settings(), lambda: r, REFERENCE, registry=CollectorRegistry())
    with TestClient(app) as c:
        scored(c, [(a, 0.005) for a in amounts])
        first = c.get("/v1/drift").json()
        assert all(c.get("/v1/drift").json() == first for _ in range(3))


async def test_buckets_are_named_by_minute():
    r = FakeRedis()
    await HistogramStore(r, {"amount": [100.0]}, 3600, clock=lambda: 3659.9).add([{"amount": 50.0}])
    assert list(r.data) == ["drift:60:amount"]   # second 3659.9 is minute 60


async def test_empty_add_writes_nothing():
    r = FakeRedis()
    assert await HistogramStore(r, {"amount": [100.0]}, 3600).add([]) == 0 and r.data == {}


async def test_window_only_sums_recent_minutes():
    now = [600.0]   # minute 10
    store = HistogramStore(FakeRedis(), {"amount": [100.0]}, 3600, clock=lambda: now[0])
    row = {"amount": 50.0}
    await store.add([row])
    now[0] = 600.0 + 60 * 4             # minute 14
    await store.add([row, row])
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
