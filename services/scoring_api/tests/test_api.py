"""HTTP layer: routes, probes, metrics, lifespan, settings, tracing setup."""
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from app.main import build_scorer, create_app
from app.settings import Settings
from tests.conftest import FakeFeatures, FakeModel, make_scorer, request_body


def test_score_returns_decision(client):
    r = client.post("/v1/score", json=request_body(amount_vnd=250_000))
    assert r.status_code == 200
    body = r.json()
    assert body["decision"] in {"approve", "step_up", "decline"} and body["fallback"] is False


def test_lifespan_starts_and_stops_the_log(scorer, fake_log):
    app = create_app(lambda s: scorer, Settings(), registry=CollectorRegistry())
    with TestClient(app):
        assert fake_log.started
    assert fake_log.stopped


def test_healthz_never_checks_dependencies():
    app = create_app(lambda s: make_scorer(FakeFeatures(error=ConnectionError())), Settings(), registry=CollectorRegistry())
    with TestClient(app) as c:
        assert c.get("/healthz").json() == {"status": "ok"}


def test_readyz_ok(client):
    assert client.get("/readyz").status_code == 200


def test_readyz_503_when_feature_store_down():
    app = create_app(lambda s: make_scorer(FakeFeatures(error=ConnectionError("redis"))), Settings(),
                     registry=CollectorRegistry())
    with TestClient(app) as c:
        assert c.get("/readyz").status_code == 503


def test_readyz_503_when_model_not_ready():
    app = create_app(lambda s: make_scorer(model=FakeModel(error=RuntimeError())), Settings(), registry=CollectorRegistry())
    with TestClient(app) as c:
        assert c.get("/readyz").status_code == 503


def test_metrics_exposes_request_counts(client):
    client.post("/v1/score", json=request_body())
    text = client.get("/metrics").text
    assert 'http_requests_total{handler="/v1/score"' in text


def test_metrics_survive_feast_multiprocess_env(client, monkeypatch, tmp_path):
    # importing feast sets PROMETHEUS_MULTIPROC_DIR in the live process; the endpoint must ignore it
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    client.post("/v1/score", json=request_body())
    assert 'http_requests_total{handler="/v1/score"' in client.get("/metrics").text


def test_settings_from_environment(monkeypatch):
    monkeypatch.setenv("T_LOW", "0.1")
    monkeypatch.setenv("T_HIGH", "0.9")
    s = Settings()
    assert (s.t_low, s.t_high) == (0.1, 0.9)


def test_build_scorer_wires_real_clients(monkeypatch):
    class Store:   # stands in for feast.FeatureStore: no registry / Redis needed
        def __init__(self, repo_path):
            self.repo = repo_path

        def get_feature_service(self, name):
            return type("S", (), {"feature_view_projections": []})()

    monkeypatch.setattr("feast.FeatureStore", Store)
    s = build_scorer(Settings(t_low=0.1, t_high=0.2))
    assert s.policy.t_low == 0.1 and s.features.names == [] and s.prediction_log.topic == "fraud_predictions"
