"""Adapters with their transport mocked: KServe over HTTP (respx), Feast (fake store), Kafka (fake producer)."""
import asyncio

import httpx
import pytest
import respx

from app.clients import FeatureClient, ModelClient, PredictionLog
from app.schemas import ScoreRequest
from tests.conftest import request_body

URL = "http://predictor/v1/models/card-fraud:predict"


@respx.mock
async def test_model_client_sends_instances_and_reads_version():
    route = respx.post(URL).mock(return_value=httpx.Response(200, json={"predictions": [0.25], "model_version": "3"}))
    score, version = await ModelClient(URL, 1.0).predict({"amount": 1.0})
    assert (score, version) == (0.25, "3")
    assert route.calls.last.request.content == b'{"instances":[{"amount":1.0}]}'


@respx.mock
async def test_model_client_raises_on_http_error():
    respx.post(URL).mock(return_value=httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        await ModelClient(URL, 1.0).predict({})


@respx.mock
async def test_model_client_version_defaults_to_unknown():
    respx.post(URL).mock(return_value=httpx.Response(200, json={"predictions": [0.1]}))
    assert (await ModelClient(URL, 1.0).predict({}))[1] == "unknown"


async def test_model_budget_does_not_cancel_the_http_call():
    # the scorer's wait_for gives up, but the HTTP call must finish on its own (cancelled httpx calls exhausted the
    # connection pool after a burst: every later request fell back until a restart)
    release, finished = asyncio.Event(), []

    class SlowClient:
        async def post(self, url, json):
            await release.wait()
            finished.append(url)
            return httpx.Response(200, json={"predictions": [0.5]}, request=httpx.Request("POST", url))

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(ModelClient(URL, 1.0, client=SlowClient()).predict({}), 0.01)
    release.set()
    await asyncio.sleep(0)
    assert finished == [URL]


@respx.mock
async def test_model_client_ready_uses_model_endpoint():
    respx.get("http://predictor/v1/models/card-fraud").mock(return_value=httpx.Response(200, json={"ready": True}))
    assert await ModelClient(URL, 1.0).ready()


class Response:
    def __init__(self, d):
        self.d = d

    def to_dict(self):
        return self.d


class Store:
    """Fake feast.FeatureStore: records the entity rows it is asked for."""

    def __init__(self, repo_path):
        self.rows = None
        proj = type("P", (), {"features": [type("F", (), {"name": n})() for n in ("amount", "is_new_device")]})()
        self.service = type("S", (), {"feature_view_projections": [proj]})()

    def get_feature_service(self, name):
        return self.service

    async def get_online_features_async(self, features, entity_rows):
        self.rows = entity_rows
        return Response({"amount": [250000.0], "is_new_device": [1], "card_id": ["c"]})


async def test_feature_client_builds_entity_row_and_keeps_model_features(monkeypatch):
    monkeypatch.setattr("feast.FeatureStore", Store)
    fc = FeatureClient("/repo", "fraud_model_v1")
    out = await fc.features(ScoreRequest(**request_body(device_id=None, channel="pos")))
    assert out == {"amount": 250000.0, "is_new_device": 1}
    row = fc.store.rows[0]
    assert row["device_id"] == "-" and row["channel"] == "pos" and row["amount_vnd"] == 250_000
    assert await fc.ready()


class Producer:
    def __init__(self, **kw):
        self.sent, self.started, self.stopped = [], False, False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send(self, topic, value, key):
        self.sent.append((topic, value, key))


async def test_prediction_log_sends_json_keyed_by_card(monkeypatch):
    monkeypatch.setattr("aiokafka.AIOKafkaProducer", Producer)
    log = PredictionLog("kafka:9092", "fraud_predictions")
    await log.start()
    await log.send({"card_id": "card_1", "score": 0.5})
    await log.stop()
    topic, value, key = log.producer.sent[0]
    assert (topic, key, value) == ("fraud_predictions", b"card_1", b'{"card_id": "card_1", "score": 0.5}')
    assert log.producer.stopped


async def test_prediction_log_disabled_when_kafka_down(monkeypatch):
    class Down(Producer):
        async def start(self):
            raise ConnectionError("no broker")

    monkeypatch.setattr("aiokafka.AIOKafkaProducer", Down)
    log = PredictionLog("kafka:9092", "t")
    await log.start()
    await log.send({"card_id": "c"})   # no error: scoring must not depend on the log
    await log.stop()
    assert log.producer is None
