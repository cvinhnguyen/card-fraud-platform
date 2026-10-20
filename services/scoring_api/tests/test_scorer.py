"""FraudScorer: happy path, every fallback cause, timeouts, the prediction log record, stage metrics, the log line."""
import json
import logging

import httpx
import pytest

from app.schemas import Decision, ScoreRequest
from app.scorer import DECISIONS, FALLBACKS, STAGE
from tests.conftest import FakeFeatures, FakeLog, FakeModel, make_scorer, request_body


def req(**over) -> ScoreRequest:
    return ScoreRequest(**request_body(**over))


async def test_happy_path_uses_model_and_thresholds():
    s = make_scorer(model=FakeModel(score=0.7))
    r = await s.score(req())
    assert (r.score, r.decision, r.model_version, r.fallback) == (0.7, Decision.decline, "7", False)


@pytest.mark.parametrize("features, model, cause", [
    (FakeFeatures(error=ConnectionError("redis down")), FakeModel(), "features_error"),
    (FakeFeatures(delay_s=0.2), FakeModel(), "features_timeout"),
    (FakeFeatures(), FakeModel(error=httpx.ConnectError("kserve down")), "model_error"),
    (FakeFeatures(), FakeModel(delay_s=0.2), "model_timeout"),
])
async def test_fallback_on_any_dependency_failure(features, model, cause):
    log = FakeLog()
    before = FALLBACKS.labels(cause)._value.get()
    r = await make_scorer(features, model, log).score(req(amount_vnd=3_000_000))
    assert r.fallback and r.score is None and r.model_version == "fallback"
    assert r.decision == Decision.step_up                       # 3M >= the 2M fallback limit
    assert log.records[0]["fallback_cause"] == cause
    assert FALLBACKS.labels(cause)._value.get() == before + 1


async def test_model_not_called_when_features_fail():
    model = FakeModel()
    await make_scorer(FakeFeatures(error=RuntimeError("x")), model).score(req())
    assert model.calls == 0


async def test_prediction_log_record_has_request_response_and_features(scorer, fake_log):
    r = await scorer.score(req())
    rec = fake_log.records[0]
    assert rec["auth_id"] == r.auth_id and rec["card_id"] == "card_00001"
    assert rec["decision"] == r.decision.value and rec["score"] == r.score
    assert rec["features"]["txn_count_30d"] == 3.0 and rec["fallback_cause"] is None


async def test_feature_store_gets_the_request():
    features, r = FakeFeatures(), req()
    await make_scorer(features).score(r)
    assert features.requests == [r]


async def test_prediction_log_record_is_json_with_iso_timestamps(scorer, fake_log):
    await scorer.score(req())
    rec = fake_log.records[0]
    assert rec["auth_ts"] == "2026-10-16T09:30:00" and json.loads(json.dumps(rec)) == rec   # the Kafka value as is


@pytest.mark.parametrize("stage", ["features", "model"])
async def test_stage_latency_is_observed(stage):
    hist = STAGE.labels(stage)
    count, total = hist._sum.get(), sum(b.get() for b in hist._buckets)
    await make_scorer().score(req())
    assert sum(b.get() for b in hist._buckets) == total + 1 and 0 <= hist._sum.get() - count < 1.0   # a duration


async def test_fallback_is_logged_with_cause_request_and_error(caplog):
    with caplog.at_level(logging.WARNING, logger="app.scorer"):
        await make_scorer(FakeFeatures(error=ConnectionError("redis down"))).score(req())
    assert [r.getMessage() for r in caplog.records] == [
        "fallback (features_error) for auth_000000000001: ConnectionError('redis down')"]


async def test_scorer_without_log():
    r = await make_scorer(log=None).score(req())
    assert not r.fallback


async def test_decision_counter():
    before = DECISIONS.labels("approve", "false")._value.get()
    await make_scorer(model=FakeModel(score=0.01)).score(req())
    assert DECISIONS.labels("approve", "false")._value.get() == before + 1
