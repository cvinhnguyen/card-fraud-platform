"""A/B test of the fraud model: sticky split by card, challenger failure handed to production, ab_arm in the log,
wiring from the settings. KServe is mocked over HTTP (respx), as in test_clients.py."""
import asyncio

import httpx
import pytest
import respx
from app.clients import ModelClient
from app.main import build_scorer
from app.schemas import ScoreRequest
from app.scorer import CHALLENGER_FAILURES, ab_arm
from app.settings import Settings

from tests.conftest import FakeLog, FakeModel, make_scorer, request_body
from tests.test_clients import Store

PROD = "http://predictor/v1/models/card-fraud:predict"
CHAL = "http://challenger/v1/models/card-fraud-challenger:predict"
CARDS = [f"card_{i:05d}" for i in range(2000)]


def req(card_id: str) -> ScoreRequest:
    return ScoreRequest(**request_body(card_id=card_id))


def ab_scorer(percent: int, challenger_status: int = 200, log=None):
    """Scorer with real ModelClients for both arms; production answers v1, the challenger v2."""
    respx.post(PROD).mock(return_value=httpx.Response(200, json={"predictions": [0.01], "model_version": "1"}))
    respx.post(CHAL).mock(return_value=httpx.Response(challenger_status,
                                                      json={"predictions": [0.9], "model_version": "2"}))
    s = make_scorer(model=ModelClient(PROD, 1.0), log=log, timeout_s=1.0)
    s.challenger, s.ab_percent = ModelClient(CHAL, 1.0), percent
    return s


def test_arm_is_sticky_and_close_to_the_share():
    arms = [ab_arm(c, 20) for c in CARDS]
    assert arms == [ab_arm(c, 20) for c in CARDS]                       # same card, same arm, every time
    assert 0.15 < arms.count("challenger") / len(CARDS) < 0.25          # ~20 % of the cards
    assert ab_arm("card_00001", 0) == "production" and ab_arm("card_00001", 100) == "challenger"


def test_arm_is_pinned_across_processes():
    # crc32 bucket of card_00042 is 19: pinned, so a restart or another worker never moves a card to the other arm
    assert (ab_arm("card_00042", 19), ab_arm("card_00042", 20)) == ("production", "challenger")


@respx.mock
async def test_challenger_scores_its_cards_and_production_the_rest():
    log = FakeLog()
    s = ab_scorer(20, log=log)
    for c in CARDS[:200]:
        r = await s.score(req(c))
        assert r.model_version == ("2" if ab_arm(c, 20) == "challenger" else "1") and not r.fallback
    assert {rec["ab_arm"] for rec in log.records} == {"production", "challenger"}
    assert all(rec["model_version"] == {"production": "1", "challenger": "2"}[rec["ab_arm"]] for rec in log.records)


@respx.mock
async def test_failing_challenger_falls_back_to_production_not_to_the_rule():
    card = next(c for c in CARDS if ab_arm(c, 20) == "challenger")
    log = FakeLog()
    before = CHALLENGER_FAILURES.labels("error")._value.get()
    r = await ab_scorer(20, challenger_status=503, log=log).score(req(card))
    assert (r.model_version, r.fallback, r.score) == ("1", False, 0.01)
    assert log.records[0]["ab_arm"] == "challenger"     # the card's arm; model_version says who scored
    assert CHALLENGER_FAILURES.labels("error")._value.get() == before + 1


async def test_slow_challenger_falls_back_to_production():
    class Slow(FakeModel):
        async def predict(self, features):
            await asyncio.sleep(1)

    card = next(c for c in CARDS if ab_arm(c, 50) == "challenger")
    before = CHALLENGER_FAILURES.labels("timeout")._value.get()
    s = make_scorer(model=FakeModel(score=0.01))
    s.challenger, s.ab_percent = Slow(), 50
    r = await s.score(req(card))
    assert (r.model_version, r.fallback) == ("7", False)
    assert CHALLENGER_FAILURES.labels("timeout")._value.get() == before + 1


async def test_split_off_logs_production_arm_and_latency():
    log = FakeLog()
    await make_scorer(log=log).score(req("card_00001"))
    assert log.records[0]["ab_arm"] == "production" and 0 <= log.records[0]["latency_ms"] < 1000


@pytest.mark.parametrize("percent, wired", [(0, False), (10, True)])
def test_build_scorer_wires_the_challenger_only_when_the_split_is_on(monkeypatch, percent, wired):
    monkeypatch.setattr("feast.FeatureStore", Store)
    s = build_scorer(Settings(ab_percent=percent, challenger_url=CHAL))
    assert (s.challenger is not None) == wired and s.ab_percent == percent
    if wired:
        assert s.challenger.url == CHAL


def test_ab_percent_is_a_share():
    with pytest.raises(ValueError):
        Settings(ab_percent=101)


async def test_each_arm_decides_with_its_own_thresholds_and_logs_them():
    # the same score 0.6: production's cut-offs (v1) step it up, the challenger's (a model scoring higher) decline it
    prod, chal = ("production", "challenger")
    cards = {arm: next(c for c in CARDS if ab_arm(c, 50) == arm) for arm in (prod, chal)}
    log = FakeLog()
    s = make_scorer(model=FakeModel(score=0.6, thresholds=(0.5, 0.95)), log=log)
    s.challenger, s.ab_percent = FakeModel(score=0.6, thresholds=(0.05, 0.5)), 50
    decisions = {arm: (await s.score(req(card))).decision.value for arm, card in cards.items()}
    assert decisions == {prod: "step_up", chal: "decline"}
    assert [(r["ab_arm"], r["t_low"], r["t_high"]) for r in log.records] == [(prod, 0.5, 0.95), (chal, 0.05, 0.5)]


async def test_model_without_thresholds_uses_the_configured_ones():
    log = FakeLog()
    r = await make_scorer(model=FakeModel(score=0.3), log=log, t_low=0.05, t_high=0.5).score(req("card_00001"))
    assert r.decision.value == "step_up" and (log.records[0]["t_low"], log.records[0]["t_high"]) == (0.05, 0.5)


async def test_fallback_rule_logs_no_thresholds():
    log = FakeLog()
    await make_scorer(model=FakeModel(error=RuntimeError("down")), log=log).score(req("card_00001"))
    assert (log.records[0]["t_low"], log.records[0]["t_high"]) == (None, None)
