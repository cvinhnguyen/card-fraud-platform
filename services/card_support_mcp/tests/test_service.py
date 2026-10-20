"""The service with fake adapters: what each tool returns and what it sends to the scoring API."""
import re
from datetime import timedelta

import pytest

from app.schemas import Channel, ScorePaymentRequest
from tests.conftest import CARD, NOW, FakeEmbedder, FakeFeatures, FakeKnowledge, FakeScoring, make_service


async def test_card_data_found_with_aged_realtime(service):
    data = await service.card_data("card_00001")
    assert data.found and data.profile.network == "visa" and data.daily.txn_count_30d == 12
    assert data.daily.last_active_day.isoformat() == "2026-10-19T00:00:00+00:00"
    assert (data.realtime.auth_count_10m, data.realtime.minutes_since_last_auth) == (3, 5.0)


async def test_card_data_stale_realtime_is_zero():
    svc = make_service(features=FakeFeatures(values=CARD | {"rt_ts": NOW - timedelta(minutes=20)}))
    rt = (await svc.card_data("card_00001")).realtime
    assert (rt.auth_count_10m, rt.amount_sum_1h, rt.minutes_since_last_auth) == (0, 9_000_000, 20.0)


async def test_card_data_unknown_card(service):
    data = await service.card_data("card_99999")
    assert not data.found and data.profile is None and data.realtime is None


async def test_chunk_found_and_unknown(service):
    assert (await service.chunk("faq#3")).text == "Lock your card."
    assert await service.chunk("faq#99") is None


async def test_search_reranks_and_drops_unknown_candidates():
    knowledge, embedder = FakeKnowledge(), FakeEmbedder()
    res = await make_service(knowledge=knowledge, embedder=embedder).search("someone used my card", 2)
    assert [h.chunk_id for h in res.hits] == ["card_terms#4.2", "faq#3"]   # by cosine, not by the stores' order
    assert res.hits[0].score > res.hits[1].score
    assert embedder.asked == ["someone used my card"]
    assert knowledge.asked == [([1.0, 0.1], 6)]                             # the question's vector, k x 3


async def test_search_with_no_candidates_returns_no_hits():
    res = await make_service(knowledge=FakeKnowledge(chunks={})).search("anything", 3)
    assert res.hits == []


async def test_score_sends_a_marked_authorization_and_maps_the_answer():
    scoring = FakeScoring()
    req = ScorePaymentRequest(card_id="card_00001", customer_id="cus_1", amount_vnd=4_500_000, channel=Channel.pos)
    res = await make_service(scoring=scoring).score(req)
    body = scoring.bodies[0]
    assert re.fullmatch(r"agent-[0-9a-f]{16}", body.pop("auth_id"))
    assert body == {"card_id": "card_00001", "customer_id": "cus_1", "merchant_id": "agent-what-if",
                    "amount_vnd": 4_500_000, "channel": "pos", "currency": "VND", "device_id": None,
                    "auth_ts": NOW.isoformat()}
    assert (res.score, res.decision, res.fallback, res.model_version) == (0.94, "decline", False, "1")


async def test_score_without_optional_fields_uses_defaults():
    scoring = FakeScoring(response={"decision": "approve"})
    req = ScorePaymentRequest(card_id="c", customer_id="u", amount_vnd=1, channel=Channel.atm)
    res = await make_service(scoring=scoring).score(req)
    assert (res.score, res.fallback, res.model_version) == (None, False, "unknown")


async def test_score_reports_the_rule_fallback():
    scoring = FakeScoring(response={"decision": "step_up", "score": None, "fallback": True, "model_version": 2})
    req = ScorePaymentRequest(card_id="c", customer_id="u", amount_vnd=1, channel=Channel.ecom)
    res = await make_service(scoring=scoring).score(req)
    assert (res.decision, res.fallback, res.model_version) == ("step_up", True, "2")


async def test_score_propagates_scoring_errors():
    req = ScorePaymentRequest(card_id="c", customer_id="u", amount_vnd=1, channel=Channel.atm)
    with pytest.raises(ConnectionError):
        await make_service(scoring=FakeScoring(error=ConnectionError("down"))).score(req)
