"""Property-based tests (hypothesis): invariants that must hold for every input, not just the chosen examples.

  idempotency   scoring the same authorization again gives the same response (no hidden state)
  monotonicity  a higher score never gets a milder decision
  totality      every valid request gets a decision, fallback or not, never an exception
"""
import asyncio

from hypothesis import given, settings
from hypothesis import strategies as st

from app.policy import ThresholdPolicy
from app.schemas import Channel, Decision, ScoreRequest
from tests.conftest import FakeFeatures, FakeModel, make_scorer

SEVERITY = {Decision.approve: 0, Decision.step_up: 1, Decision.decline: 2}
requests = st.builds(
    ScoreRequest,
    auth_id=st.text("abcdef0123456789", min_size=1, max_size=64),
    card_id=st.text("abc123", min_size=1, max_size=64),
    customer_id=st.just("cus_1"), merchant_id=st.just("mer_1"),
    amount_vnd=st.integers(1, 10_000_000_000),
    channel=st.sampled_from(list(Channel)),
    currency=st.sampled_from(["VND", "USD", "SGD"]),
    auth_method=st.sampled_from(["3ds_otp", "3ds_app", "chip_pin", "contactless", "pin"]),
    device_id=st.one_of(st.none(), st.text("dev0123", min_size=1, max_size=64)),
    auth_ts=st.datetimes())


@settings(max_examples=200, deadline=None)
@given(req=requests)
def test_scoring_is_idempotent(req):
    scorer = make_scorer(FakeFeatures({"amount": float(req.amount_vnd)}), FakeModel())
    first = asyncio.run(scorer.score(req))
    again = [asyncio.run(scorer.score(req)) for _ in range(3)]
    assert all(r == first for r in again)


@settings(max_examples=500)
@given(a=st.floats(0, 1), b=st.floats(0, 1), lo=st.floats(0, 1), hi=st.floats(0, 1))
def test_decision_is_monotonic_in_score(a, b, lo, hi):
    lo, hi = min(lo, hi), max(lo, hi)
    p = ThresholdPolicy(lo, hi)
    low, high = min(a, b), max(a, b)
    assert SEVERITY[p.decide(low)] <= SEVERITY[p.decide(high)]


@settings(max_examples=100, deadline=None)
@given(req=requests, fail=st.sampled_from([None, "features", "model"]))
def test_every_request_gets_a_decision(req, fail):
    feats = FakeFeatures(error=RuntimeError()) if fail == "features" else FakeFeatures()
    model = FakeModel(error=RuntimeError()) if fail == "model" else FakeModel()
    r = asyncio.run(make_scorer(feats, model).score(req))
    assert r.decision in Decision and r.fallback == (fail is not None)
