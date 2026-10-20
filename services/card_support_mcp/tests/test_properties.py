"""Property-based tests: rules that must hold for every input, not only for the examples above.

  idempotency  ranking the same candidates twice gives the same answer (an agent asking twice gets the same chunks)
  invariance   the order the candidates arrive in does not change the answer (Feast's order is not trusted)
  shape        best first, never more than k, scores within [-1, 1]
  freshness    a count is either the stored value or 0, and a younger timestamp is never less fresh
The freshness property also runs on the crosshair backend (symbolic execution of the rule).
"""
from datetime import datetime, timedelta, timezone

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from app import logic

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
vectors = st.lists(st.floats(-10, 10, allow_nan=False), min_size=3, max_size=3)
candidates = st.dictionaries(st.text("abcdef#.0123", min_size=1, max_size=6), vectors, min_size=1, max_size=12)


@given(vectors, candidates, st.integers(1, 15))
def test_rank_is_idempotent(query, emb, k):
    assert logic.rank(query, emb, k) == logic.rank(query, dict(emb), k)


@given(vectors, candidates, st.integers(1, 15), st.randoms())
def test_rank_ignores_candidate_order(query, emb, k, rnd):
    items = list(emb.items())
    rnd.shuffle(items)
    assert logic.rank(query, dict(items), k) == logic.rank(query, emb, k)


@given(vectors, candidates, st.integers(1, 15))
def test_rank_shape(query, emb, k):
    out = logic.rank(query, emb, k)
    scores = [s for _, s in out]
    assert len(out) == min(k, len(emb))
    assert scores == sorted(scores, reverse=True)
    assert all(-1.0 <= s <= 1.0 for s in scores)


counts = st.fixed_dictionaries({n: st.one_of(st.none(), st.integers(0, 10**9)) for n in logic.WINDOW_S})


@given(counts, st.integers(-86_400, 86_400))
def test_realtime_is_stored_value_or_zero(values, age_s):
    out = logic.realtime_now(values, NOW - timedelta(seconds=age_s), NOW)
    for name in logic.WINDOW_S:
        assert out[name] in (0, values[name])


@settings(backend="crosshair", max_examples=30, deadline=None, suppress_health_check=list(HealthCheck))
@given(st.integers(0, 10**6), st.integers(0, 7_200), st.integers(0, 7_200))
def test_younger_is_never_less_fresh_crosshair(value, age_a, age_b):
    younger, older = sorted((age_a, age_b))
    v = {n: value for n in logic.WINDOW_S}
    fresh_young = logic.realtime_now(v, NOW - timedelta(seconds=younger), NOW)
    fresh_old = logic.realtime_now(v, NOW - timedelta(seconds=older), NOW)
    for name in logic.WINDOW_S:
        assert fresh_young[name] >= fresh_old[name]
