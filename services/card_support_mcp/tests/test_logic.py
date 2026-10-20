"""Equivalence partitions and boundary values of the tool rules.

realtime_now partitions: no timestamp | fresh for every window | fresh for 1 h only | stale; boundaries at the
window edges (600 / 601 s, 3600 / 3601 s) and at 0 / negative age (replayed data ahead of the clock).
rank partitions: k below range | within | above the number of candidates; vectors identical / orthogonal /
opposite / zero / of different length; equal scores.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app import logic

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
VALUES = {"auth_count_10m": 4, "amount_sum_1h": 5_000_000, "distinct_merchants_1h": 3, "distinct_units_1h": 2}


@pytest.mark.parametrize("age_s, count_10m, sum_1h", [
    (0, 4, 5_000_000),          # just now
    (90, 4, 5_000_000),         # 1.5 minutes: minutes keep one decimal
    (600, 4, 5_000_000),        # 10-minute boundary: still fresh
    (601, 0, 5_000_000),        # just past it: the 10-minute count is stale, the 1-hour ones are not
    (3600, 0, 5_000_000),       # 1-hour boundary: still fresh
    (3601, 0, 0),               # just past it: everything is stale
    (-120, 4, 5_000_000),       # timestamp ahead of the clock (replay): age 0
])
def test_realtime_window_boundaries(age_s, count_10m, sum_1h):
    out = logic.realtime_now(VALUES, NOW - timedelta(seconds=age_s), NOW)
    assert (out["auth_count_10m"], out["amount_sum_1h"]) == (count_10m, sum_1h)
    assert out["minutes_since_last_auth"] == round(max(age_s, 0) / 60, 1)


def test_realtime_without_timestamp_is_all_zero():
    out = logic.realtime_now(VALUES, None, NOW)
    assert out == {"auth_count_10m": 0, "amount_sum_1h": 0, "distinct_merchants_1h": 0, "distinct_units_1h": 0,
                   "minutes_since_last_auth": None}


def test_realtime_missing_value_is_zero_even_when_fresh():
    out = logic.realtime_now({"auth_count_10m": None}, NOW, NOW)
    assert out["auth_count_10m"] == 0 and out["distinct_units_1h"] == 0


@pytest.mark.parametrize("value, expected", [
    (None, None),
    (0, datetime(1970, 1, 1, tzinfo=timezone.utc)),                        # epoch seconds
    (datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=timezone.utc)),     # naive = UTC
    (datetime(2026, 1, 1, 7, tzinfo=timezone(timedelta(hours=7))),
     datetime(2026, 1, 1, 7, tzinfo=timezone(timedelta(hours=7)))),        # aware: kept
])
def test_as_utc(value, expected):
    assert logic.as_utc(value) == expected


@pytest.mark.parametrize("a, b, expected", [
    ([1.0, 0.0], [2.0, 0.0], 1.0),        # same direction
    ([1.0, 0.0], [0.0, 3.0], 0.0),        # orthogonal
    ([1.0, 0.0], [-1.0, 0.0], -1.0),      # opposite
    ([0.0, 0.0], [1.0, 0.0], 0.0),        # zero vector
    ([1.0, 0.0], [1.0, 0.0, 0.0], 0.0),   # different length
])
def test_cosine(a, b, expected):
    assert logic.cosine(a, b) == pytest.approx(expected)


EMB = {"a": [1.0, 0.0], "b": [0.0, 1.0], "c": [0.7, 0.7]}


@pytest.mark.parametrize("k, ids", [(1, ["a"]), (2, ["a", "c"]), (3, ["a", "c", "b"]), (4, ["a", "c", "b"])])
def test_rank_k_boundaries(k, ids):
    assert [i for i, _ in logic.rank([1.0, 0.1], EMB, k)] == ids


@pytest.mark.parametrize("k", [0, -1])
def test_rank_rejects_k_below_one(k):
    with pytest.raises(ValueError, match="^k must be at least 1$"):
        logic.rank([1.0, 0.0], EMB, k)


def test_rank_ties_are_ordered_by_id():
    assert logic.rank([1.0, 0.0], {"z": [1.0, 0.0], "m": [2.0, 0.0]}, 2) == [("m", 1.0), ("z", 1.0)]
