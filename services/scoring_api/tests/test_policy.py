"""Decision policies. Test cases come from equivalence partitioning + boundary value analysis:

  ThresholdPolicy(t_low=0.05, t_high=0.5), score in [0, 1]
    partitions: approve [0, 0.05)  step_up [0.05, 0.5)  decline [0.5, 1]
    boundaries: 0, just below / at each threshold, 1
  FallbackPolicy(step_up_from_vnd=2,000,000), amount > 0
    partitions: approve [1, 2M)  step_up [2M, max]
    boundaries: 1, 2M - 1, 2M, 2M + 1
  invalid configurations: t_low > t_high, below 0, above 1
"""
import pytest

from app.policy import FallbackPolicy, ThresholdPolicy
from app.schemas import Decision

EPS = 1e-9


@pytest.mark.parametrize("score, expected", [
    (0.0, Decision.approve),            # lower bound of the domain
    (0.02, Decision.approve),           # inside approve partition
    (0.05 - EPS, Decision.approve),     # just below t_low
    (0.05, Decision.step_up),           # at t_low
    (0.05 + EPS, Decision.step_up),     # just above t_low
    (0.2, Decision.step_up),            # inside step_up partition
    (0.5 - EPS, Decision.step_up),      # just below t_high
    (0.5, Decision.decline),            # at t_high
    (0.8, Decision.decline),            # inside decline partition
    (1.0, Decision.decline),            # upper bound of the domain
])
def test_threshold_policy_partitions_and_boundaries(score, expected):
    assert ThresholdPolicy(0.05, 0.5).decide(score) == expected


@pytest.mark.parametrize("t_low, t_high", [(0.6, 0.5), (-0.01, 0.5), (0.05, 1.01)])
def test_threshold_policy_rejects_invalid_thresholds(t_low, t_high):
    with pytest.raises(ValueError, match=f"got {t_low}, {t_high}"):   # the message names the bad values
        ThresholdPolicy(t_low, t_high)


@pytest.mark.parametrize("t_low, t_high", [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.3, 0.3)])
def test_threshold_policy_accepts_boundary_configurations(t_low, t_high):
    ThresholdPolicy(t_low, t_high)


def test_equal_thresholds_never_step_up():
    p = ThresholdPolicy(0.3, 0.3)
    assert [p.decide(s) for s in (0.29, 0.3, 0.31)] == [Decision.approve, Decision.decline, Decision.decline]


@pytest.mark.parametrize("amount, expected", [
    (1, Decision.approve),                  # smallest valid amount
    (500_000, Decision.approve),            # inside approve partition
    (1_999_999, Decision.approve),          # just below the limit
    (2_000_000, Decision.step_up),          # at the limit
    (2_000_001, Decision.step_up),          # just above
    (10_000_000_000, Decision.step_up),     # largest valid amount
])
def test_fallback_policy_never_declines(amount, expected):
    assert FallbackPolicy(2_000_000).decide(amount) == expected
