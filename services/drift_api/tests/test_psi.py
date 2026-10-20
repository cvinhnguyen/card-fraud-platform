"""PSI math. Equivalence partitions / boundary values:

  level(psi)    no_data None | stable [0, 0.1] | moderate (0.1, 0.25] | major above 0.25   (boundaries 0.1, 0.25)
  bin_of(v)     missing None / NaN | below the first edge | exactly on an edge (closes its bin) | above the last edge
"""
import math

import pytest
from hypothesis import given
from hypothesis import strategies as st

from app.psi import EPS, MISSING, bin_of, level, psi


@pytest.mark.parametrize("value, expected", [
    (None, "no_data"), (0.0, "stable"), (0.1, "stable"), (0.1 + 1e-9, "moderate"),
    (0.25, "moderate"), (0.25 + 1e-9, "major"), (3.0, "major")])
def test_level_boundaries(value, expected):
    assert level(value) == expected


@pytest.mark.parametrize("value, expected", [
    (None, MISSING), (math.nan, MISSING), (-1.0, "0"), (99.9, "0"), (100.0, "0"), (100.1, "1"), (300.0, "1"),
    (500.0, "1"), (500.1, "2"), (1e12, "2")])
def test_bin_of_partitions(value, expected):
    assert bin_of(value, [100.0, 500.0]) == expected


def test_flag_with_all_zero_deciles_can_drift():
    # a 0/1 feature that is 0 in > 90% of the reference: deciles all 0 -> edges [0.0]; 0 and 1 must not share a bin
    assert (bin_of(0.0, [0.0]), bin_of(1.0, [0.0])) == ("0", "1")
    assert level(psi({"0": 0.97, "1": 0.03}, {"0": 60, "1": 40})) == "major"


def test_identical_distribution_has_zero_psi():
    assert psi({"0": 0.25, "1": 0.75}, {"0": 25, "1": 75}) == pytest.approx(0.0, abs=1e-12)


def test_shifted_distribution_is_major():
    assert level(psi({"0": 0.8, "1": 0.2}, {"0": 20, "1": 80})) == "major"


@pytest.mark.parametrize("ref, live, expected", [
    ({"0": 0.5, "1": 0.5}, {"0": 80, "1": 20}, 0.3 * math.log(1.6) + 0.3 * math.log(2.5)),   # the docs example, 0.416
    ({"0": 0.5, "1": 0.5}, {"0": 10}, 0.5 * math.log(2) + (EPS - 0.5) * math.log(EPS / 0.5)),   # a bin empty live
    ({"0": 1.0}, {"0": 5, MISSING: 5}, -0.5 * math.log(0.5) + (0.5 - EPS) * math.log(0.5 / EPS)),   # a bin new live
])
def test_psi_exact_values(ref, live, expected):
    # every bin of either side counts, an empty side is floored at EPS (mutmut: & for |, = for +=, other defaults)
    assert psi(ref, live) == pytest.approx(expected)


def test_no_live_data_is_none():
    assert psi({"0": 1.0}, {}) is None


def test_new_bin_in_live_data_counts_as_drift():
    assert psi({"0": 1.0}, {"0": 50, MISSING: 50}) > 0.25


shares = st.lists(st.floats(0.01, 1), min_size=2, max_size=8).map(lambda w: [x / sum(w) for x in w])


@given(ref=shares, counts=st.lists(st.integers(0, 1000), min_size=2, max_size=8))
def test_psi_is_never_negative(ref, counts):
    live = {str(i): c for i, c in enumerate(counts)}
    v = psi({str(i): s for i, s in enumerate(ref)}, live)
    assert v is None if sum(counts) == 0 else v >= 0


@given(ref=shares, scale=st.integers(1, 1000))
def test_live_proportional_to_reference_is_stable(ref, scale):
    counts = {str(i): s * 1_000_000 * scale for i, s in enumerate(ref)}
    assert psi({str(i): s for i, s in enumerate(ref)}, counts) == pytest.approx(0.0, abs=1e-6)
