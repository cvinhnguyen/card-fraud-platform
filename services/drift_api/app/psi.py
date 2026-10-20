"""Population Stability Index between a reference profile (training data) and live counts on the same bins.

  PSI = sum over bins (live% - ref%) * ln(live% / ref%)      < 0.1 stable, 0.1-0.25 moderate, > 0.25 major drift
Bins are the reference's quantile edges plus one bin for missing values, so a feature that suddenly goes NULL
(a broken join, a dead stream) drifts too.
"""
import bisect
import math

EPS = 1e-4         # floor for empty bins: ln(0) is undefined
MODERATE, MAJOR = 0.1, 0.25
MISSING = "missing"


def bin_of(value: float | None, edges: list[float]) -> str:
    """Bin label of a value: index of the quantile interval (an edge closes its bin: (e[i-1], e[i]]), or `missing`.
    Closed on the right so a value equal to an edge has a bin of its own side: for a 0/1 flag whose deciles are all
    0 (edges [0.0]) 0 -> "0" and 1 -> "1"; bisect_right put both in "1" and the flag could never drift."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return MISSING
    return str(bisect.bisect_left(edges, value))


def psi(reference: dict[str, float], live: dict[str, int]) -> float | None:
    """PSI of live counts against reference shares (same bin labels); None without live data."""
    total = sum(live.values())
    if total == 0:
        return None
    out = 0.0
    for b in set(reference) | set(live):
        e = max(reference.get(b, 0.0), EPS)
        a = max(live.get(b, 0) / total, EPS)
        out += (a - e) * math.log(a / e)
    return out


def level(value: float | None) -> str:
    """Drift level of a PSI value."""
    if value is None:
        return "no_data"
    return "major" if value > MAJOR else "moderate" if value > MODERATE else "stable"
