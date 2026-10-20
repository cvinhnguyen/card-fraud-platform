"""Reference profile of the model's inputs: quantile edges + share of each bin (and of missing values), measured on
the validation period with the model's scores. The drift API and the drift job compare live data with it (PSI);
bin labels are the same as services/drift_api/app/psi.py: index of the quantile interval, or "missing".
"""
import bisect
import math

import numpy as np
import pandas as pd

QUANTILES = [i / 10 for i in range(1, 10)]
MISSING = "missing"
EPS = 1e-4


def _bin(value, edges: list[float]) -> str:
    """Same rule as the drift API (an edge closes its bin, so 0/1 flags with all-zero deciles keep 0 and 1 apart)."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return MISSING
    return str(bisect.bisect_left(edges, value))


def build(df: pd.DataFrame, model_version: str | None = None) -> dict:
    """{"model_version", "inputs": {column: {"edges": [...], "shares": {bin: share}}}} for every column of df."""
    inputs = {}
    for c in df.columns:
        s = pd.to_numeric(df[c], errors="coerce").astype("float64")
        edges = sorted(set(np.nanquantile(s, QUANTILES).round(6).tolist())) if s.notna().any() else []
        bins = s.map(lambda v: _bin(v, edges))
        inputs[c] = {"edges": edges, "shares": bins.value_counts(normalize=True).round(6).to_dict()}
    return {"model_version": model_version, "inputs": inputs}


def psi_against(profile: dict, df: pd.DataFrame) -> dict[str, float]:
    """PSI of every profiled column present in df."""
    out = {}
    for c, spec in profile["inputs"].items():
        if c not in df:
            continue
        live = pd.to_numeric(df[c], errors="coerce").astype("float64").map(lambda v: _bin(v, spec["edges"]))
        a, e = live.value_counts(normalize=True).to_dict(), spec["shares"]
        out[c] = float(sum((max(a.get(b, 0), EPS) - max(e.get(b, 0), EPS)) * math.log(max(a.get(b, 0), EPS) / max(e.get(b, 0), EPS))
                           for b in set(a) | set(e)))
    return out
