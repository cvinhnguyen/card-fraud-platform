"""Checks on the training set before any model sees it. A failed check stops the run (pipeline step validate_data).

  schema      every feature of the feature service is present and numeric
  nulls       per-feature null share under its limit (history features are NULL before a card's first active
              day: allowed up to 30%; a whole column of NULLs means a broken join)
  labels      fraud share per split inside the expected range (train is downsampled, val / test are not)
  volume      every split has rows and frauds
  keys        one row per transaction (a reader that ignores Delta deletion vectors would return MERGE-deleted rows)
"""
from dataclasses import dataclass, field

import pandas as pd

from fraud_ml.data import KEY, LABEL

LABEL_RATE = {"train": (0.03, 0.40), "val": (0.0005, 0.02), "test": (0.0005, 0.02)}   # data: ~0.3% mature fraud
DEFAULT_MAX_NULL = 0.05
MAX_NULL = {   # features that are legitimately NULL often
    "avg_amount_90d": 0.3, "std_amount_90d": 0.3, "ecom_ratio_90d": 0.3, "foreign_txn_ratio_90d": 0.3,
    "txn_count_30d": 0.3, "distinct_merchants_30d": 0.3, "dispute_count_180d": 0.3, "amount_to_avg_ratio": 0.3,
    "days_since_last_active": 0.3,   # NULL = the card's first activity: no history yet
    "days_since_card_issue": 0.01,
}
MAX_ALL_NULL = 0.999   # no feature may be (almost) entirely NULL


@dataclass
class DataValidator:
    """Runs every check and returns a report; `raise_if_failed` stops the pipeline."""

    features: list[str]
    results: list[dict] = field(default_factory=list)

    def _check(self, name: str, ok: bool, detail) -> None:
        """Record one check result."""
        self.results.append({"check": name, "ok": bool(ok), "detail": detail})

    def validate(self, df: pd.DataFrame) -> list[dict]:
        """All checks on the training set (one row per transaction, with `split` and the label)."""
        dup = int(df[KEY].duplicated().sum())
        self._check("keys: transaction_id unique", dup == 0, dup)
        missing = [f for f in self.features if f not in df]
        self._check("schema: features present", not missing, missing)
        for f in (f for f in self.features if f in df):
            self._check(f"schema: {f} numeric", pd.api.types.is_numeric_dtype(df[f]), str(df[f].dtype))
            null = float(df[f].isna().mean())
            limit = min(MAX_NULL.get(f, DEFAULT_MAX_NULL), MAX_ALL_NULL)
            self._check(f"nulls: {f} <= {limit:.0%}", null <= limit, round(null, 4))
        for split, (lo, hi) in LABEL_RATE.items():
            part = df[df.split == split]
            rate = float(part[LABEL].mean()) if len(part) else 0.0
            self._check(f"volume: {split} rows and frauds", len(part) > 0 and part[LABEL].sum() > 0,
                        {"rows": len(part), "frauds": int(part[LABEL].sum())})
            self._check(f"labels: {split} fraud rate in [{lo}, {hi}]", lo <= rate <= hi, round(rate, 5))
        return self.results

    def raise_if_failed(self) -> None:
        """Stop the run with the list of failed checks."""
        failed = [r for r in self.results if not r["ok"]]
        if failed:
            raise ValueError(f"{len(failed)} of {len(self.results)} data checks failed: {failed}")
