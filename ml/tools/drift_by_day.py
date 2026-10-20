"""Replay report: PSI per business day from the prediction log, and live model quality per day from the ground truth.

The Drift API sums a wall-clock window (15 minutes); a replay packs a day into minutes, and any test traffic in that
window counts too. This report groups the logged decisions by the payment's own day instead, with the Drift API's
binning and PSI (services/drift_api/app/psi.py), so each row is one business day.

Usage (host, after ml/tools/replay_scoring.py; Kafka on 127.0.0.1:9092):
  python ml/tools/drift_by_day.py --days 2026-10-18,2026-10-27,2026-11-09
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "drift_api"))
from app.psi import bin_of, level, psi  # noqa: E402


def average_precision(y: pd.Series, score: pd.Series) -> float:
    """PR-AUC as average precision: mean precision at the rank of each fraud (scores sorted high to low)."""
    hits = y.to_numpy()[score.to_numpy().argsort()[::-1]]
    return float((hits.cumsum() / range(1, len(hits) + 1))[hits == 1].mean())


def read_log(days: set[str]) -> pd.DataFrame:
    """Every non-fallback decision of the given days; the last copy of an auth_id wins."""
    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": "drift_by_day", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe(["fraud_predictions"])
    rows, idle = {}, 0
    while idle < 5:
        msgs = c.consume(1000, timeout=1.0)
        idle = 0 if msgs else idle + 1
        for m in msgs:
            e = json.loads(m.value())
            if not e["fallback"] and e["auth_ts"][:10] in days:
                rows[e["auth_id"]] = {"day": e["auth_ts"][:10], "transaction_id": e["auth_id"].replace("auth_", "txn_"),
                                      "score": e["score"], **e["features"]}
    c.close()
    return pd.DataFrame(rows.values())


def main(days: list[str], reference: Path, labels: Path) -> None:
    """One row per day: payments, frauds, PR-AUC of the live scores, the three largest PSIs."""
    ref = json.loads(reference.read_text())["inputs"]
    df = read_log(set(days)).merge(pd.read_parquet(labels), on="transaction_id", how="left")
    out = []
    for day, g in df.groupby("day"):
        psis = {}
        for name, spec in ref.items():
            counts = Counter(bin_of(None if pd.isna(v) else float(v), spec["edges"]) for v in g[name])
            psis[name] = psi(spec["shares"], dict(counts))
        top = sorted(psis.items(), key=lambda kv: -kv[1])[:3]
        out.append({"day": day, "payments": len(g), "frauds": int(g.is_fraud.sum()),
                    "pr_auc": round(average_precision(g.is_fraud, g.score), 3),
                    **{f"psi_{i + 1}": f"{n} {v:.3f} ({level(v)})" for i, (n, v) in enumerate(top)}})
    print(pd.DataFrame(out).to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--days", required=True, help="comma-separated auth dates")
    ap.add_argument("--reference", type=Path, default=ROOT / "services" / "predictor" / "model" / "reference_profile.json")
    ap.add_argument("--labels", type=Path, default=ROOT / "data" / "drift" / "labels" / "labels.parquet")
    a = ap.parse_args()
    main(a.days.split(","), a.reference, a.labels)
