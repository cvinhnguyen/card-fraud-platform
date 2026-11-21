"""A/B report of the fraud model: per arm and model version, from the decision log (Kafka fraud_predictions).

Per row: the thresholds the model decided with (its own t_low / t_high, logged per decision), decisions, decline
and step-up rate, p95 latency of features + model (latency_ms, logged since the A/B split), and where the ground
truth already has labels, frauds, PR-AUC of the scores and recall (share of the frauds that were not approved). ab_arm is the card's arm; model_version is the model that scored (a failing challenger is
answered by production, and "fallback" is the amount rule). Older records without ab_arm show "-".

Usage (host; Kafka on 127.0.0.1:9092):
  python ml/tools/ab_report.py [--since 2026-11-20]
  python ml/tools/ab_report.py --self-check
"""
import argparse
import json
from pathlib import Path

import pandas as pd
from drift_by_day import ROOT, average_precision


def read_log(since: str) -> pd.DataFrame:
    """Every decision with auth date >= since; the last copy of an auth_id wins."""
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": "127.0.0.1:9092", "group.id": "ab_report", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe(["fraud_predictions"])
    rows, idle = {}, 0
    while idle < 5:
        msgs = c.consume(1000, timeout=1.0)
        idle = 0 if msgs else idle + 1
        for m in msgs:
            e = json.loads(m.value())
            if e["auth_ts"][:10] >= since:
                rows[e["auth_id"]] = {"transaction_id": e["auth_id"].replace("auth_", "txn_"),
                                      "ab_arm": e.get("ab_arm") or "-", "model_version": e["model_version"],
                                      "decision": e["decision"], "score": e["score"], "latency_ms": e.get("latency_ms"),
                                      "t_low": e.get("t_low"), "t_high": e.get("t_high")}
    c.close()
    return pd.DataFrame(rows.values())


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (ab_arm, model_version); df has the read_log columns plus is_fraud (NaN = no label yet)."""
    out = []
    for (arm, version), g in df.groupby(["ab_arm", "model_version"]):
        lab = g[g.is_fraud.notna()]
        fraud = lab[lab.is_fraud == 1]
        scored = lab[lab.score.notna()]
        th = g[["t_low", "t_high"]].dropna() if "t_low" in g else g.iloc[:0]
        out.append({"ab_arm": arm, "model_version": version,
                    "thresholds": f"{th.t_low.iloc[-1]:.4g} / {th.t_high.iloc[-1]:.4g}" if len(th) else None,
                    "decisions": len(g),
                    "decline_rate": round((g.decision == "decline").mean(), 4),
                    "step_up_rate": round((g.decision == "step_up").mean(), 4),
                    "p95_ms": round(g.latency_ms.quantile(0.95), 1) if g.latency_ms.notna().any() else None,
                    "labelled": len(lab), "frauds": len(fraud),
                    "pr_auc": round(average_precision(scored.is_fraud, scored.score), 3)
                    if (scored.is_fraud == 1).any() else None,
                    "recall": round((fraud.decision != "approve").mean(), 3) if len(fraud) else None})
    return pd.DataFrame(out)


def self_check() -> None:
    """Two arms, a hand-checked answer."""
    df = pd.DataFrame({
        "ab_arm": ["production"] * 4 + ["challenger"] * 2,
        "model_version": ["1", "1", "1", "1", "2", "2"],
        "decision": ["decline", "approve", "step_up", "approve", "decline", "approve"],
        "score": [0.9, 0.1, 0.3, 0.2, 0.8, 0.7],
        "latency_ms": [10.0, 20.0, 30.0, 40.0, None, None],
        "is_fraud": [1, 0, 1, None, 1, 0],
        "t_low": [0.002, 0.002, 0.002, 0.002, 0.05, 0.05], "t_high": [0.06, 0.06, 0.06, 0.06, 0.5, 0.5]})
    r = summarize(df).set_index("ab_arm")
    p, c = r.loc["production"], r.loc["challenger"]
    assert (p.decisions, p.decline_rate, p.step_up_rate, p.labelled, p.frauds) == (4, 0.25, 0.25, 3, 2)
    assert p.pr_auc == 1.0 and p.recall == 1.0 and p.p95_ms == 38.5
    assert c.pr_auc == 1.0 and c.recall == 1.0 and pd.isna(c.p95_ms)
    assert (p.thresholds, c.thresholds) == ("0.002 / 0.06", "0.05 / 0.5")
    print("ab_report self-check OK")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--since", default="0000", help="first auth date (YYYY-MM-DD); default: the whole log")
    ap.add_argument("--labels", type=Path, default=ROOT / "data" / "drift" / "labels" / "labels.parquet")
    ap.add_argument("--self-check", action="store_true")
    a = ap.parse_args()
    if a.self_check:
        self_check()
    else:
        log = read_log(a.since)
        if log.empty:
            raise SystemExit(f"no decisions in fraud_predictions since {a.since}")
        labels = pd.read_parquet(a.labels) if a.labels.exists() else pd.DataFrame(columns=["transaction_id", "is_fraud"])
        print(summarize(log.merge(labels[["transaction_id", "is_fraud"]], on="transaction_id", how="left"))
              .to_string(index=False))
