"""Run pipelines/flink/rt_features.py on a replay and recompute every feature with pandas.

Same semantics as the job: arrival order, first copy of each auth_id only, and for each authorization the same
card's authorizations that arrived before it (or are it) with auth_ts in [auth_ts - 1 h, auth_ts].

Usage (repo root; stream profile up):
  python pipelines/flink/check_features.py --replay-from "2025-03-01 00:00" --limit 100000
"""
import argparse
import subprocess
import sys
import time

import pandas as pd

from check_windows import ROOT, load, read_all, reset_topics

FEATURES = ["auth_count_10m", "amount_sum_1h", "distinct_merchants_1h", "distinct_units_1h", "distinct_cities_raw_1h"]


def truth(src: pd.DataFrame) -> pd.DataFrame:
    """The same features as rt_features.py, recomputed with pandas from the replayed slice."""
    m = pd.read_csv(ROOT / "generator" / "ref" / "province_mapping.csv")
    unit = {**dict(zip(m.new_unit_name, m.new_unit_name)), **dict(zip(m.old_province_name, m.new_unit_name))}
    rows, seen = [], {}
    for a in src.drop_duplicates("auth_id").itertuples():   # arrival order
        prev = seen.setdefault(a.card_id, [])
        prev.append(a)
        hour = [p for p in prev if a.auth_ts - pd.Timedelta(hours=1) <= p.auth_ts <= a.auth_ts]
        rows.append({"auth_id": a.auth_id,
                     "auth_count_10m": sum(p.auth_ts >= a.auth_ts - pd.Timedelta(minutes=10) for p in hour),
                     "amount_sum_1h": sum(p.amount_vnd for p in hour),
                     "distinct_merchants_1h": len({p.merchant_id for p in hour}),
                     "distinct_units_1h": len({unit[p.geo_city] for p in hour}),
                     "distinct_cities_raw_1h": len({p.geo_city for p in hour})})
    return pd.DataFrame(rows).set_index("auth_id")


def main(args: argparse.Namespace) -> None:
    """Load the replay, run the job, compare every feature row with the pandas truth."""
    reset_topics(["card_auth_replay", "card_features_rt"])
    subprocess.run([sys.executable, str(ROOT / "generator" / "producer.py"), "--replay-from", args.replay_from,
                    "--limit", str(args.limit), "--speed", "0", "--target", "card_auth_replay"], check=True)
    t0 = time.perf_counter()
    subprocess.run(["docker", "exec", "flink-jobmanager", "flink", "run", "-py", "/opt/flink/jobs/rt_features.py"],
                   check=True, capture_output=True)
    print(f"job: {time.perf_counter() - t0:.0f} s")
    flink = pd.DataFrame(read_all("card_features_rt")).set_index("auth_id")[FEATURES]
    src = load(ROOT / "data" / "source" / "stream" / "card_authorizations", pd.Timestamp(args.replay_from), args.limit)
    want = truth(src)
    print(f"{len(src):,} events, {len(want):,} unique auths, {len(flink):,} feature rows")
    for f in FEATURES:
        print(f"  {f:<24} match {(flink[f] == want[f].reindex(flink.index)).mean():.2%}, mean {flink[f].mean():,.3f}")
    print(f"auths whose card was in >= 2 places within the hour: by raw name {int(flink.distinct_cities_raw_1h.ge(2).sum()):,}, "
          f"by unit {int(flink.distinct_units_1h.ge(2).sum()):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--replay-from", required=True)
    ap.add_argument("--limit", type=int, required=True)
    main(ap.parse_args())
