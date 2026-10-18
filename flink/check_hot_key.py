"""Run flink/hot_key.py in both modes: compare its unit counts with the Parquet truth and print, per subtask of each
operator, the records read and the share of time busy / backpressured / idle (Flink REST API, after the job ends).

Truth: the same replayed slice (first --limit events by received_ts from --replay-from), every message counted
(retries included, as the job does), per unit per day of auth_ts (UTC, as Flink windows).

Usage (repo root; stream profile up):
  python flink/check_hot_key.py --replay-from "2025-01-01 00:00" --limit 1000000
  ... --modes two_phase --salts 16 --salt-all --no-replay      # salt every unit, reuse the loaded replay
"""
import argparse
import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "generator"))
sys.path.insert(0, str(ROOT / "flink"))
from check_windows import read_all, reset_topics  # noqa: E402
from producer import load  # noqa: E402

REST = "http://127.0.0.1:8088"
MODES = ["hot", "two_phase"]


def rest(path: str) -> dict:
    """GET a Flink REST endpoint and decode the JSON."""
    with urllib.request.urlopen(REST + path) as r:
        return json.load(r)


def subtask_table(job_id: str) -> pd.DataFrame:
    """One row per (operator, subtask): records in and accumulated busy / backpressured / idle time as shares."""
    rows = []
    for v in rest(f"/jobs/{job_id}")["vertices"]:
        for s in rest(f"/jobs/{job_id}/vertices/{v['id']}")["subtasks"]:
            m = s["metrics"]
            t = m["accumulated-busy-time"] + m["accumulated-backpressured-time"] + m["accumulated-idle-time"] or 1
            rows.append({"operator": v["name"][:60], "subtask": s["subtask"], "records_in": m["read-records"],
                         "busy": m["accumulated-busy-time"] / t, "backpressured": m["accumulated-backpressured-time"] / t,
                         "idle": m["accumulated-idle-time"] / t})
    return pd.DataFrame(rows)


def main(args: argparse.Namespace) -> None:
    """Load the replay, run each mode, print correctness and per-subtask load."""
    if not args.no_replay:
        reset_topics(["card_auth_replay"])
        subprocess.run([sys.executable, str(ROOT / "generator" / "producer.py"), "--replay-from", args.replay_from,
                        "--limit", str(args.limit), "--speed", "0", "--target", "card_auth_replay"], check=True)
    src = load(ROOT / "data" / "source" / "stream" / "card_authorizations", pd.Timestamp(args.replay_from), args.limit)
    mapping = pd.read_csv(ROOT / "generator" / "ref" / "province_mapping.csv")
    unit = dict(zip(mapping.old_province_name, mapping.new_unit_name)) | {u: u for u in mapping.new_unit_name}
    start_ms = (src.auth_ts.dt.floor("1D") - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)
    truth = src.assign(unit=src.geo_city.map(unit), window_start=start_ms).groupby(["unit", "window_start"]).size()
    shares = src.geo_city.map(unit).value_counts(normalize=True)
    print(f"replay: {len(src):,} events, {len(truth):,} unit-days; top units "
          + ", ".join(f"{u} {s:.1%}" for u, s in shares.head(3).items()) + "\n")

    for mode in args.modes:
        reset_topics([f"unit_counts_{mode}", f"unit_late_{mode}"])
        cmd = ["docker", "exec", "flink-jobmanager", "flink", "run", "-py", "/opt/flink/jobs/hot_key.py", "--mode", mode,
               "--salts", str(args.salts)] + ["--salt-all"] * args.salt_all
        out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
        job_id = re.search(r"JobID ([0-9a-f]{32})", out).group(1)
        res = pd.DataFrame(read_all(f"unit_counts_{mode}")).set_index(["unit", "window_start"])["count"]
        late = len(read_all(f"unit_late_{mode}"))
        j = pd.concat([truth.rename("truth"), res.rename("flink")], axis=1).fillna(0)
        job = rest(f"/jobs/{job_id}")
        salted = f" ({args.salts} salts, {'all units' if args.salt_all else 'hot unit only'})" if mode == "two_phase" else ""
        print(f"== {mode}{salted}: job {job['duration'] / 1000:.1f} s, {len(res):,} results, late {late}, "
              f"unit-days equal to truth {(j.truth == j.flink).mean():.2%}")
        t = subtask_table(job_id)
        t = t[~t.operator.str.startswith("Source")]
        print(t.to_string(index=False, formatters={c: "{:.0%}".format for c in ["busy", "backpressured", "idle"]}))
        print()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--replay-from", required=True)
    ap.add_argument("--limit", type=int, required=True)
    ap.add_argument("--modes", nargs="+", default=MODES, choices=MODES)
    ap.add_argument("--salts", type=int, default=8)
    ap.add_argument("--salt-all", action="store_true")
    ap.add_argument("--no-replay", action="store_true", help="reuse the replay already in card_auth_replay")
    main(ap.parse_args())
