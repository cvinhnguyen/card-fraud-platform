"""Run flink/ato_sequence.py in arrival order and in event-time order on a replay of app events, and compare.

Truth: sessions whose events, sorted by event_ts, contain password_reset -> add_payee -> transfer within 10 minutes;
and among them the new_device ATO sessions from the generator (data/source/_truth).

Usage (repo root; stream profile up):
  python flink/check_ato.py --replay-from "2024-03-01 00:00" --limit 150000   # before the biometric rule stops most ATO
"""
import argparse
import subprocess
import sys

import pandas as pd

from check_windows import ROOT, load, read_all, reset_topics

ORDERS = ["arrival", "event"]


def pattern_sessions(events: pd.DataFrame) -> set:
    """Sessions whose first password_reset, add_payee and transfer are in order within 10 minutes (event time)."""
    first = events.pivot_table(index="session_id", columns="event_type", values="event_ts", aggfunc="min")
    ok = ((first.password_reset < first.add_payee) & (first.add_payee < first.transfer)
          & (first.transfer - first.password_reset <= pd.Timedelta(minutes=10)))
    return set(first.index[ok])


def main(args: argparse.Namespace) -> None:
    """Load the replay, run both orders, compare with the generator's ATO truth."""
    reset_topics(["app_events_replay"] + [f"ato_alerts_{o}" for o in ORDERS])
    subprocess.run([sys.executable, str(ROOT / "generator" / "producer.py"), "--topic", "app_events", "--replay-from",
                    args.replay_from, "--limit", str(args.limit), "--speed", "0", "--target", "app_events_replay"], check=True)
    src = load(ROOT / "data" / "source" / "stream" / "app_events", pd.Timestamp(args.replay_from), args.limit)
    ato = set(pd.read_parquet(ROOT / "data" / "source" / "_truth" / "ato_sessions.parquet").query("kind == 'new_device'").session_id)
    # Only sessions fully inside the replay (no event cut off at either end).
    whole = src.groupby("session_id").size().index.difference(
        pd.Index(src.session_id[:1]).union(pd.Index(src.session_id[-1:])))
    truth = pattern_sessions(src[src.session_id.isin(whole)])
    print(f"{len(src):,} events, {src.session_id.nunique():,} sessions; full pattern in event time: {len(truth):,} sessions, "
          f"{len(truth & ato):,} of them new_device ATO")
    for order in ORDERS:
        subprocess.run(["docker", "exec", "flink-jobmanager", "flink", "run", "-py", "/opt/flink/jobs/ato_sequence.py",
                        "--order", order], check=True, capture_output=True)
        found = {a["session_id"] for a in read_all(f"ato_alerts_{order}")}
        print(f"  {order:<8} alerts {len(found):>5,}  pattern caught {len(found & truth):>5,} / {len(truth):,} "
              f"({len(found & truth) / len(truth):.1%})  ATO caught {len(found & truth & ato):,} / {len(truth & ato):,}  "
              f"alerts not in truth {len(found - truth):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--replay-from", required=True)
    ap.add_argument("--limit", type=int, required=True)
    main(ap.parse_args())
