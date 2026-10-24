"""Run pipelines/flink/ato_cep.py (Flink CEP) and the hand-written ato_sequence.py --order event on one replay of app
events, and compare both with the expected sessions per pattern and with the generator's ATO truth.

Expected sessions, recomputed with pandas in event time: a session matches a pattern if some ordered set of its
events fits it inside the time limit (the same rules as the MATCH_RECOGNIZE definitions).

Usage (repo root; stream profile up):
  python pipelines/flink/check_ato_cep.py --replay-from "2024-09-01 00:00" --limit 300000   # after the biometric rule
"""
import argparse
import subprocess
import sys

import pandas as pd

from check_windows import ROOT, load, read_all, reset_topics

TEN, THIRTY = pd.Timedelta(minutes=10), pd.Timedelta(minutes=30)


def first_after(times: list, t) -> pd.Timestamp | None:
    """The earliest time in `times` after t (sorted list), or None."""
    return next((x for x in times if x > t), None)


def expected(events: pd.DataFrame) -> dict[str, set]:
    """Sessions matching each pattern; the earliest next event is the best candidate at every step."""
    out = {"takeover": set(), "blocked_takeover": set(), "split_transfers": set()}
    # every pattern starts with a password_reset or an add_payee: only those sessions can match
    starts = events.session_id[events.event_type.isin(["password_reset", "add_payee"])]
    for sid, s in events[events.session_id.isin(starts)].sort_values("event_ts").groupby("session_id"):
        at = {k: g.event_ts.tolist() for k, g in s.groupby("event_type")}
        failed = s[(s.event_type == "biometric_verify") & (s.result == "failed")
                   & s.reason.isin(["new_device", "amount"])].event_ts.tolist()
        for r in at.get("password_reset", []):
            p = first_after(at.get("add_payee", []), r)
            tr = first_after(at.get("transfer", []), p) if p is not None else None
            if tr is not None and tr - r <= TEN:
                out["takeover"].add(sid)
            v = first_after(failed, r)
            if v is not None and v - r <= TEN:
                out["blocked_takeover"].add(sid)
        for _, a in s[s.event_type == "add_payee"].iterrows():
            split = s[(s.event_type == "transfer") & (s.payee_id == a.payee_id) & (s.event_ts > a.event_ts)
                      & s.amount_vnd.between(9_000_000, 9_999_999)].event_ts
            if len(split) >= 2 and split.iloc[1] - a.event_ts <= THIRTY:
                out["split_transfers"].add(sid)
    return out


def main(args: argparse.Namespace) -> None:
    """Load the replay, run the hand-written job and the CEP job, print per pattern and per ATO kind."""
    reset_topics(["app_events_replay", "ato_alerts_event", "ato_alerts_cep"])
    subprocess.run([sys.executable, str(ROOT / "generator" / "producer.py"), "--topic", "app_events", "--replay-from",
                    args.replay_from, "--limit", str(args.limit), "--speed", "0", "--target", "app_events_replay"], check=True)
    src = load(ROOT / "data" / "source" / "stream" / "app_events", pd.Timestamp(args.replay_from), args.limit)
    # Only sessions fully inside the replay (no event cut off at either end).
    whole = src.groupby("session_id").size().index.difference(
        pd.Index(src.session_id[:1]).union(pd.Index(src.session_id[-1:])))
    want = expected(src[src.session_id.isin(whole)])
    truth = pd.read_parquet(ROOT / "data" / "source" / "_truth" / "ato_sessions.parquet")
    ato = {k: set(g.session_id) & set(whole) for k, g in truth.groupby("kind")}
    print(f"{len(src):,} events, {len(whole):,} whole sessions; ATO: "
          + ", ".join(f"{k} {len(v):,}" for k, v in ato.items()))

    for job in ("ato_sequence.py", "ato_cep.py"):
        extra = ["--order", "event"] if job == "ato_sequence.py" else []
        subprocess.run(["docker", "exec", "flink-jobmanager", "flink", "run", "-py", f"/opt/flink/jobs/{job}", *extra],
                       check=True, capture_output=True)
    hand = {a["session_id"] for a in read_all("ato_alerts_event")}
    cep = pd.DataFrame(read_all("ato_alerts_cep"))
    found = {p: set(g.session_id) for p, g in cep.groupby("pattern")}
    all_ato = set().union(*ato.values())

    print(f"\n{'pattern':<17}{'expected':>9}{'alerts':>8}{'caught':>8}{'not expected':>14}{'on ATO':>8}")
    for p, sessions in want.items():
        got = found.get(p, set())
        print(f"{p:<17}{len(sessions):>9,}{len(got):>8,}{len(got & sessions):>8,}{len(got - sessions):>14,}"
              f"{len(got & all_ato):>8,}")
    print(f"{'hand-written':<17}{len(want['takeover']):>9,}{len(hand):>8,}{len(hand & want['takeover']):>8,}"
          f"{len(hand - want['takeover']):>14,}{len(hand & all_ato):>8,}")

    any_cep = set().union(*found.values())
    print(f"\n{'ATO kind':<15}{'sessions':>9}{'hand-written':>14}{'CEP (any)':>11}")
    for k, sessions in ato.items():
        print(f"{k:<15}{len(sessions):>9,}{len(hand & sessions):>14,}{len(any_cep & sessions):>11,}")
    legit = any_cep - all_ato
    print(f"CEP alerts on sessions that are not ATO: {len(legit):,} of {len(any_cep):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--replay-from", required=True)
    ap.add_argument("--limit", type=int, required=True)
    main(ap.parse_args())
