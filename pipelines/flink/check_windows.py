"""Run pipelines/flink/auth_windows.py in each mode and compare its window counts with the truth from the Parquet files.

Truth: the same replayed slice (first --limit events by received_ts from --replay-from), one count per auth_id,
per card per 5-minute window of auth_ts. A window "matches" when Flink's final count equals the truth.

Usage (repo root; stream profile up). Loads the replay into the 1-partition topic card_auth_replay, then runs:
  python pipelines/flink/check_windows.py --replay-from "2026-06-01 00:00" --limit 20000
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "generator"))
from producer import load  # noqa: E402

BROKER = "127.0.0.1:9092"
MODES = ["baseline", "lateness", "dedup"]


def reset_topics(names: list[str]) -> None:
    """Delete and recreate the topics: 1 partition, no time-based retention."""
    admin = AdminClient({"bootstrap.servers": BROKER})
    old = [t for t in names if t in admin.list_topics(timeout=10).topics]
    for f in admin.delete_topics(old).values() if old else []:
        f.result()
    time.sleep(2)
    # Flink stamps results with event time (2026): time-based retention would delete them right away.
    new = [NewTopic(t, num_partitions=1, replication_factor=1, config={"retention.ms": "-1"}) for t in names]
    for f in admin.create_topics(new).values():
        f.result()


def read_all(topic: str) -> list[dict]:
    """Read a 1-partition topic from the start to its end (or a 120 s deadline)."""
    c = Consumer({"bootstrap.servers": BROKER, "group.id": f"check-{time.time()}", "auto.offset.reset": "earliest",
                  "enable.partition.eof": True})
    c.subscribe([topic])
    out, deadline = [], time.time() + 120
    while time.time() < deadline:   # the first polls return nothing while the consumer joins its group
        m = c.poll(1)
        if m is None:
            continue
        if m.error() and m.error().code() == -191:   # _PARTITION_EOF: the single partition is read to the end
            break
        if not m.error():
            out.append(json.loads(m.value()))
    c.close()
    return out


def main(args: argparse.Namespace) -> None:
    """Load the replay, run each mode, compare its final window counts with the truth."""
    reset_topics(["card_auth_replay"])
    subprocess.run([sys.executable, str(ROOT / "generator" / "producer.py"), "--replay-from", args.replay_from,
                    "--limit", str(args.limit), "--speed", "0", "--target", "card_auth_replay"], check=True)
    src = load(ROOT / "data" / "source" / "stream" / "card_authorizations", pd.Timestamp(args.replay_from), args.limit)
    uniq = src.drop_duplicates("auth_id")
    start_ms = (uniq.auth_ts.dt.floor("5min") - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)   # as Flink, naive = UTC
    truth = uniq.assign(window_start=start_ms).groupby(["card_id", "window_start"]).size().rename("truth")
    print(f"replay: {len(src):,} events, {len(uniq):,} unique auths, {len(truth):,} card windows\n")
    print(f"{'mode':<9} {'run s':>6} {'results':>8} {'counted':>8} {'late out':>8} {'windows ok':>11} {'over':>6} {'under':>6}")
    for mode in args.modes:
        reset_topics([f"auth_counts_{mode}", f"auth_late_{mode}"])
        t0 = time.perf_counter()
        subprocess.run(["docker", "exec", "flink-jobmanager", "flink", "run", "-py", "/opt/flink/jobs/auth_windows.py",
                        "--mode", mode], check=True, capture_output=True)
        secs = time.perf_counter() - t0
        res = pd.DataFrame(read_all(f"auth_counts_{mode}"))
        final = res.groupby(["card_id", "window_start"])["count"].max()   # updates only ever add events
        late = len(read_all(f"auth_late_{mode}"))
        j = pd.concat([truth, final.rename("flink")], axis=1).fillna(0)
        ok = (j.truth == j.flink).mean()
        print(f"{mode:<9} {secs:>6.0f} {len(res):>8,} {int(final.sum()):>8,} {late:>8,} {ok:>11.2%} "
              f"{int((j.flink > j.truth).sum()):>6,} {int((j.flink < j.truth).sum()):>6,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--replay-from", required=True)
    ap.add_argument("--limit", type=int, required=True)
    ap.add_argument("--modes", nargs="+", default=MODES, choices=MODES)
    main(ap.parse_args())
