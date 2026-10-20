"""100 GB offline dataset (P9): the nightly transaction exports at scale, generated in parallel, straight to S3.

Same generator code and config as generate.py (merchant + city skew, cross-border, old province names, 2% export
duplicates, auth columns from 2024-07-01), with larger volumes. Differences, all for scale:
  - work unit = (day, chunk of <= CHUNK rows) with its own random stream [seed, 10, day, chunk], so worker processes
    are independent and the output does not depend on the number of workers
  - each worker uploads its file to s3://core-banking-export/scale_100g/transactions/export_date=D/part-C.parquet
    and deletes the local copy: no 100 GB staging on disk
  - only what the scale benchmarks read: the exports and the merchants of this dataset (no stream, app events,
    disputes, Postgres load)
The 1 GB dataset (generate.py) is untouched: different prefix, different volumes.

Usage (core profile up):
  python generator/generate_scale.py --transactions 4050000000 --customers 2000000 --merchants 20000 --workers 5
  python generator/generate_scale.py --transactions 3000000000 --days 2 --no-upload      # measure speed, bytes per row
  python generator/generate_scale.py --transactions 4050000000 --every 51 --workers 3   # 20-day slice (scale_dedup)
"""
import argparse
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import yaml

import generate as g

sys.path.insert(0, str(g.ROOT / "infra"))
from make_env import read_env  # noqa: E402

BUCKET, PREFIX = "core-banking-export", "scale_100g"
CHUNK = 400_000   # rows per file; a worker holds one chunk (~0.6 GB of pandas objects) at a time
TMP = g.ROOT / "data" / "_scale_tmp"
W = {}            # per-worker state, built once by setup()


def scaled_config(path: Path, transactions: int, customers: int, merchants: int) -> dict:
    """config.yaml with the volumes of the 100 GB run."""
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    cfg["volume"].update(n_transactions=transactions, n_customers=customers, n_merchants=merchants)
    return cfg


def dims(cfg: dict) -> dict:
    """Customers, versions, cards and merchants exactly as generate.main builds them (same random stream)."""
    rng = np.random.default_rng(cfg["seed"])
    mapping = pd.read_csv(g.MAPPING_CSV)
    p_units, backmap = g.unit_probs(cfg, mapping, rng), g.back_map_table(cfg, mapping)
    customers = g.gen_customers(rng, cfg, p_units, backmap)
    versions = g.customer_versions(rng, cfg, customers, p_units, backmap)
    _, cards = g.gen_accounts_cards(rng, customers)
    merchants, p_merchant = g.gen_merchants(rng, cfg, p_units)
    # Customers as integer codes for gen_day's merge_asof (it only joins on them, they are not written): factorizing
    # 2M string keys for every 400k-row chunk was a quarter of the chunk time.
    codes = {c: i for i, c in enumerate(customers.customer_id)}
    homes = versions[["customer_id", "updated_ts", "_unit"]].sort_values("updated_ts")
    homes["customer_id"] = homes.customer_id.map(codes).astype("int64")
    cards["_customer_id"] = cards._customer_id.map(codes).astype("int64")
    return {"cfg": cfg, "rng": rng, "p_units": p_units, "backmap": backmap, "cards": cards, "merchants": merchants,
            "p_merchant": p_merchant, "homes": homes}


def setup(cfg: dict, upload: bool) -> None:
    """Pool initializer: rebuild the dims in this process (spawn on Windows shares nothing) and open an S3 client."""
    W.update(dims(cfg), s3=None)
    if not upload:
        return
    env = read_env()
    W["s3"] = boto3.client("s3", endpoint_url="http://127.0.0.1:8333", region_name="us-east-1",
                           aws_access_key_id=env["S3_ACCESS_KEY"], aws_secret_access_key=env["S3_SECRET_KEY"])


def work(task: tuple) -> tuple[int, int]:
    """Generate one chunk of one day, upload it, delete the local file. Returns (rows, bytes)."""
    day_i, day, chunk, n, first_id = task
    cfg = W["cfg"]
    rng = np.random.default_rng([cfg["seed"], 10, day_i, chunk])
    p_day = g.day_merchant_probs(cfg, W["p_merchant"], W["merchants"], day)
    df, _ = g.gen_day(rng, cfg, day, n, first_id, W["cards"], W["homes"], W["merchants"], p_day, W["p_units"],
                      W["backmap"])
    export_date = (day + pd.Timedelta(days=1)).date()
    local = TMP / f"{os.getpid()}-{export_date}-{chunk}.parquet"
    g.write(df, local)
    size = local.stat().st_size
    if W["s3"]:
        W["s3"].upload_file(str(local), BUCKET, f"{PREFIX}/transactions/export_date={export_date}/part-{chunk:03d}.parquet")
    local.unlink()
    return len(df), size


def main(args: argparse.Namespace) -> None:
    """Plan the (day, chunk) tasks, write the merchants, run the pool, report rows and size."""
    t0 = time.perf_counter()
    cfg = scaled_config(args.config, args.transactions, args.customers, args.merchants)
    d = dims(cfg)   # parent: per-day counts continue the same random stream as generate.main
    days = pd.date_range(cfg["period"]["start"], cfg["period"]["end"], freq="D")
    per_day = d["rng"].poisson(cfg["volume"]["n_transactions"] / len(days), len(days))
    tasks, first_id = [], 0
    for i, (day, n) in enumerate(zip(days, per_day)):
        sizes = [CHUNK] * (int(n) // CHUNK) + ([int(n) % CHUNK] if n % CHUNK else [])
        for c, m in enumerate(sizes):
            tasks.append((i, day, c, m, first_id))
            first_id += m
    tasks = [t for t in tasks if t[0] < args.days] if args.days else tasks
    tasks = [t for t in tasks if t[0] % args.every == 0]
    if args.skip_existing:   # each (day, chunk) has its own random stream, so a rerun writes identical files
        env = read_env()
        s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:8333", region_name="us-east-1",
                          aws_access_key_id=env["S3_ACCESS_KEY"], aws_secret_access_key=env["S3_SECRET_KEY"])
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/transactions/")
        done = {o["Key"] for p in pages for o in p.get("Contents", [])}
        key = lambda t: (f"{PREFIX}/transactions/export_date={(t[1] + pd.Timedelta(days=1)).date()}"
                         f"/part-{t[2]:03d}.parquet")
        tasks = [t for t in tasks if key(t) not in done]
        print(f"{len(done):,} files already in S3, {len(tasks):,} to write", flush=True)

    TMP.mkdir(parents=True, exist_ok=True)
    if not args.no_upload:
        s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:8333", region_name="us-east-1",
                          aws_access_key_id=read_env()["S3_ACCESS_KEY"], aws_secret_access_key=read_env()["S3_SECRET_KEY"])
        local = TMP / "merchants.parquet"
        g.write(d["merchants"], local)
        s3.upload_file(str(local), BUCKET, f"{PREFIX}/merchants.parquet")
    print(f"{len(d['cards']):,} cards, {len(d['merchants']):,} merchants; {len(tasks):,} files to write "
          f"({sum(t[3] for t in tasks):,} transactions + 2% duplicates), setup {time.perf_counter() - t0:.0f}s", flush=True)
    del d   # the workers rebuild their own copy; the parent only collects counts

    rows = size = 0
    with mp.Pool(args.workers, initializer=setup, initargs=(cfg, not args.no_upload)) as pool:
        for k, (r, b) in enumerate(pool.imap_unordered(work, tasks), 1):
            rows, size = rows + r, size + b
            if k % 100 == 0 or k == len(tasks):
                el = time.perf_counter() - t0
                print(f"{k:,}/{len(tasks):,} files, {rows:,} rows, {size / 2**30:.1f} GiB, {el / 60:.0f} min, "
                      f"{rows / el:,.0f} rows/s, {size / rows:.1f} B/row", flush=True)
    print(f"done: {rows:,} rows, {size / 2**30:.1f} GiB in s3://{BUCKET}/{PREFIX}/ ({(time.perf_counter() - t0) / 60:.0f} min)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    ap.add_argument("--transactions", type=int, required=True)
    ap.add_argument("--customers", type=int, default=2_000_000)
    ap.add_argument("--merchants", type=int, default=20_000)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--days", type=int, help="only the first N days (measure speed and bytes per row)")
    ap.add_argument("--every", type=int, default=1, help="only every K-th day: a slice spread over the period")
    ap.add_argument("--no-upload", action="store_true", help="write and delete locally, no S3 (speed test)")
    ap.add_argument("--skip-existing", action="store_true", help="resume: skip files already in S3")
    main(ap.parse_args())
