"""Layer 2: a live period after the training data, with the data drift and concept drift set in config.yaml `drift`.

Same customers, cards, merchants and phones as generate.py (its setup is replayed with the same seed), then the
days of `drift.start .. drift.end` with their own random streams, each day under the drift phases covering it:
  sale_season   11/11 festival: more payments, more of them online, larger amounts        (data drift)
  fraud_shift   fraud rings move to large night-time payments from the victim's own phone (concept drift: the
                "new device" signal the model leans on fades, so recall drops while inputs barely move)
Writes under data/drift/ (gitignored):
  stream/card_authorizations/auth_date=D/   live authorizations   -> producer.py --stream-dir data/drift/stream
  requests/auth_date=D/                     what the switch sends to the scoring API (ml/tools/replay_scoring.py)
  labels/labels.parquet                     ground truth, 2 columns: transaction_id, is_fraud (--upload: to
                                            s3://lakehouse/ml/ground_truth/, read by the drift DAG)
  core-banking-export/transactions/export_date=D+1/   nightly exports (--upload: to S3, so DP1-DP3 continue)

Usage: python generator/generate_drift.py [--upload]
"""
import argparse
import copy
import sys
import time

import boto3
import numpy as np
import pandas as pd
import yaml

import generate as g

OUT = g.ROOT / "data" / "drift"
FIRST_ID = 0x1_0000_0000   # transaction / auth ids after every id of the training period


def world(cfg: dict):
    """The entities of generate.py, rebuilt by replaying its setup calls in the same order with the same seed."""
    rng = np.random.default_rng(cfg["seed"])
    mapping = pd.read_csv(g.MAPPING_CSV)
    p_units, backmap = g.unit_probs(cfg, mapping, rng), g.back_map_table(cfg, mapping)
    customers = g.gen_customers(rng, cfg, p_units, backmap)
    versions = g.customer_versions(rng, cfg, customers, p_units, backmap)
    homes = versions[["customer_id", "updated_ts", "_unit"]].sort_values("updated_ts")
    _, cards = g.gen_accounts_cards(rng, customers)
    merchants, p_merchant = g.gen_merchants(rng, cfg, p_units)
    devices = g.gen_devices(np.random.default_rng([cfg["seed"], 1]), cfg, len(customers))
    return cards, homes, merchants, p_merchant, p_units, backmap, devices


def day_settings(cfg: dict, day: pd.Timestamp) -> tuple[dict, dict, list[str]]:
    """The day's config (fraud rate / risk weights overridden) and knobs of every phase covering the day."""
    c, knobs, names = copy.deepcopy(cfg), {}, []
    for ph in cfg["drift"]["phases"]:
        if pd.Timestamp(ph["from"]) <= day <= pd.Timestamp(ph.get("to", cfg["drift"]["end"])):
            names.append(ph["name"])
            if "fraud_rate" in ph:
                c["labels"]["fraud_rate"] = ph["fraud_rate"]
            c["labels"]["risk_weights"].update(ph.get("risk_weights", {}))
            knobs.update({k: v for k, v in ph.items() if k not in ("name", "from", "to", "fraud_rate", "risk_weights")})
    return c, knobs, names


def main(upload: bool) -> None:
    """Generate every live day; print the drift per phase; optionally upload the exports."""
    t0 = time.perf_counter()
    cfg = yaml.safe_load((g.ROOT / "generator" / "config.yaml").read_text(encoding="utf-8"))
    d = cfg["drift"]
    cards, homes, merchants, p_merchant, p_units, backmap, devices = world(cfg)
    known = pd.read_parquet(g.ROOT / cfg["output_dir"] / "core_banking" / "cards.parquet", columns=["card_id"])
    assert (known.card_id.to_numpy()[:1000] == cards.card_id.to_numpy()[:1000]).all(), "world differs from generate.py"
    ids, first, _ = devices
    card_cust = cards.set_index("card_id")._cust
    rng, arng = np.random.default_rng([cfg["seed"], 20]), np.random.default_rng([cfg["seed"], 21])
    online = merchants.is_online.to_numpy()
    next_id, rows, labels = FIRST_ID, [], []
    for day in pd.date_range(d["start"], d["end"], freq="D"):
        c, k, phases = day_settings(cfg, day)
        n = int(rng.poisson(d["daily_transactions"] * k.get("volume_scale", 1.0)))
        p_day = g.day_merchant_probs(c, p_merchant, merchants, day)
        p_day = np.where(online, p_day * k.get("online_weight", 1.0), p_day)
        df, _ = g.gen_day(rng, c, day, n, next_id, cards, homes, merchants, p_day / p_day.sum(), p_units, backmap)
        if (scale := k.get("amount_scale", 1.0)) != 1.0:
            atm = df.channel == "atm"
            df.loc[~atm, "amount_vnd"] = (np.round(df.amount_vnd[~atm] * scale / 1000) * 1000).astype("int64")
        txns = df.iloc[:n]
        fraud = g.fraud_flags(rng, c, txns)   # on the drifted amounts, with the day's weights
        cust = card_cust.reindex(txns.card_id).to_numpy()
        auths = g.gen_auths(arng, c, txns, fraud, cust, devices)
        sel = pd.Series(False, index=auths.index)
        if share := k.get("fraud_known_device_share", 0.0):   # malware on the victim's phone: a known device
            hit = txns.transaction_id[fraud & (txns.channel == "ecom").to_numpy() & (arng.random(n) < share)]
            own = pd.Series(ids[first[cust]], index=txns.transaction_id.to_numpy())
            sel = auths.auth_id.str.replace("auth_", "txn_", regex=False).isin(hit)
            auths.loc[sel, "device_id"] = own.reindex(auths.auth_id[sel].str.replace("auth_", "txn_", regex=False)).to_numpy()
        export_date = (day + pd.Timedelta(days=1)).date()
        g.write(df, OUT / "core-banking-export" / "transactions" / f"export_date={export_date}" / "part-0.parquet")
        g.write(auths, OUT / "stream" / "card_authorizations" / f"auth_date={day.date()}" / "part-0.parquet")
        req = (auths.drop_duplicates("auth_id").assign(transaction_id=lambda a: a.auth_id.str.replace("auth_", "txn_", regex=False))
               .merge(txns[["transaction_id", "currency", "auth_method"]], on="transaction_id")   # live days: after 2024-07-01
               .assign(customer_id=lambda a: cards.set_index("card_id")._customer_id.reindex(a.card_id).to_numpy()))
        g.write(req.sort_values("received_ts"), OUT / "requests" / f"auth_date={day.date()}" / "part-0.parquet")
        labels.append(pd.DataFrame({"transaction_id": txns.transaction_id.to_numpy(), "is_fraud": fraud.astype("int8")}))
        rows.append({"day": day.date(), "phases": "+".join(phases) or "-", "payments": n,
                     "ecom_share": (txns.channel == "ecom").mean(), "avg_amount_vnd": txns.amount_vnd.mean(),
                     "fraud_rate": fraud.mean(), "frauds": int(fraud.sum()),
                     "known_device_frauds": int(sel.sum())})
        next_id += n
    g.write(pd.concat(labels, ignore_index=True), OUT / "labels" / "labels.parquet")
    by = pd.DataFrame(rows).groupby("phases").agg(days=("day", "count"), payments_per_day=("payments", "mean"),
                                                  ecom_share=("ecom_share", "mean"), avg_amount_vnd=("avg_amount_vnd", "mean"),
                                                  fraud_rate=("fraud_rate", "mean"), frauds=("frauds", "sum"),
                                                  known_device_frauds=("known_device_frauds", "sum"))
    print(by.round(4).to_string())
    print(f"{next_id - FIRST_ID:,} payments, {d['start']} .. {d['end']} -> {OUT} ({time.perf_counter() - t0:.0f}s)")
    if upload:
        sys.path.insert(0, str(g.ROOT / "infra"))
        from make_env import read_env
        e = read_env()
        s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:8333", aws_access_key_id=e["S3_ACCESS_KEY"],
                          aws_secret_access_key=e["S3_SECRET_KEY"], region_name="us-east-1")
        files = sorted((OUT / "core-banking-export").rglob("*.parquet"))
        for f in files:
            s3.upload_file(str(f), "core-banking-export", f.relative_to(OUT / "core-banking-export").as_posix())
        print(f"uploaded {len(files)} export files to s3://core-banking-export/transactions/")
        s3.upload_file(str(OUT / "labels" / "labels.parquet"), "lakehouse", "ml/ground_truth/labels.parquet")
        print("uploaded the ground truth to s3://lakehouse/ml/ground_truth/labels.parquet (drift DAG)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--upload", action="store_true", help="upload the nightly exports for DP1")
    main(ap.parse_args().upload)
