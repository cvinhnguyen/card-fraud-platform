"""Generate synthetic core-banking source data for the card fraud platform.

Outputs stand in for another department's systems (loaded into Postgres / SeaweedFS later):
  <output_dir>/core_banking/{customers,accounts,cards,merchants,ref_province_mapping}.parquet
  <output_dir>/core-banking-export/transactions/export_date=YYYY-MM-DD/part-0.parquet

Problems injected in this version: merchant skew, txn_city skew, administrative
reorganization (63 old names -> 34 units), offline duplicates.
All identifiers are random synthetic tokens; no real card numbers (PAN) are produced.

Usage: python generator/generate.py [--config generator/config.yaml]
"""
import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parent.parent
MAPPING_CSV = Path(__file__).parent / "ref" / "province_mapping.csv"

# Share of card payments per hour of day (quiet at night, peaks at lunch and evening).
HOUR_WEIGHTS = np.array([1, 0.5, 0.3, 0.3, 0.4, 1, 2, 4, 5, 5, 5, 6, 7, 6, 5, 5, 5, 6, 7, 8, 7, 5, 3, 2], float)
HOUR_WEIGHTS /= HOUR_WEIGHTS.sum()

LAST = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Phan", "Vu", "Vo", "Dang", "Bui", "Do"]
MIDDLE = ["Van", "Thi", "Minh", "Ngoc", "Duc", "Thanh", "Hoang", "Quoc", "Gia", "Bao"]
FIRST = ["An", "Binh", "Chi", "Dung", "Giang", "Ha", "Hung", "Khanh", "Lan", "Linh",
         "Long", "Mai", "Nam", "Phuong", "Quan", "Son", "Trang", "Tuan", "Vy", "Yen"]
OTHER_MCC = {5812: "Restaurant", 5411: "Grocery", 5541: "Fuel", 5651: "Fashion", 5732: "Electronics",
             5912: "Pharmacy", 7011: "Hotel", 4511: "Airline", 5814: "Fast Food", 5999: "Retail"}


def tokens(prefix: str, rng: np.random.Generator, n: int) -> np.ndarray:
    """Return n unique synthetic ids like `card_7f3a91c2` (random hex, never a real card number)."""
    return np.array([f"{prefix}_{x:08x}" for x in rng.choice(2**32, n, replace=False)])


def random_ts(rng: np.random.Generator, start: str, end: str, n: int) -> pd.Series:
    """Return n timestamps uniformly distributed between two dates, at second precision."""
    lo, hi = pd.Timestamp(start).value // 10**9, pd.Timestamp(end).value // 10**9
    return pd.Series(pd.to_datetime(rng.integers(lo, hi, n), unit="s"))


def unit_probs(cfg: dict, mapping: pd.DataFrame, rng: np.random.Generator) -> pd.Series:
    """Probability of each of the 34 new units: configured shares, then a Zipf tail for `others`."""
    shares = {k: v for k, v in cfg["skew"]["txn_city"].items() if k != "others"}
    rest = [u for u in sorted(mapping.new_unit_name.unique()) if u not in shares]
    rng.shuffle(rest)   # random but seeded order of the long tail
    tail = 1 / np.arange(1, len(rest) + 1) ** 1.1
    probs = pd.Series({**shares, **dict(zip(rest, cfg["skew"]["txn_city"]["others"] * tail / tail.sum()))})
    assert len(probs) == 34 and abs(probs.sum() - 1) < 1e-9, "txn_city shares must cover 34 units and sum to 1"
    return probs


def back_map_table(cfg: dict, mapping: pd.DataFrame) -> dict[str, tuple[list[str], np.ndarray]]:
    """For each new unit, the old province names it came from and the weight of each."""
    reorg = cfg["reorganization"]
    table = {}
    for unit, grp in mapping.groupby("new_unit_name"):
        names = grp.old_province_name.tolist()
        if unit in reorg["back_map_weights"]:
            w = np.array([reorg["back_map_weights"][unit][n] for n in names], float)
        elif len(names) == 1:
            w = np.ones(1)
        else:
            is_namesake = (grp.old_province_code == grp.new_unit_code).to_numpy()
            w = np.where(is_namesake, reorg["namesake_weight"], (1 - reorg["namesake_weight"]) / (len(names) - 1))
        table[unit] = (names, w / w.sum())
    return table


def to_source_names(units: np.ndarray, ts: pd.Series, rng: np.random.Generator, cfg: dict, backmap: dict) -> np.ndarray:
    """Return the location string the source system stores: old names before the cutoff
    and for a stale share of rows after it, the new unit name otherwise."""
    reorg = cfg["reorganization"]
    use_old = (ts < pd.Timestamp(reorg["cutoff"])).to_numpy() | (rng.random(len(units)) < reorg["stale_share_after_cutoff"])
    out = units.astype(object)
    for unit in np.unique(units[use_old]):
        idx = np.flatnonzero(use_old & (units == unit))
        names, w = backmap[unit]
        out[idx] = rng.choice(names, idx.size, p=w)
    return out


def gen_customers(rng, cfg, p_units, backmap) -> pd.DataFrame:
    """One row per customer (single version for now; SCD2 versions come later).
    `_unit` is the true new-space unit, kept only for generating transactions."""
    n = cfg["volume"]["n_customers"]
    unit = rng.choice(p_units.index.to_numpy(), n, p=p_units.to_numpy())
    updated_ts = random_ts(rng, "2015-01-01", cfg["period"]["end"], n)   # onboarding time
    return pd.DataFrame({
        "customer_id": tokens("cust", rng, n),
        "full_name": [f"{a} {b} {c}" for a, b, c in zip(rng.choice(LAST, n), rng.choice(MIDDLE, n), rng.choice(FIRST, n))],
        "dob": random_ts(rng, "1955-01-01", "2006-01-01", n).dt.date,
        "city": to_source_names(unit, updated_ts, rng, cfg, backmap),
        "income_band": rng.choice(["low", "mid", "high", "very_high"], n, p=[0.35, 0.4, 0.2, 0.05]),
        "risk_tier": rng.choice(["low", "medium", "high"], n, p=[0.7, 0.25, 0.05]),
        "kyc_level": rng.choice([1, 2, 3], n, p=[0.2, 0.5, 0.3]),
        "updated_ts": updated_ts,
        "_unit": unit,
    })


def gen_accounts_cards(rng, customers) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One account per customer and 1-2 cards per account. Cards are sorted by issue_ts."""
    n = len(customers)
    is_credit = rng.random(n) < 0.3
    accounts = pd.DataFrame({
        "account_id": tokens("acc", rng, n),
        "customer_id": customers.customer_id.to_numpy(),
        "account_type": np.where(is_credit, "credit", "checking"),
        "open_ts": customers.updated_ts.to_numpy(),
        "status": "active",
        "credit_limit": np.where(is_credit, rng.choice([20, 50, 100, 200], n) * 1_000_000, np.nan),
    })
    per_acc = rng.choice([1, 2], n, p=[0.7, 0.3])
    acc_idx = np.repeat(np.arange(n), per_acc)
    issue_ts = accounts.open_ts.to_numpy()[acc_idx] + pd.to_timedelta(rng.integers(0, 30 * 86400, acc_idx.size), unit="s")
    cards = pd.DataFrame({
        "card_id": tokens("card", rng, acc_idx.size),
        "account_id": accounts.account_id.to_numpy()[acc_idx],
        "card_type": np.where(is_credit[acc_idx], "credit", "debit"),
        "network": rng.choice(["Visa", "Mastercard", "Napas"], acc_idx.size, p=[0.4, 0.3, 0.3]),
        "issue_ts": issue_ts,
        "status": "active",
        "_unit": customers._unit.to_numpy()[acc_idx],
    })
    cards["expiry_month"] = (cards.issue_ts + pd.DateOffset(years=5)).dt.strftime("%Y-%m")
    return accounts, cards.sort_values("issue_ts", ignore_index=True)


def gen_merchants(rng, cfg, p_units) -> tuple[pd.DataFrame, np.ndarray]:
    """Super merchants first, then 20 ATM merchants and a long tail. Returns merchants and
    the probability that a transaction goes to each merchant (the primary skew)."""
    sk = cfg["skew"]["merchant"]
    supers = pd.DataFrame(cfg["super_merchants"]).rename(columns={"name": "merchant_name"})
    n_other = cfg["volume"]["n_merchants"] - len(supers)
    mcc = np.where(np.arange(n_other) < 20, 6011, rng.choice(list(OTHER_MCC), n_other))
    others = pd.DataFrame({
        "merchant_name": [f"ATM Network {i}" if m == 6011 else f"{OTHER_MCC[m]} {i:05d}" for i, m in enumerate(mcc)],
        "mcc": mcc,
        "is_online": (mcc != 6011) & (rng.random(n_other) < 0.15),
    })
    merchants = pd.concat([supers, others], ignore_index=True)
    n = len(merchants)
    merchants.insert(0, "merchant_id", tokens("mer", rng, n))
    hq = rng.choice(["Ho Chi Minh", "Ha Noi"], n)   # online merchants: head office city
    merchants["city"] = np.where(merchants.is_online, hq, rng.choice(p_units.index.to_numpy(), n, p=p_units.to_numpy()))
    merchants["country"] = "VN"

    top = 1 / np.arange(1, sk["top_n_merchants"] + 1)
    tail = 1 / np.arange(1, n - sk["top_n_merchants"] + 1) ** 0.8
    p = np.concatenate([sk["top_share"] * top / top.sum(), (1 - sk["top_share"]) * tail / tail.sum()])
    return merchants, p


def gen_day(rng, cfg, day, n, first_id, cards, merchants, p_merchant, p_units, backmap) -> pd.DataFrame:
    """Settled transactions of one day plus the duplicates of a re-run nightly export."""
    txn_ts = day + pd.to_timedelta(rng.choice(24, n, p=HOUR_WEIGHTS) * 3600 + rng.integers(0, 3600, n), unit="s")
    m = rng.choice(len(merchants), n, p=p_merchant)
    online, mcc = merchants.is_online.to_numpy()[m], merchants.mcc.to_numpy()[m]
    channel = np.where(online, "ecom", np.where(mcc == 6011, "atm", "pos"))

    # Only cards already issued can pay. ponytail: uniform over issued cards, add per-card activity later.
    eligible = int(np.searchsorted(cards.issue_ts.to_numpy(), (day + pd.Timedelta(days=1)).to_datetime64()))
    c = rng.integers(0, eligible, n)
    # Online: cardholder's registered city. POS/ATM: mostly the home city, sometimes travelling.
    travelling = (channel != "ecom") & (rng.random(n) < 0.15)
    unit = np.where(travelling, rng.choice(p_units.index.to_numpy(), n, p=p_units.to_numpy()), cards._unit.to_numpy()[c])

    amount = np.where(channel == "atm", rng.choice([500, 1000, 2000, 3000, 5000], n) * 1000,
                      np.maximum(10_000, np.round(rng.lognormal(np.log(np.where(online, 250_000, 150_000)), 1.0) / 1000) * 1000))
    df = pd.DataFrame({
        "transaction_id": [f"txn_{i:012x}" for i in range(first_id, first_id + n)],
        "card_id": cards.card_id.to_numpy()[c],
        "merchant_id": merchants.merchant_id.to_numpy()[m],
        "txn_ts": txn_ts,
        "txn_city": to_source_names(unit, pd.Series(txn_ts), rng, cfg, backmap),
        "amount_vnd": amount.astype(np.int64),
        "currency": "VND",
        "channel": channel,
        "status": rng.choice(["settled", "reversed"], n, p=[0.985, 0.015]),
        # nightly settlement batch exports yesterday's transactions between 01:00 and 03:00
        "created_ts": day + pd.Timedelta(days=1, hours=1) + pd.to_timedelta(rng.integers(0, 7200, n), unit="s"),
    })
    # Problem: export job re-run after a timeout -> same transaction_id, later created_ts.
    dup = df.iloc[rng.choice(n, round(n * cfg["duplicates"]["offline_rate"]), replace=False)].copy()
    dup["created_ts"] += pd.to_timedelta(rng.integers(30 * 60, 180 * 60, len(dup)), unit="s")
    return pd.concat([df, dup], ignore_index=True)


def write(df: pd.DataFrame, path: Path) -> None:
    """Write Parquet with microsecond timestamps (Spark cannot read nanosecond Parquet timestamps)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df.drop(columns=[c for c in df.columns if c.startswith("_")]), preserve_index=False)
    pq.write_table(table, path, coerce_timestamps="us", allow_truncated_timestamps=True)


def main(config_path: Path) -> None:
    """Generate every table described in the config and write it under `output_dir`."""
    t0 = time.perf_counter()
    cfg = yaml.safe_load(config_path.read_text())
    out = ROOT / cfg["output_dir"]
    rng = np.random.default_rng(cfg["seed"])

    mapping = pd.read_csv(MAPPING_CSV)
    p_units, backmap = unit_probs(cfg, mapping, rng), back_map_table(cfg, mapping)
    customers = gen_customers(rng, cfg, p_units, backmap)
    accounts, cards = gen_accounts_cards(rng, customers)
    merchants, p_merchant = gen_merchants(rng, cfg, p_units)

    cb = out / "core_banking"
    write(mapping.assign(effective_from=pd.Timestamp(cfg["reorganization"]["cutoff"]).date()), cb / "ref_province_mapping.parquet")
    for name, df in {"customers": customers, "accounts": accounts, "cards": cards, "merchants": merchants}.items():
        write(df, cb / f"{name}.parquet")

    days = pd.date_range(cfg["period"]["start"], cfg["period"]["end"], freq="D")
    per_day = rng.poisson(cfg["volume"]["n_transactions"] / len(days), len(days))
    next_id, rows = 0, 0
    for day, n in zip(days, per_day):
        df = gen_day(rng, cfg, day, int(n), next_id, cards, merchants, p_merchant, p_units, backmap)
        export_date = (day + pd.Timedelta(days=1)).date()
        write(df, out / "core-banking-export" / "transactions" / f"export_date={export_date}" / "part-0.parquet")
        next_id, rows = next_id + n, rows + len(df)

    size_mb = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 2**20
    print(f"wrote {rows:,} transaction rows ({next_id:,} unique) + {len(customers):,} customers, "
          f"{len(cards):,} cards, {len(merchants):,} merchants -> {out} ({size_mb:.1f} MB, {time.perf_counter() - t0:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    main(ap.parse_args().config)
