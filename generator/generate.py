"""Generate synthetic core-banking source data for the card fraud platform.

Outputs stand in for another department's systems (loaded into Postgres / SeaweedFS later):
  <output_dir>/core_banking/{customers,accounts,cards,merchants,ref_province_mapping}.parquet
  <output_dir>/core-banking-export/transactions/export_date=YYYY-MM-DD/part-0.parquet
  <output_dir>/stream/card_authorizations/auth_date=YYYY-MM-DD/part-0.parquet   (replayed by producer.py)
  <output_dir>/stream/app_events/event_date=YYYY-MM-DD/part-0.parquet            (replayed by producer.py)
  <output_dir>/_truth/ato_sessions.parquet   generator ground truth for the checks, not a source system

Problems injected in this version: merchant skew, txn_city skew, administrative
reorganization (63 old names -> 34 units), offline duplicates, schema evolution, SCD2,
label delay, high cardinality; streaming late arrivals, duplicates and out-of-order events.
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

from app_events import arrival, gen_app_day

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
    """One onboarding row per customer. `_unit` is the true new-space unit, kept only for generating."""
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


def customer_versions(rng, cfg, base: pd.DataFrame, p_units: pd.Series, backmap: dict) -> pd.DataFrame:
    """SCD2 source: one row per customer version.

    After onboarding, customers move to another unit or get a new risk_tier. On the reorganization
    date the core system relabels old province names to the new unit name: only the label changes,
    so a naive SCD2 comparison sees a fake move (the problem). A stale share is never relabeled.
    """
    sc, reorg = cfg["scd2"], cfg["reorganization"]
    cutoff, end = pd.Timestamp(reorg["cutoff"]), pd.Timestamp(cfg["period"]["end"])
    units, probs = p_units.index.to_numpy(), p_units.to_numpy()
    span = (end - base.updated_ts).to_numpy()
    parts = [base.assign(_kind="onboard")]
    for kind, rate in (("move", sc["move_rate_per_year"]), ("rescore", sc["rescore_rate_per_year"])):
        idx = np.repeat(np.arange(len(base)), rng.poisson(rate * span / np.timedelta64(365, "D")))
        parts.append(pd.DataFrame({"customer_id": base.customer_id.to_numpy()[idx], "_kind": kind,
                                   "updated_ts": base.updated_ts.to_numpy()[idx] + span[idx] * rng.random(idx.size)}))
    v = pd.concat(parts, ignore_index=True).sort_values(["customer_id", "updated_ts"], ignore_index=True)
    cols = [c for c in base.columns if c not in ("customer_id", "updated_ts")]

    move = v._kind.eq("move")
    v.loc[move, "_unit"] = rng.choice(units, move.sum(), p=probs)
    # Destinations follow the unit shares, so the txn_city skew stays stable over time; a draw that
    # lands in the current unit is no move and is dropped.
    # ponytail: effective move rate = move_rate × (1 − share of the current unit), ~3%/yr.
    prev = v.groupby("customer_id")._unit.ffill().groupby(v.customer_id).shift()
    v = v[~(move & v._unit.eq(prev))].reset_index(drop=True)
    move, rescore = v._kind.eq("move"), v._kind.eq("rescore")
    v.loc[rescore, "risk_tier"] = rng.choice(["low", "medium", "high"], rescore.sum(), p=[0.7, 0.25, 0.05])
    v.loc[move, "city"] = to_source_names(v.loc[move, "_unit"].to_numpy(), v.loc[move, "updated_ts"], rng, cfg, backmap)
    v[cols] = v.groupby("customer_id")[cols].ffill()

    # Relabel batch on the cutoff night, for customers whose current city is an old-only name.
    state = v[v.updated_ts < cutoff].groupby("customer_id").tail(1)
    old_only = ~state.city.isin(backmap.keys()) & (rng.random(len(state)) >= reorg["stale_share_after_cutoff"])
    rel = state[old_only].assign(_kind="relabel", city=lambda d: d._unit,
                                 updated_ts=cutoff + pd.to_timedelta(rng.integers(0, 7200, old_only.sum()), unit="s"))
    later = (v.updated_ts >= cutoff) & ~move & v.customer_id.isin(rel.customer_id)
    v.loc[later, "city"] = None   # re-filled from the relabel row or a later move
    v = pd.concat([v, rel], ignore_index=True).sort_values(["customer_id", "updated_ts"], ignore_index=True)
    v["city"] = v.groupby("customer_id").city.ffill()
    return v[base.columns.tolist() + ["_kind"]].astype({"kyc_level": "int64"})


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
        "credit_limit": pd.Series(np.where(is_credit, rng.choice([20, 50, 100, 200], n) * 1_000_000, 0), dtype="Int64").where(is_credit),
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
        "_customer_id": customers.customer_id.to_numpy()[acc_idx],
        "_cust": acc_idx,   # customer position (one account per customer)
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
    # Physical terminals per merchant (high cardinality on terminal_id). Online merchants have none.
    t = cfg["terminals"]
    merchants["_n_terminals"] = np.select(
        [merchants.is_online, merchants.mcc == 6011, np.arange(n) < sk["top_n_merchants"]],
        [0, t["atm_network"], t["chain"]], rng.integers(1, t["other_pos_max"] + 1, n))

    top = 1 / np.arange(1, sk["top_n_merchants"] + 1)
    tail = 1 / np.arange(1, n - sk["top_n_merchants"] + 1) ** 0.8
    p = np.concatenate([sk["top_share"] * top / top.sum(), (1 - sk["top_share"]) * tail / tail.sum()])
    return merchants, p


def day_merchant_probs(cfg, p: np.ndarray, merchants: pd.DataFrame, day: pd.Timestamp) -> np.ndarray:
    """Trend: ATM use declines every year (cash withdrawals give way to QR and app payments).
    The ATM share lost so far moves to the other non-top merchants, so the top-N merchant skew is unchanged."""
    years = (day - pd.Timestamp(cfg["period"]["start"])).days / 365.25
    keep = (1 - cfg["trends"]["atm_decline_per_year"]) ** years
    atm = (merchants.mcc == 6011).to_numpy()
    rest = ~atm & (np.arange(len(p)) >= cfg["skew"]["merchant"]["top_n_merchants"])
    q = p.copy()
    q[rest] += p[atm].sum() * (1 - keep) * p[rest] / p[rest].sum()
    q[atm] *= keep
    return q


def pick(rng: np.random.Generator, shares: dict[str, float], n: int) -> np.ndarray:
    """Draw n values from a {value: share} mapping."""
    return rng.choice(list(shares), n, p=list(shares.values()))


def add_auth_columns(df: pd.DataFrame, rng: np.random.Generator, cfg: dict) -> None:
    """Add `auth_method` and `three_ds_version` in place.

    From 2024-07-01 (Decision 2345/QĐ-NHNN and Circular 18/2024/TT-NHNN take effect) the card
    system records how each payment was authenticated. Only online payments go through 3-D Secure.
    """
    se = cfg["schema_evolution"]
    channel = df.channel.to_numpy()
    method = np.empty(len(df), dtype=object)
    for ch, shares in se["auth_method"].items():
        idx = np.flatnonzero(channel == ch)
        method[idx] = pick(rng, shares, idx.size)
    df["auth_method"] = pd.Series(method, dtype="string")
    df["three_ds_version"] = pd.Series(pick(rng, se["three_ds_version"], len(df)), dtype="string").where(channel == "ecom")


def fraud_flags(rng, cfg, df: pd.DataFrame) -> np.ndarray:
    """Mark fraud with a light, learnable signal: online, night, large amount and OTP payments are
    riskier. Weights are relative and rescaled so the day's average rate is `fraud_rate`.
    ponytail: independent multiplicative weights; richer fraud patterns (ATO, testing cards) come with Layer 2."""
    w_cfg, lab = cfg["labels"]["risk_weights"], cfg["labels"]
    w = (np.where(df.channel == "ecom", w_cfg["ecom"], 1.0)
         * np.where(df.txn_ts.dt.hour < 6, w_cfg["night"], 1.0)
         * np.where(df.amount_vnd > 5_000_000, w_cfg["high_amount"], 1.0))
    if "auth_method" in df:
        w = w * np.where(df.auth_method == "3ds_otp", w_cfg["otp"], 1.0)
    return rng.random(len(df)) < lab["fraud_rate"] * w / w.mean()


def gen_day(rng, cfg, day, n, first_id, cards, homes, merchants, p_merchant, p_units, backmap) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Settled transactions of one day plus the duplicates of a re-run nightly export.
    Also returns the day's disputed transactions (fraud and non-fraud) for the disputes table."""
    txn_ts = day + pd.to_timedelta(rng.choice(24, n, p=HOUR_WEIGHTS) * 3600 + rng.integers(0, 3600, n), unit="s")
    m = rng.choice(len(merchants), n, p=p_merchant)
    online, mcc = merchants.is_online.to_numpy()[m], merchants.mcc.to_numpy()[m]
    channel = np.where(online, "ecom", np.where(mcc == 6011, "atm", "pos"))

    # Only cards issued before the day starts can pay, so txn_ts > issue_ts >= onboarding time.
    # ponytail: uniform over issued cards, add per-card activity later.
    eligible = int(np.searchsorted(cards.issue_ts.to_numpy(), day.to_datetime64()))
    c = rng.integers(0, eligible, n)
    # Home unit as of the payment time, so a customer who moved pays mostly in the new unit.
    pay = pd.DataFrame({"txn_ts": txn_ts, "customer_id": cards._customer_id.to_numpy()[c], "i": np.arange(n)}).sort_values("txn_ts")
    home = pd.merge_asof(pay, homes, left_on="txn_ts", right_on="updated_ts", by="customer_id").sort_values("i")._unit.to_numpy()
    assert not pd.isna(home).any(), "every payment must find a customer version"
    # Online: cardholder's registered city. POS/ATM: mostly the home city, sometimes travelling.
    travelling = (channel != "ecom") & (rng.random(n) < 0.15)
    unit = np.where(travelling, rng.choice(p_units.index.to_numpy(), n, p=p_units.to_numpy()), home)

    amount = np.where(channel == "atm", rng.choice([500, 1000, 2000, 3000, 5000], n) * 1000,
                      np.maximum(10_000, np.round(rng.lognormal(np.log(np.where(online, 250_000, 150_000)), 1.0) / 1000) * 1000))
    n_term = merchants._n_terminals.to_numpy()[m]
    term = rng.integers(0, np.maximum(n_term, 1))
    df = pd.DataFrame({
        "transaction_id": [f"txn_{i:012x}" for i in range(first_id, first_id + n)],
        "card_id": cards.card_id.to_numpy()[c],
        "merchant_id": merchants.merchant_id.to_numpy()[m],
        "terminal_id": pd.Series([f"term_{a:05x}{b:04x}" for a, b in zip(m, term)], dtype="string").where(n_term > 0),
        "txn_ts": txn_ts,
        "txn_city": to_source_names(unit, pd.Series(txn_ts), rng, cfg, backmap),
        "amount_vnd": amount.astype(np.int64),
        "currency": "VND",
        "channel": channel,
        "status": rng.choice(["settled", "reversed"], n, p=[0.985, 0.015]),
        # nightly settlement batch exports yesterday's transactions between 01:00 and 03:00
        "created_ts": day + pd.Timedelta(days=1, hours=1) + pd.to_timedelta(rng.integers(0, 7200, n), unit="s"),
    })
    # Problem: schema evolution. The columns only exist from the regulation date, so older files lack them entirely.
    if day >= pd.Timestamp(cfg["schema_evolution"]["cutoff"]):
        add_auth_columns(df, rng, cfg)
    fraud = fraud_flags(rng, cfg, df)
    disputed = fraud | (rng.random(n) < cfg["labels"]["non_fraud_dispute_rate"])
    disputes = df.loc[disputed, ["transaction_id", "txn_ts", "channel"]].assign(is_confirmed_fraud=fraud[disputed])
    # Problem: export job re-run after a timeout -> same transaction_id, later created_ts.
    dup = df.iloc[rng.choice(n, round(n * cfg["duplicates"]["offline_rate"]), replace=False)].copy()
    dup["created_ts"] += pd.to_timedelta(rng.integers(30 * 60, 180 * 60, len(dup)), unit="s")
    return pd.concat([df, dup], ignore_index=True), disputes


def gen_disputes(rng, cfg, disputed: pd.DataFrame) -> pd.DataFrame:
    """Problem: label delay. A dispute is reported 1-60 days after the payment (truncated log-normal).
    Disputes reported after the end of the period do not exist yet, so recent months look clean."""
    ld = cfg["labels"]["dispute_delay_days"]
    mu, sigma = np.log(ld["median"]), (np.log(ld["p95"]) - np.log(ld["median"])) / 1.645
    delay = rng.lognormal(mu, sigma, len(disputed))
    while (bad := (delay < ld["min"]) | (delay > ld["max"])).any():   # redraw = truncate
        delay[bad] = rng.lognormal(mu, sigma, bad.sum())
    reported = disputed.txn_ts + pd.to_timedelta(delay * 86400, unit="s").round("s")
    fraud = disputed.is_confirmed_fraud.to_numpy()
    reason = np.where(fraud, np.where(disputed.channel == "ecom", "fraud_card_not_present", "fraud_card_present"),
                      rng.choice(["not_received", "duplicate_charge", "amount_differs"], len(disputed)))
    out = pd.DataFrame({"dispute_id": tokens("dsp", rng, len(disputed)), "transaction_id": disputed.transaction_id.to_numpy(),
                        "reported_ts": reported.to_numpy(), "reason_code": reason, "is_confirmed_fraud": fraud})
    end = pd.Timestamp(cfg["period"]["end"]) + pd.Timedelta(days=1)
    return out[out.reported_ts < end].sort_values("reported_ts", ignore_index=True)


def gen_devices(rng, cfg, n_customers: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Phones each customer pays from online: 1 + Poisson(extra). Returns the device ids and,
    per customer position, the first index and count of that customer's block of ids."""
    count = 1 + rng.poisson(cfg["devices"]["extra_per_customer"], n_customers)
    return tokens("dev", rng, int(count.sum())), np.cumsum(count) - count, count


def gen_auths(rng, cfg, txns: pd.DataFrame, fraud: np.ndarray, cust: np.ndarray, devices) -> pd.DataFrame:
    """Streaming source: the card network's authorization request for each payment, before settlement.

    auth_id shares the counter of transaction_id (the settlement record references its authorization),
    and geo_city is the same location string as txn_city. Problems: late arrivals from offline POS
    terminals (store-and-forward) and duplicates from network retries (same auth_id, later received_ts).
    """
    st, n = cfg["card_authorizations"], len(txns)
    channel = txns.channel.to_numpy()
    ids, first, count = devices
    dev = ids[first[cust] + (rng.random(n) * count[cust]).astype(int)]
    # Fraudsters pay online from their own phone, so the card sees a device it never used (a realtime signal).
    new = fraud & (channel == "ecom")
    dev[new] = tokens("dev", rng, int(new.sum()))
    delay = rng.uniform(*st["network_delay_s"], n)
    late = (channel == "pos") & (rng.random(n) < st["late_share_of_pos"])
    delay[late] = rng.uniform(*st["late_delay_min"], late.sum()) * 60
    df = pd.DataFrame({
        "auth_id": txns.transaction_id.str.replace("txn_", "auth_", regex=False).to_numpy(),
        "card_id": txns.card_id.to_numpy(),
        "merchant_id": txns.merchant_id.to_numpy(),
        "amount_vnd": txns.amount_vnd.to_numpy(),
        "channel": channel,
        "terminal_id": txns.terminal_id.to_numpy(),
        "device_id": pd.Series(dev, dtype="string").where(channel == "ecom"),
        "geo_city": txns.txn_city.to_numpy(),
        "auth_ts": txns.txn_ts.to_numpy(),
        "received_ts": txns.txn_ts.to_numpy() + pd.to_timedelta(np.round(delay * 1000), unit="ms"),
    })
    dup = df.iloc[rng.choice(n, round(n * st["duplicate_rate"]), replace=False)].copy()
    dup["received_ts"] += pd.to_timedelta(np.round(rng.uniform(*st["retry_delay_s"], len(dup)) * 1000), unit="ms")
    return pd.concat([df, dup], ignore_index=True).sort_values("received_ts", ignore_index=True)


def write_app_events(cfg, out: Path, customers: pd.DataFrame, devices, days: pd.DatetimeIndex) -> tuple[int, int]:
    """Sessions of the mobile banking app, day by day, with their own random stream (so the card data
    stays unchanged). State such as each customer's last device carries over between days."""
    rng = np.random.default_rng([cfg["seed"], 2])
    a = cfg["app_events"]
    onboard = customers.updated_ts.to_numpy()
    order = np.argsort(onboard, kind="stable")
    shop_w = 1 / np.arange(1, a["shops"] + 1) ** 0.9
    ctx = {"customer_ids": customers.customer_id.to_numpy(), "onboard_order": order, "onboard_sorted": onboard[order],
           "dev_ids": devices[0], "dev_first": devices[1], "dev_count": devices[2], "hour_p": HOUR_WEIGHTS,
           "shops": tokens("pay", rng, a["shops"]), "shop_p": shop_w / shop_w.sum(),
           "attackers": tokens("dev", rng, a["ato"]["attacker_devices"]), "mules": tokens("pay", rng, a["ato"]["mule_payees"]),
           "last_device": {}, "payees": {}}
    per_day = rng.poisson(a["n_sessions"] / len(days), len(days))
    next_session, next_event, truth = 0, 0, []
    for day, n in zip(days, per_day):
        df, ato = gen_app_day(rng, cfg, day, int(n), next_session, ctx)
        received = arrival(rng, cfg, df)
        to_ts = lambda s: pd.to_datetime(np.round(s * 1000).astype(np.int64), unit="ms")
        df.insert(0, "event_id", [f"evt_{i:012x}" for i in range(next_event, next_event + len(df))])
        df["event_ts"], df["received_ts"] = to_ts(df.event_ts.to_numpy()), to_ts(received)
        write(df.sort_values("received_ts", ignore_index=True), out / "stream" / "app_events" / f"event_date={day.date()}" / "part-0.parquet")
        next_session, next_event = next_session + n, next_event + len(df)
        truth += ato
    write(pd.DataFrame(truth, columns=["session_id", "kind"]), out / "_truth" / "ato_sessions.parquet")
    return next_event, len(truth)


def write(df: pd.DataFrame, path: Path) -> None:
    """Write Parquet with microsecond timestamps (Spark cannot read nanosecond Parquet timestamps)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df.drop(columns=[c for c in df.columns if c.startswith("_")]), preserve_index=False)
    pq.write_table(table, path, coerce_timestamps="us", allow_truncated_timestamps=True)


def main(config_path: Path) -> None:
    """Generate every table described in the config and write it under `output_dir`."""
    t0 = time.perf_counter()
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    out = ROOT / cfg["output_dir"]
    rng = np.random.default_rng(cfg["seed"])

    mapping = pd.read_csv(MAPPING_CSV)
    p_units, backmap = unit_probs(cfg, mapping, rng), back_map_table(cfg, mapping)
    customers = gen_customers(rng, cfg, p_units, backmap)
    versions = customer_versions(rng, cfg, customers, p_units, backmap)
    homes = versions[["customer_id", "updated_ts", "_unit"]].sort_values("updated_ts")
    accounts, cards = gen_accounts_cards(rng, customers)
    merchants, p_merchant = gen_merchants(rng, cfg, p_units)
    # Separate random stream, so changing the streaming data leaves the offline data unchanged.
    srng = np.random.default_rng([cfg["seed"], 1])
    devices = gen_devices(srng, cfg, len(customers))
    card_cust = cards.set_index("card_id")._cust

    days = pd.date_range(cfg["period"]["start"], cfg["period"]["end"], freq="D")
    per_day = rng.poisson(cfg["volume"]["n_transactions"] / len(days), len(days))
    next_id, rows, n_auths, disputed = 0, 0, 0, []
    for day, n in zip(days, per_day):
        p_day = day_merchant_probs(cfg, p_merchant, merchants, day)
        df, dsp = gen_day(rng, cfg, day, int(n), next_id, cards, homes, merchants, p_day, p_units, backmap)
        export_date = (day + pd.Timedelta(days=1)).date()
        write(df, out / "core-banking-export" / "transactions" / f"export_date={export_date}" / "part-0.parquet")
        txns = df.iloc[:n]   # gen_day puts the originals before the export duplicates
        fraud = txns.transaction_id.isin(dsp.transaction_id[dsp.is_confirmed_fraud]).to_numpy()
        auths = gen_auths(srng, cfg, txns, fraud, card_cust.reindex(txns.card_id).to_numpy(), devices)
        write(auths, out / "stream" / "card_authorizations" / f"auth_date={day.date()}" / "part-0.parquet")
        next_id, rows, n_auths = next_id + n, rows + len(df), n_auths + len(auths)
        disputed.append(dsp)
    disputes = gen_disputes(rng, cfg, pd.concat(disputed, ignore_index=True))
    n_events, n_ato = write_app_events(cfg, out, customers, devices, days)

    cb = out / "core_banking"
    write(mapping.assign(effective_from=pd.Timestamp(cfg["reorganization"]["cutoff"]).date()), cb / "ref_province_mapping.parquet")
    for name, df in {"customers": versions, "accounts": accounts, "cards": cards, "merchants": merchants, "disputes": disputes}.items():
        write(df, cb / f"{name}.parquet")

    size_mb = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 2**20
    print(f"wrote {rows:,} transaction rows ({next_id:,} unique), {len(customers):,} customers ({len(versions):,} versions), "
          f"{len(cards):,} cards, {len(merchants):,} merchants, {len(disputes):,} disputes, {n_auths:,} authorizations, "
          f"{n_events:,} app events ({n_ato:,} ATO sessions) -> {out} "
          f"({size_mb:.1f} MB, {time.perf_counter() - t0:.0f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    main(ap.parse_args().config)
