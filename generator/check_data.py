"""Verify that the generated source data really contains the injected problems.

Every check is an assert with a tolerance. The printed metrics feed docs/data_characteristics.md.
Usage: python generator/check_data.py [--config generator/config.yaml]
"""
import argparse
from math import erf, log, sqrt
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parent.parent
MAPPING_CSV = Path(__file__).parent / "ref" / "province_mapping.csv"
failures: list[str] = []


def check(name: str, value: float, lo: float, hi: float) -> None:
    """Print one metric and record a failure when it falls outside [lo, hi]."""
    ok = lo <= value <= hi
    print(f"{'PASS' if ok else 'FAIL'}  {name:<48} {value:>10.4f}   expected [{lo:.4g}, {hi:.4g}]")
    if not ok:
        failures.append(name)


def read_merged(export_dir: Path, schemas: list[pa.Schema]) -> pd.DataFrame:
    """Read every export file with the union of their schemas; missing columns become null."""
    part = pa.schema([("export_date", pa.string())])
    merged = pa.unify_schemas(schemas).append(part.field(0))
    dataset = ds.dataset(export_dir, schema=merged, partitioning=ds.partitioning(part, flavor="hive"))
    return dataset.to_table().to_pandas()


def check_schema_evolution(cfg: dict, files: list[Path], schemas: list[pa.Schema], dedup: pd.DataFrame) -> None:
    """Problem: schema evolution. Auth columns exist only in files of transactions from the cutoff,
    and their values match the channel of each payment."""
    se = cfg["schema_evolution"]
    cutoff = pd.Timestamp(se["cutoff"])
    new_cols = {"auth_method", "three_ds_version"}
    # export_date = transaction date + 1, so files exported up to the cutoff date hold only older transactions
    new_file = [pd.Timestamp(f.parent.name.split("=")[1]) > cutoff for f in files]
    has_cols = [new_cols <= set(s.names) for s in schemas]
    check("files before cutoff without auth columns (share)", sum(not h for h, n in zip(has_cols, new_file) if not n) / max(1, new_file.count(False)), 1, 1)
    check("files after cutoff with auth columns (share)", sum(h for h, n in zip(has_cols, new_file) if n) / max(1, new_file.count(True)), 1, 1)

    before, after = dedup[dedup.txn_ts < cutoff], dedup[dedup.txn_ts >= cutoff]
    check("auth_method null before cutoff (share)", before.auth_method.isna().mean(), 1, 1)
    allowed = after.channel.map({ch: set(shares) for ch, shares in se["auth_method"].items()})
    check("auth_method valid for its channel (share)", pd.Series([m in a for m, a in zip(after.auth_method, allowed)]).mean(), 1, 1)
    ecom = after.channel == "ecom"
    app = se["auth_method"]["ecom"]["3ds_app"]
    check("3ds_app among ecom (share)", (after.auth_method[ecom] == "3ds_app").mean(), app - 0.02, app + 0.02)
    v22 = se["three_ds_version"]["2.2.0"]
    check("three_ds_version 2.2.0 among ecom (share)", (after.three_ds_version[ecom] == "2.2.0").mean(), v22 - 0.02, v22 + 0.02)
    check("three_ds_version null for pos/atm (share)", after.three_ds_version[~ecom].isna().mean(), 1, 1)


def check_scd2(cfg: dict, customers: pd.DataFrame, to_unit: dict, new_units: set) -> None:
    """SCD2 source + reorganization: real changes (move, re-score) vs spurious relabel versions,
    where only the province label changed and the normalized unit is the same."""
    cutoff = pd.Timestamp(cfg["reorganization"]["cutoff"])
    v = customers.sort_values(["customer_id", "updated_ts"])
    prev = v.groupby("customer_id")[["city", "risk_tier"]].shift()
    has_prev = prev.city.notna()
    raw_change = has_prev & ((v.city != prev.city) | (v.risk_tier != prev.risk_tier))
    real_change = has_prev & ((v.city.map(to_unit) != prev.city.map(to_unit)) | (v.risk_tier != prev.risk_tier))
    spurious = raw_change & ~real_change
    check("customers with more than one version (share)", (v.groupby("customer_id").size() > 1).mean(), 0.2, 1)
    check("spurious versions on the cutoff date (share)", (v.updated_ts[spurious].dt.normalize() == cutoff).mean(), 1, 1)
    state = v[v.updated_ts < cutoff].groupby("customer_id").tail(1)
    old_names = (~state.city.isin(new_units)).sum()
    stale = cfg["reorganization"]["stale_share_after_cutoff"]
    check("old-name customers relabeled at cutoff (share)", spurious.sum() / max(1, old_names), 1 - stale - 0.03, 1 - stale + 0.03)
    print(f"      spurious relabel versions: {spurious.sum():,} of {len(v):,} rows")


def check_labels(cfg: dict, dedup: pd.DataFrame, disputes: pd.DataFrame) -> None:
    """Disputes + label delay: delay distribution, fraud rate on mature months, and the
    artificial drop in the most recent month."""
    lab, ld = cfg["labels"], cfg["labels"]["dispute_delay_days"]
    end = pd.Timestamp(cfg["period"]["end"]) + pd.Timedelta(days=1)
    d = disputes.merge(dedup[["transaction_id", "txn_ts", "channel"]], on="transaction_id", how="left")
    check("dispute.transaction_id found in transactions (share)", d.txn_ts.notna().mean(), 1, 1)
    delay = (d.reported_ts - d.txn_ts).dt.total_seconds() / 86400
    check("dispute delay median (days)", delay.median(), ld["median"] - 2, ld["median"] + 2)
    check("dispute delay within [min, max] (share)", delay.between(ld["min"], ld["max"]).mean(), 1, 1)
    check("disputes reported before the snapshot (share)", (d.reported_ts < end).mean(), 1, 1)

    fraud_ids = set(d.transaction_id[d.is_confirmed_fraud])
    is_fraud = dedup.transaction_id.isin(fraud_ids)
    mature = dedup.txn_ts < end - pd.Timedelta(days=ld["max"])
    rate = is_fraud[mature].mean()
    check("fraud rate on mature transactions", rate, lab["fraud_rate"] * 0.8, lab["fraud_rate"] * 1.2)
    # Expected visible share = P(delay < age) under the truncated log-normal; tolerance = 3x Poisson noise
    # on the recent fraud count, so the band is wide on the smoke test and tight at 1 GB.
    recent = dedup.txn_ts >= end - pd.Timedelta(days=30)
    mu, sigma = log(ld["median"]), (log(ld["p95"]) - log(ld["median"])) / 1.645
    cdf = lambda x: 0.5 * (1 + erf((log(max(x, 1e-9)) - mu) / (sigma * sqrt(2))))
    trunc = lambda x: min(max((cdf(x) - cdf(ld["min"])) / (cdf(ld["max"]) - cdf(ld["min"])), 0), 1)
    expected = ((end - dedup.txn_ts[recent]).dt.total_seconds() / 86400).map(trunc).mean()
    tol = 3 / sqrt(rate * recent.sum() * expected)
    check("last-30-day fraud rate / mature rate (label delay)", is_fraud[recent].mean() / rate, max(0, expected * (1 - tol)), expected * (1 + tol))
    print(f"      expected {expected:.2f} from the delay distribution, {recent.sum():,} recent transactions")
    ecom = dedup.channel == "ecom"
    check("fraud rate ecom / non-ecom (learnable signal)", is_fraud[mature & ecom].mean() / is_fraud[mature & ~ecom].mean(), 1.5, float("inf"))


def check_moves(dedup: pd.DataFrame, cards: pd.DataFrame, accounts: pd.DataFrame, customers: pd.DataFrame, to_unit: dict) -> None:
    """After a real move, a customer pays mostly in the home unit of the version valid at payment time."""
    v = customers.sort_values(["customer_id", "updated_ts"]).assign(home=lambda d: d.city.map(to_unit))
    moved_at = v[v.home.ne(v.groupby("customer_id").home.shift()) & v.customer_id.duplicated()].groupby("customer_id").updated_ts.min()
    t = dedup.merge(cards.merge(accounts, on="account_id")[["card_id", "customer_id"]], on="card_id")
    t = t[t.txn_ts > t.customer_id.map(moved_at)]
    asof = pd.merge_asof(t.sort_values("txn_ts"), v[["customer_id", "updated_ts", "home"]].sort_values("updated_ts"),
                         left_on="txn_ts", right_on="updated_ts", by="customer_id")
    check("moved customers pay in the as-of home unit (share)", (asof.txn_city.map(to_unit) == asof.home).mean(), 0.8, 1)
    print(f"      transactions after a move: {len(t):,}")


def check_terminals(dedup: pd.DataFrame) -> None:
    """High cardinality: terminal_id on POS / ATM payments, null for online payments."""
    physical = dedup.channel != "ecom"
    check("terminal_id present for pos/atm (share)", dedup.terminal_id[physical].notna().mean(), 1, 1)
    check("terminal_id null for ecom (share)", dedup.terminal_id[~physical].isna().mean(), 1, 1)
    check("distinct terminal_id", dedup.terminal_id.nunique(), 1000, float("inf"))


def check_stream(cfg: dict, src: Path, dedup: pd.DataFrame, disputes: pd.DataFrame, to_unit: dict) -> None:
    """card_authorizations: one event per payment, late events from offline POS, network-retry duplicates."""
    st = cfg["card_authorizations"]
    a = pd.read_parquet(src / "stream" / "card_authorizations").drop(columns="auth_date")
    print(f"\nstream: {len(a):,} card_authorizations, {a.auth_id.nunique():,} unique")
    rate = st["duplicate_rate"]
    check("stream duplicate rate (extra rows / unique)", len(a) / a.auth_id.nunique() - 1, rate - 0.003, rate + 0.003)
    check("stream duplicates differ only in received_ts (share)", len(a.drop(columns="received_ts").drop_duplicates()) / a.auth_id.nunique(), 1, 1)
    gap = a[a.auth_id.duplicated(keep=False)].groupby("auth_id").received_ts.agg(lambda s: (s.max() - s.min()).total_seconds())
    check("retry gap within retry_delay_s (share)", gap.between(*st["retry_delay_s"]).mean(), 1, 1)

    u = a.sort_values("received_ts").drop_duplicates("auth_id")   # first arrival
    t = dedup.assign(auth_id=dedup.transaction_id.str.replace("txn_", "auth_", regex=False)).merge(u, on="auth_id", how="left", suffixes=("", "_a"))
    same = ((t.card_id == t.card_id_a) & (t.amount_vnd == t.amount_vnd_a) & (t.txn_city == t.geo_city) & (t.txn_ts == t.auth_ts))
    check("unique auths / unique transactions", len(u) / len(dedup), 1, 1)
    check("auth matches its transaction (share)", same.mean(), 1, 1)
    check("share of Ho Chi Minh in geo_city (normalized)", (u.geo_city.map(to_unit) == "Ho Chi Minh").mean(), 0.58, 0.62)

    # Problem: late arrivals. Offline POS terminals send minutes later; everything else within network delay.
    delay = (u.received_ts - u.auth_ts).dt.total_seconds()
    late = delay >= 60
    pos = u.channel == "pos"
    share = st["late_share_of_pos"]
    check("late events are POS (share)", pos[late].mean(), 1, 1)
    check("late share of POS", late[pos].mean(), share - 0.015, share + 0.015)
    check("late delay within late_delay_min (share)", (delay[late] / 60).between(*st["late_delay_min"]).mean(), 1, 1)
    check("on-time delay max (s)", delay[~late].max(), 0, st["network_delay_s"][1])
    print(f"      late events: {late.sum():,} = {late.mean():.2%} of all authorizations")

    # High cardinality + realtime signal: device_id on online payments; fraud comes from a device new to the card.
    ecom = u.channel == "ecom"
    check("device_id present only for ecom (share)", (u.device_id.notna() == ecom).mean(), 1, 1)
    check("distinct device_id", u.device_id.nunique(), 1000, float("inf"))
    e = u[ecom].sort_values("auth_ts")
    first_use = ~e.duplicated(["card_id", "device_id"])
    fraud = e.auth_id.str.replace("auth_", "txn_", regex=False).isin(disputes.transaction_id[disputes.is_confirmed_fraud])
    check("fraud ecom on a device new to the card (share)", first_use[fraud].mean(), 1, 1)
    print(f"      legit ecom on a device new to the card: {first_use[~fraud].mean():.2%}")


def check_app_events(cfg: dict, src: Path) -> None:
    """app_events: out-of-order arrival, the Decision 2345 biometric rule, ATO patterns and the QR trend."""
    a, bio = cfg["app_events"], cfg["app_events"]["biometric"]
    cutoff = pd.Timestamp(bio["cutoff"])
    e = pq.read_table(src / "stream" / "app_events", partitioning=None).to_pandas().sort_values(["session_id", "event_ts"], ignore_index=True)
    kind = e.session_id.map(pd.read_parquet(src / "_truth" / "ato_sessions.parquet").set_index("session_id").kind).fillna("legit")
    print(f"\napp_events: {len(e):,} events, {e.session_id.nunique():,} sessions, {kind[~e.session_id.duplicated()].value_counts().to_dict()}")

    # Problem: out-of-order. An event is out of order when a later event of its session arrived before it.
    rev = e.iloc[::-1]
    later_min = rev.groupby("session_id").received_ts.cummin().groupby(rev.session_id).shift()
    ooo = e.received_ts > later_min.reindex(e.index)
    delay = (e.received_ts - e.event_ts).dt.total_seconds()
    share = a["out_of_order_share"]
    check("out-of-order events (share)", ooo.mean(), share - 0.01, share + 0.01)
    check("max delay of out-of-order events (s)", delay[ooo].max(), 0, a["max_disorder_s"])
    check("max delay of in-order events (s)", delay[~ooo].max(), 0, 1)

    # Decision 2345 identity check (domain rule, from the cutoff).
    nxt, prev = e.groupby("session_id").shift(-1), e.groupby("session_id").shift()
    post, is_bio = e.event_ts >= cutoff, e.event_type == "biometric_verify"
    check("no biometric_verify before cutoff (share)", 1 - is_bio[~post].mean(), 1, 1)
    big = post & (e.event_type == "transfer") & (e.amount_vnd >= bio["single_transfer_vnd"])
    check("transfer >= 10M right after a passed amount check (share)", ((prev.event_type == "biometric_verify") & (prev.result == "success") & (prev.reason == "amount"))[big].mean(), 1, 1)
    new_login = post & (e.event_type == "login") & e.is_new_device
    check("new-device login followed by a new_device check (share)", ((nxt.event_type == "biometric_verify") & (nxt.reason == "new_device"))[new_login].mean(), 1, 1)
    check("failed check ends the session (share)", nxt.event_type[is_bio & (e.result == "failed")].isna().mean(), 1, 1)
    t = e[post & (e.event_type == "transfer")].assign(day=lambda d: d.event_ts.dt.date).sort_values("event_ts")
    cum = t.groupby(["customer_id", "day"]).amount_vnd.cumsum()
    cross = t[(cum > bio["daily_total_vnd"]) & (cum - t.amount_vnd <= bio["daily_total_vnd"])]
    ok_bio = e[is_bio & (e.result == "success") & e.reason.isin(["amount", "daily_total"])][["customer_id", "event_ts"]].rename(columns={"event_ts": "bio_ts"})
    hit = pd.merge_asof(cross, ok_bio.sort_values("bio_ts"), left_on="event_ts", right_on="bio_ts", by="customer_id")
    check("transfer crossing 20M/day has a passed check that day (share)", (hit.bio_ts.dt.date == hit.day).mean(), 1, 1)
    checks = e[is_bio]
    passed = checks.result.eq("success").groupby(kind[is_bio].eq("legit")).mean()
    check("identity check pass rate, legit", passed[True], bio["pass_rate_legit"] - 0.01, bio["pass_rate_legit"] + 0.01)
    check("identity check pass rate, ATO", passed[False], bio["pass_rate_ato"] - 0.05, bio["pass_rate_ato"] + 0.05)
    dev_bio = a["transfer_auth_method"]["device_biometric"]
    legit_tr = (e.event_type == "transfer") & kind.eq("legit")
    check("legit transfers approved with device_biometric (share)", e.auth_method[legit_tr].eq("device_biometric").mean(), dev_bio - 0.02, dev_bio + 0.02)

    # ATO patterns (labels from the generator truth; Layer 2 gets them later).
    s = e[kind.eq("new_device")]
    first = s.pivot_table(index="session_id", columns="event_type", values="event_ts", aggfunc="min")
    done = first.transfer.notna()
    seq = (first.password_reset < first.add_payee) & (first.add_payee < first.transfer) & (first.transfer - first.password_reset < pd.Timedelta(minutes=10))
    check("new_device ATO: reset -> add_payee -> transfer < 10 min (share)", seq[done].mean(), 1, 1)
    arr = s.pivot_table(index="session_id", columns="event_type", values="received_ts", aggfunc="min")[done]
    broken = ~((arr.password_reset < arr.add_payee) & (arr.add_payee < arr.transfer))
    print(f"      new_device ATO with a transfer: {done.sum()}, sequence broken in arrival order: {broken.sum()}")
    remote_tr = (e.event_type == "transfer") & kind.eq("remote_access")
    check("remote_access ATO transfers in [9M, 10M) (share)", e.amount_vnd[remote_tr].between(9_000_000, 9_999_999).mean(), 1, 1)
    print(f"      9M-10M transfers: remote ATO {remote_tr.sum()}, legit {(legit_tr & e.amount_vnd.between(9_000_000, 9_999_999)).sum()}")

    # Trend: VietQR at shops grows among transfers.
    tr = e[e.event_type == "transfer"]
    qr = tr.transfer_type.eq("qr_merchant").groupby(tr.event_ts.dt.year).mean()
    growth = 1 + a["qr_share"]["growth_per_year"]
    check("qr_merchant share 2025 / 2024 (growing trend)", qr[2025] / qr[2024], growth - 0.06, growth + 0.06)


def main(config_path: Path) -> None:
    """Load the generated tables and run every check."""
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    src = ROOT / cfg["output_dir"]
    cards = pd.read_parquet(src / "core_banking" / "cards.parquet")
    merchants = pd.read_parquet(src / "core_banking" / "merchants.parquet")
    customers = pd.read_parquet(src / "core_banking" / "customers.parquet")
    export_dir = src / "core-banking-export" / "transactions"
    files = sorted(export_dir.rglob("*.parquet"))
    schemas = [pq.read_schema(f) for f in files]
    # Old and new export files have different columns, so read them with the merged schema
    # (the same fix the Spark job needs: mergeSchema).
    txn = read_merged(export_dir, schemas)
    mapping = pd.read_csv(MAPPING_CSV)
    new_units = set(mapping.new_unit_name)
    # Values can be old OR new names: "Hue" is a new name only (old: "Thua Thien Hue"), so map both.
    to_unit = {**{u: u for u in new_units}, **dict(zip(mapping.old_province_name, mapping.new_unit_name))}
    cutoff = pd.Timestamp(cfg["reorganization"]["cutoff"])
    print(f"rows: {len(txn):,} transactions, {txn.transaction_id.nunique():,} unique\n")

    # Safety: synthetic tokens only, and referential integrity with the dimension tables.
    check("card_id is a synthetic token (share)", cards.card_id.str.fullmatch(r"card_[0-9a-f]{8}").mean(), 1, 1)
    check("txn.card_id found in cards (share)", txn.card_id.isin(cards.card_id).mean(), 1, 1)
    check("txn.merchant_id found in merchants (share)", txn.merchant_id.isin(merchants.merchant_id).mean(), 1, 1)
    check("txn_ts after card issue_ts (share)", (txn.txn_ts > txn.card_id.map(cards.set_index("card_id").issue_ts)).mean(), 1, 1)

    # Problem: offline duplicates. Extra rows share transaction_id and carry a later created_ts.
    rate = cfg["duplicates"]["offline_rate"]
    check("duplicate rate (extra rows / unique)", len(txn) / txn.transaction_id.nunique() - 1, rate - 0.003, rate + 0.003)
    span = txn[txn.transaction_id.duplicated(keep=False)].groupby("transaction_id").created_ts.agg(["min", "max"])
    check("duplicates have a later created_ts (share)", (span["max"] > span["min"]).mean(), 1, 1)
    dedup = txn.sort_values("created_ts").drop_duplicates("transaction_id", keep="last")

    # Problem: primary skew, merchant join skew.
    counts = dedup.merchant_id.value_counts()
    top_n, top_share = cfg["skew"]["merchant"]["top_n_merchants"], cfg["skew"]["merchant"]["top_share"]
    check(f"top-{top_n} merchant share", counts.head(top_n).sum() / len(dedup), top_share - 0.02, top_share + 0.02)
    check("biggest merchant / median merchant rows", counts.iloc[0] / counts.median(), 100, float("inf"))

    # Trend: ATM share of card payments shrinks every year.
    atm, year = dedup.channel.eq("atm"), dedup.txn_ts.dt.year
    keep = 1 - cfg["trends"]["atm_decline_per_year"]
    check("ATM share 2025 / 2024 (declining trend)", atm[year == 2025].mean() / atm[year == 2024].mean(), keep - 0.05, keep + 0.05)

    # Problem: secondary skew on txn_city, measured after normalizing to the 34 units.
    unit = dedup.txn_city.map(to_unit)
    check("txn_city normalizes to the 34 units (share)", unit.isin(new_units).mean(), 1, 1)
    for name, share in cfg["skew"]["txn_city"].items():
        if name != "others":
            check(f"share of {name} (normalized)", (unit == name).mean(), share - 0.02, share + 0.02)

    # Problem: administrative reorganization. Old names dominate before the cutoff, linger after it.
    before, after = dedup.txn_ts < cutoff, dedup.txn_ts >= cutoff
    old_before = (~dedup.txn_city[before].isin(new_units)).mean()
    old_after = (~dedup.txn_city[after].isin(new_units)).mean()
    check("old-only names before cutoff (share)", old_before, 0.05, 1)
    check("old-only names after cutoff (share, stale)", old_after, 0.001, old_before / 5)
    hcm = cfg["skew"]["txn_city"]["Ho Chi Minh"] * cfg["reorganization"]["back_map_weights"]["Ho Chi Minh"]["Ho Chi Minh"]
    check("raw 'Ho Chi Minh' share before cutoff (fake jump)", (dedup.txn_city[before] == "Ho Chi Minh").mean(), hcm - 0.02, hcm + 0.02)
    check("customers.city normalizes to the 34 units (share)", customers.city.map(to_unit).isin(new_units).mean(), 1, 1)

    check_schema_evolution(cfg, files, schemas, dedup)
    check_scd2(cfg, customers, to_unit, new_units)
    disputes = pd.read_parquet(src / "core_banking" / "disputes.parquet")
    check_labels(cfg, dedup, disputes)
    check_moves(dedup, cards, pd.read_parquet(src / "core_banking" / "accounts.parquet"), customers, to_unit)
    check_terminals(dedup)
    check_stream(cfg, src, dedup, disputes, to_unit)
    check_app_events(cfg, src)

    assert not failures, f"{len(failures)} check(s) failed: {failures}"
    print("\nall checks passed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    main(ap.parse_args().config)
