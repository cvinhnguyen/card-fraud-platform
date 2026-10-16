"""Verify that the generated source data really contains the injected problems.

Every check is an assert with a tolerance. The printed metrics feed docs/data_characteristics.md.
Usage: python generator/check_data.py [--config generator/config.yaml]
"""
import argparse
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
    recent = dedup.txn_ts >= end - pd.Timedelta(days=30)
    check("last-30-day fraud rate / mature rate (label delay)", is_fraud[recent].mean() / rate, 0, 0.7)
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
    check_labels(cfg, dedup, pd.read_parquet(src / "core_banking" / "disputes.parquet"))
    check_moves(dedup, cards, pd.read_parquet(src / "core_banking" / "accounts.parquet"), customers, to_unit)
    check_terminals(dedup)

    assert not failures, f"{len(failures)} check(s) failed: {failures}"
    print("\nall checks passed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    main(ap.parse_args().config)
