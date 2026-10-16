"""Verify that the generated source data really contains the injected problems.

Every check is an assert with a tolerance. The printed metrics feed docs/data_characteristics.md.
Usage: python generator/check_data.py [--config generator/config.yaml]
"""
import argparse
from pathlib import Path

import pandas as pd
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


def main(config_path: Path) -> None:
    """Load the generated tables and run every check."""
    cfg = yaml.safe_load(config_path.read_text())
    src = ROOT / cfg["output_dir"]
    cards = pd.read_parquet(src / "core_banking" / "cards.parquet")
    merchants = pd.read_parquet(src / "core_banking" / "merchants.parquet")
    customers = pd.read_parquet(src / "core_banking" / "customers.parquet")
    txn = pd.read_parquet(src / "core-banking-export" / "transactions")
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

    assert not failures, f"{len(failures)} check(s) failed: {failures}"
    print("\nall checks passed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    main(ap.parse_args().config)
