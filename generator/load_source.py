"""Load generated source data into "another department".

  core_banking/*.parquet          -> Postgres database `core_banking` (tables recreated)
  core-banking-export/...parquet  -> SeaweedFS bucket `core-banking-export` (files overwritten)
  generator/knowledge/*.md        -> SeaweedFS bucket `bank-documents` (the bank's card documents, Layer 3 RAG source)

All stores belong to the source side; our pipelines (DP1, rag_ingest) only read from them.
Usage: python generator/load_source.py [--config generator/config.yaml] [--part core_banking|exports|documents]
"""
import argparse
import io
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import pandas as pd
import psycopg
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "infra"))
from make_env import read_env  # noqa: E402

# Column order matches the Parquet files written by generate.py.
DDL = {
    "customers": "customer_id text, full_name text, dob date, city text, income_band text, risk_tier text, "
                 "kyc_level int, updated_ts timestamp, PRIMARY KEY (customer_id, updated_ts)",
    "accounts": "account_id text PRIMARY KEY, customer_id text NOT NULL, account_type text, open_ts timestamp, "
                "status text, credit_limit bigint",
    "cards": "card_id text PRIMARY KEY, account_id text NOT NULL, card_type text, network text, issue_ts timestamp, "
             "status text, expiry_month text",
    "merchants": "merchant_id text PRIMARY KEY, merchant_name text, mcc int, is_online boolean, city text, country text",
    "ref_province_mapping": "old_province_code text PRIMARY KEY, old_province_name text, new_unit_code text, "
                            "new_unit_name text, effective_from date",
    "disputes": "dispute_id text PRIMARY KEY, transaction_id text NOT NULL, reported_ts timestamp, reason_code text, "
                "is_confirmed_fraud boolean",
}
BUCKET = "core-banking-export"
DOCS_BUCKET = "bank-documents"


def load_postgres(env: dict[str, str], src: Path) -> None:
    """Recreate each table and bulk-load it with COPY, then compare row counts."""
    with psycopg.connect(host="localhost", port=5433, dbname="core_banking",
                         user=env["CORE_DB_USER"], password=env["CORE_DB_PASSWORD"]) as conn, conn.cursor() as cur:
        for table, ddl in DDL.items():
            df = pd.read_parquet(src / f"{table}.parquet")
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} ({ddl})")
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False)   # NA -> empty field -> NULL
            with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv)") as cp:
                cp.write(buf.getvalue())
            loaded = cur.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            assert loaded == len(df), f"{table}: loaded {loaded}, expected {len(df)}"
            print(f"postgres {table}: {loaded:,} rows")


def upload_exports(env: dict[str, str], export_root: Path) -> None:
    """Upload every export file under the same relative path, then compare object counts."""
    s3 = boto3.client("s3", endpoint_url="http://localhost:8333", region_name="us-east-1",
                      aws_access_key_id=env["S3_ACCESS_KEY"], aws_secret_access_key=env["S3_SECRET_KEY"])
    files = sorted(export_root.rglob("*.parquet"))
    keys = [f.relative_to(export_root).as_posix() for f in files]
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda fk: s3.upload_file(str(fk[0]), BUCKET, fk[1]), zip(files, keys)))
    # ponytail: objects from an earlier, longer run are not deleted; empty the bucket if the period shrinks.
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="transactions/")
    found = {o["Key"] for p in pages for o in p.get("Contents", [])}
    assert set(keys) <= found, f"{len(set(keys) - found)} files missing in s3://{BUCKET}"
    print(f"s3://{BUCKET}: {len(keys):,} export files")


def upload_documents(env: dict[str, str], doc_dir: Path) -> None:
    """Upload the bank's documents (Markdown) as `<name>.md`; the RAG pipeline reads them from this bucket."""
    s3 = boto3.client("s3", endpoint_url="http://localhost:8333", region_name="us-east-1",
                      aws_access_key_id=env["S3_ACCESS_KEY"], aws_secret_access_key=env["S3_SECRET_KEY"])
    docs = sorted(doc_dir.glob("*.md"))
    for d in docs:
        s3.upload_file(str(d), DOCS_BUCKET, d.name, ExtraArgs={"ContentType": "text/markdown"})
    print(f"s3://{DOCS_BUCKET}: {len(docs)} documents")


def main(config_path: Path, part: str | None) -> None:
    """Load Postgres tables, upload export files and documents (or only one part)."""
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    src, env = ROOT / cfg["output_dir"], read_env()
    if part in (None, "core_banking"):
        load_postgres(env, src / "core_banking")
    if part in (None, "exports"):
        upload_exports(env, src / "core-banking-export")
    if part in (None, "documents"):
        upload_documents(env, Path(__file__).parent / "knowledge")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load generated source data into Postgres and SeaweedFS.")
    ap.add_argument("--config", type=Path, default=Path(__file__).parent / "config.yaml")
    ap.add_argument("--part", choices=["core_banking", "exports", "documents"], help="load only this part")
    a = ap.parse_args()
    main(a.config, a.part)
