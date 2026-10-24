"""Put bronze.raw_transactions (the nightly core banking export) under lakeFS once (EDAI-1 novel idea,
docs/novel_lakefs_wap.md). Idempotent.

  1. repository `bronze` (new objects in s3://lakefs-data/bronze, default branch main)
  2. zero-copy import of s3://lakehouse/bronze/raw_transactions/ into main: lakeFS records the objects, nothing is
     copied, so those files must stay where they are
  3. branch protection on main: no direct writes or commits, only merges (DP1 merges a validated run branch)
  4. the metastore table bronze.raw_transactions re-pointed to s3a://bronze/main/raw_transactions (Spark SQL in
     spark-master), so silver, Trino (catalog lakefs) and every reader see main

Keys come from infra/.env and are never printed. Usage (repo root; core + batch profiles up):
  python infra/init_lakefs.py
"""
import base64
import json
import subprocess
import time
import urllib.error
import urllib.request

from make_env import read_env

API = "http://127.0.0.1:8001/api/v1"
REPO, TABLE = "bronze", "raw_transactions"
SOURCE = f"s3://lakehouse/bronze/{TABLE}/"


def call(method: str, path: str, body=None, ok=(200, 201, 202, 204)):
    """One lakeFS API call as the admin; returns the JSON answer (or None), raises on any other status."""
    env = read_env()
    auth = base64.b64encode(f"{env['LAKEFS_ACCESS_KEY_ID']}:{env['LAKEFS_SECRET_ACCESS_KEY']}".encode()).decode()
    req = urllib.request.Request(API + path, None if body is None else json.dumps(body).encode(), method=method,
                                 headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as e:
        if e.code in ok:
            return None
        raise RuntimeError(f"lakeFS {method} {path}: {e.code} {e.read()[:300]!r}") from None


def main() -> None:
    """Repository, import, protection, metastore locations; each step skipped when already done."""
    if call("GET", f"/repositories/{REPO}", ok=(404,)) is None:
        call("POST", "/repositories", {"name": REPO, "storage_namespace": "s3://lakefs-data/bronze",
                                       "default_branch": "main"})
        print(f"repository {REPO}: created")
    imported = call("GET", f"/repositories/{REPO}/refs/main/objects/ls?prefix={TABLE}/&amount=1")["results"]
    if not imported:
        job = call("POST", f"/repositories/{REPO}/branches/main/import", {
            "paths": [{"path": SOURCE, "destination": f"{TABLE}/", "type": "common_prefix"}],
            "commit": {"message": f"Import {TABLE} from {SOURCE} (zero-copy: metadata only)",
                       "metadata": {"source": SOURCE}}})
        t0 = time.time()
        while not (st := call("GET", f"/repositories/{REPO}/branches/main/import?id={job['id']}"))["completed"]:
            if st.get("error"):
                raise RuntimeError(st["error"])
            time.sleep(2)
        print(f"import: {st['ingested_objects']:,} objects in {time.time() - t0:.0f} s, commit {st['commit']['id'][:12]}")
    call("PUT", f"/repositories/{REPO}/settings/branch_protection", [{"pattern": "main"}])
    print("branch protection: main (merges only)")
    sql = f"ALTER TABLE bronze.{TABLE} SET LOCATION 's3a://{REPO}/main/{TABLE}'"
    subprocess.run(["docker", "compose", "-f", "infra/docker-compose.yaml", "exec", "-T", "spark-master",
                    "/opt/spark/bin/spark-sql", "-e", sql], check=True, capture_output=True)
    print(f"metastore: bronze.{TABLE} -> s3a://{REPO}/main/{TABLE}")


if __name__ == "__main__":
    main()
