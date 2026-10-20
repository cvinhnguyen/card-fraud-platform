"""Create local secrets once: `infra/.env`, the SeaweedFS S3 identity file and the DataHub login file.

Both files are gitignored. Values are random and never printed.
Re-running is safe: existing values are kept (running containers keep working), only new keys are added.

Usage: python infra/make_env.py
"""
import base64
import json
import secrets
from pathlib import Path

INFRA = Path(__file__).parent
ENV_FILE = INFRA / ".env"
S3_FILE = INFRA / "conf" / "seaweedfs" / "s3.json"
NEW_VALUE = {   # key -> how to make its value; keys added later are appended to an existing .env
    "CORE_DB_USER": lambda: "core",
    "CORE_DB_PASSWORD": lambda: secrets.token_urlsafe(24),
    "S3_ACCESS_KEY": lambda: f"fraud{secrets.token_hex(6)}",
    "S3_SECRET_KEY": lambda: secrets.token_urlsafe(32),
    "METASTORE_DB_PASSWORD": lambda: secrets.token_urlsafe(24),   # P3: Hive Metastore backend
    "AIRFLOW_DB_PASSWORD": lambda: secrets.token_urlsafe(24),     # P7: Airflow metadata database
    "AIRFLOW_FERNET_KEY": lambda: base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),   # encrypts connections
    "AIRFLOW_SECRET_KEY": lambda: secrets.token_urlsafe(32),      # webserver session signing
    "AIRFLOW_ADMIN_PASSWORD": lambda: secrets.token_urlsafe(16),  # UI user `admin`
    "DATAHUB_DB_PASSWORD": lambda: secrets.token_urlsafe(24),     # P8: DataHub metadata database
    "DATAHUB_SECRET": lambda: secrets.token_urlsafe(32),          # frontend session signing
    "DATAHUB_TOKEN_SIGNING_KEY": lambda: base64.b64encode(secrets.token_bytes(32)).decode(),
    "DATAHUB_TOKEN_SALT": lambda: secrets.token_urlsafe(24),
    "DATAHUB_ADMIN_PASSWORD": lambda: secrets.token_urlsafe(16),  # UI user `datahub` (written to user.props)
    "ML_DB_PASSWORD": lambda: secrets.token_urlsafe(24),          # Layer 2: Feast registry + MLflow backend
    "REDIS_PASSWORD": lambda: secrets.token_urlsafe(24),          # Layer 2: Feast online store
    "JUPYTER_TOKEN": lambda: secrets.token_urlsafe(24),           # Layer 2: notebook login
    "LLM_API_KEY": lambda: secrets.token_urlsafe(24),             # Layer 3: llama.cpp model servers (Bearer key)
    "VECTOR_DB_PASSWORD": lambda: secrets.token_urlsafe(24),      # Layer 3: pgvector store of the RAG knowledge base
    "AGW_API_KEY": lambda: secrets.token_urlsafe(24),             # Layer 3: clients (agents) -> agentgateway
    "MCP_KEY_CARD_DATA_AGENT": lambda: secrets.token_urlsafe(24), # Layer 3: card data agent -> MCP route (its tools only)
    "MCP_KEY_RISK_AGENT": lambda: secrets.token_urlsafe(24),      # Layer 3: risk agent -> MCP route (its tools only)
}
DATAHUB_USERS = INFRA / "conf" / "datahub" / "user.props"   # frontend login file, replaces the default datahub:datahub


def read_env(path: Path = ENV_FILE) -> dict[str, str]:
    """Parse a KEY=VALUE env file into a dict (comments and blank lines skipped)."""
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#"))
    return {k.strip(): v.strip() for k, v in pairs}


def main() -> None:
    """Add any missing key to `.env`, then write `s3.json` and DataHub's `user.props` if they do not exist yet."""
    env = read_env() if ENV_FILE.exists() else {}
    missing = [k for k in NEW_VALUE if k not in env]
    if missing:
        with ENV_FILE.open("a", newline="\n") as f:   # LF on Windows too: these files are read in Linux containers
            f.writelines(f"{k}={NEW_VALUE[k]()}\n" for k in missing)
        print(f"{ENV_FILE}: added {', '.join(missing)}")
    if not S3_FILE.exists():
        env = read_env()
        identity = {"name": "admin", "credentials": [{"accessKey": env["S3_ACCESS_KEY"], "secretKey": env["S3_SECRET_KEY"]}],
                    "actions": ["Admin", "Read", "Write", "List", "Tagging"]}
        S3_FILE.write_text(json.dumps({"identities": [identity]}, indent=2), newline="\n")
        print(f"created {S3_FILE}")
    if not DATAHUB_USERS.exists():
        DATAHUB_USERS.parent.mkdir(parents=True, exist_ok=True)
        DATAHUB_USERS.write_text(f"datahub:{read_env()['DATAHUB_ADMIN_PASSWORD']}\n", newline="\n")
        print(f"created {DATAHUB_USERS}")

if __name__ == "__main__":
    main()
