"""Create local secrets once: `infra/.env` and the SeaweedFS S3 identity file.

Both files are gitignored. Values are random and never printed.
Re-running is safe: existing values are kept (running containers keep working), only new keys are added.

Usage: python infra/make_env.py
"""
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
}


def read_env(path: Path = ENV_FILE) -> dict[str, str]:
    """Parse a KEY=VALUE env file into a dict (comments and blank lines skipped)."""
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#"))
    return {k.strip(): v.strip() for k, v in pairs}


def main() -> None:
    """Add any missing key to `.env`, then write `s3.json` if it does not exist yet."""
    env = read_env() if ENV_FILE.exists() else {}
    missing = [k for k in NEW_VALUE if k not in env]
    if missing:
        with ENV_FILE.open("a") as f:
            f.writelines(f"{k}={NEW_VALUE[k]()}\n" for k in missing)
        print(f"{ENV_FILE}: added {', '.join(missing)}")
    if not S3_FILE.exists():
        env = read_env()
        identity = {"name": "admin", "credentials": [{"accessKey": env["S3_ACCESS_KEY"], "secretKey": env["S3_SECRET_KEY"]}],
                    "actions": ["Admin", "Read", "Write", "List", "Tagging"]}
        S3_FILE.write_text(json.dumps({"identities": [identity]}, indent=2))
        print(f"created {S3_FILE}")

if __name__ == "__main__":
    main()
