"""Create local secrets once: `infra/.env` and the SeaweedFS S3 identity file.

Both files are gitignored. Values are random and never printed.
Re-running is safe: existing files are kept, so running containers keep working.

Usage: python infra/make_env.py
"""
import json
import secrets
from pathlib import Path

INFRA = Path(__file__).parent
ENV_FILE = INFRA / ".env"
S3_FILE = INFRA / "conf" / "seaweedfs" / "s3.json"


def read_env(path: Path = ENV_FILE) -> dict[str, str]:
    """Parse a KEY=VALUE env file into a dict (comments and blank lines skipped)."""
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#"))
    return {k.strip(): v.strip() for k, v in pairs}


def main() -> None:
    """Write `.env` and `s3.json` if they do not exist yet."""
    if not ENV_FILE.exists():
        ENV_FILE.write_text(
            "CORE_DB_USER=core\n"
            f"CORE_DB_PASSWORD={secrets.token_urlsafe(24)}\n"
            f"S3_ACCESS_KEY=fraud{secrets.token_hex(6)}\n"
            f"S3_SECRET_KEY={secrets.token_urlsafe(32)}\n"
        )
        print(f"created {ENV_FILE}")
    if not S3_FILE.exists():
        env = read_env()
        identity = {"name": "admin", "credentials": [{"accessKey": env["S3_ACCESS_KEY"], "secretKey": env["S3_SECRET_KEY"]}],
                    "actions": ["Admin", "Read", "Write", "List", "Tagging"]}
        S3_FILE.write_text(json.dumps({"identities": [identity]}, indent=2))
        print(f"created {S3_FILE}")


if __name__ == "__main__":
    main()
