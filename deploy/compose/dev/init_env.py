"""Write deploy/compose/dev/.env from .env.example, generating every secret.

Safe to run again: an existing .env is left untouched, because the databases were
created with the passwords in it.
"""

from __future__ import annotations

import secrets
from pathlib import Path

HERE = Path(__file__).parent


def main() -> None:
    target = HERE / ".env"
    if target.exists():
        print(f"{target} already exists; leaving it as it is")
        return
    lines = []
    for line in (HERE / ".env.example").read_text(encoding="utf-8").splitlines():
        if line.endswith("=change-me"):
            line = line.replace("change-me", secrets.token_hex(16))
        lines.append(line)
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {target} with generated secrets")


if __name__ == "__main__":
    main()
