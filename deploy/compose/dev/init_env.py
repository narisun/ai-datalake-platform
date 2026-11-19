"""Write deploy/compose/dev/.env from .env.example, generating every secret.

Safe to run again. Values already in .env are never changed, because the databases
and accounts were created with them; entries added to .env.example since are appended.
"""

from __future__ import annotations

import secrets
from pathlib import Path

HERE = Path(__file__).parent


def main() -> None:
    target = HERE / ".env"
    existing = target.read_text(encoding="utf-8") if target.exists() else ""
    have = {line.split("=", 1)[0] for line in existing.splitlines() if "=" in line}

    added: list[str] = []
    for line in (HERE / ".env.example").read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.startswith("#"):
            continue
        name, value = line.split("=", 1)
        if name in have:
            continue
        if value == "change-me":
            value = secrets.token_hex(16)
        added.append(f"{name}={value}")

    if not added:
        print(f"{target} is complete; nothing to add")
        return
    with target.open("a", encoding="utf-8", newline="\n") as handle:
        if existing and not existing.endswith("\n"):
            handle.write("\n")
        handle.write("\n".join(added) + "\n")
    print(f"added {len(added)} entr{'y' if len(added) == 1 else 'ies'} to {target}")


if __name__ == "__main__":
    main()
