"""Connect to the dev stack's Trino, signed in. Used by the scenario scripts.

    python /opt/datalake/dev/trino_dev.py "select 1"            # as the pipeline
    python /opt/datalake/dev/trino_dev.py --role analyst_sales "select ..."

`--role` signs in as the platform's own account and acts as that role, which is how
the agent will query: no role has a password of its own.
"""

from __future__ import annotations

import os
import sys

import trino

HOST, PORT, CA = "trino", 8443, "/etc/trino-tls/ca.crt"
PASSWORDS = {
    "svc_pipeline": "DL_SECRET_TRINO_PIPELINE_PASSWORD",
    "svc_datalake": "DL_SECRET_TRINO_APP_PASSWORD",
    "svc_catalog": "TRINO_CATALOG_PASSWORD",
}


def connect(account: str = "svc_pipeline", act_as: str | None = None, password: str | None = None):
    """Sign in as `account`. With `act_as`, run queries as that other user."""
    if password is None:
        password = os.environ[PASSWORDS[account]]
    return trino.dbapi.connect(
        host=HOST,
        port=PORT,
        http_scheme="https",
        verify=CA,
        user=act_as or account,
        auth=trino.auth.BasicAuthentication(account, password),
    )


def main(argv: list[str]) -> int:
    role = None
    if argv[:1] == ["--role"]:
        role, argv = argv[1], argv[2:]
    connection = connect("svc_datalake", act_as=f"role_{role}") if role else connect("svc_pipeline")
    cursor = connection.cursor()
    cursor.execute(argv[0])
    for row in cursor.fetchall():
        print("   ", *row, sep=" | ")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
