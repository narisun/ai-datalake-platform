"""Access-control scenarios against the live dev stack. Run inside the airflow container:

    python /opt/datalake/dev/scenarios_access.py

Each check connects to Trino as one user and states what must happen. It needs the
nightly pipeline to have run, so the lake tables and the generated rules exist.

Trino requires sign-in. Only the three service accounts have passwords. A check made
"as a role" signs in as the platform's own account (svc_datalake) and acts as that
role, which is how the agent will query.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import requests
import trino
from trino_dev import CA, HOST, PASSWORDS, PORT, connect

WORK = Path("/opt/datalake/work")
RULES = WORK / "trino-rules.json"
ANALYST = "role_analyst_sales"
SUPPORT = "role_support_sales"
PROBE = "select count(*) from lake.analytics_marts.fct_orders"
failures: list[str] = []


def query(connection, sql: str) -> tuple[list[str], list[list[object]]] | str:
    """Rows and column names, or the refusal message."""
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = [list(row) for row in cursor.fetchall()]
        return [column[0] for column in cursor.description or []], rows
    except trino.exceptions.TrinoQueryError as exc:
        return str(exc.message)
    except (trino.exceptions.HttpError, requests.RequestException) as exc:
        return str(exc)[:160]


def run(user: str, sql: str) -> tuple[list[str], list[list[object]]] | str:
    """Run as a service account, or as a role through the platform's account."""
    if user.startswith("role_"):
        return query(connect("svc_datalake", act_as=user), sql)
    return query(connect(user), sql)


def check(label: str, passed: bool, detail: object = "") -> None:
    print(f"  {'PASS' if passed else 'FAIL'}  {label}" + (f"   [{detail}]" if detail else ""))
    if not passed:
        failures.append(label)


def allowed(label: str, user: str, sql: str) -> list[list[object]]:
    result = run(user, sql)
    check(label, not isinstance(result, str), result if isinstance(result, str) else "")
    return [] if isinstance(result, str) else result[1]


def denied(label: str, user: str, sql: str) -> None:
    result = run(user, sql)
    check(
        label,
        isinstance(result, str),
        "" if isinstance(result, str) else f"returned {result[1][:2]}",
    )


def generate_rules(policy: Path) -> None:
    subprocess.run(
        [
            "dl",
            "policy",
            "trino-rules",
            "--policy",
            str(policy),
            "--manifest",
            str(WORK / "target/manifest.json"),
            "--catalog-user",
            "svc_catalog",
            "--out",
            str(RULES),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )


def wait_until(predicate, seconds: int = 40) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(2)
    return False


def refused(label: str, result: object) -> None:
    check(label, isinstance(result, str), "" if isinstance(result, str) else "it was allowed")


def unsigned(user: str, port: int = PORT, scheme: str = "https"):
    """A connection that states a user but offers no password."""
    return trino.dbapi.connect(host=HOST, port=port, http_scheme=scheme, verify=CA, user=user)


print("== 0. Nobody gets in without signing in")
refused("no password is refused", query(unsigned("svc_pipeline"), PROBE))
refused(
    "a wrong password is refused",
    query(connect("svc_pipeline", password="not-the-password"), PROBE),
)
refused(
    "an account that does not exist is refused",
    query(connect("mallory", password="anything"), PROBE),
)
refused(
    "a role cannot sign in directly: it has no password",
    query(connect(ANALYST, password="anything"), PROBE),
)
refused(
    "one account's password does not work for another",
    query(
        connect("svc_pipeline", password=__import__("os").environ[PASSWORDS["svc_datalake"]]),
        PROBE,
    ),
)
refused(
    "the plain-HTTP port does not let a user in",
    query(unsigned("svc_pipeline", port=8080, scheme="http"), PROBE),
)
result = query(connect("svc_pipeline"), PROBE)
check("the pipeline signs in with its password", not isinstance(result, str), result)

print("== 1. An analyst sees internal sales data")
rows = allowed(
    "reads the orders mart", ANALYST, "select count(*) from lake.analytics_marts.fct_orders"
)
check("and gets rows", bool(rows) and rows[0][0] > 0, rows[0][0] if rows else "")
allowed(
    "reads the daily revenue table",
    ANALYST,
    "select * from lake.analytics_marts.agg_daily_revenue limit 1",
)
# Staging is a hidden schema in access.yml: a working layer for the pipeline only.
denied(
    "cannot read a staging view, though it is tagged internal",
    ANALYST,
    "select count(*) from lake.analytics_staging.stg_orders",
)
denied(
    "cannot read the rejected-rows list",
    ANALYST,
    "select count(*) from lake.analytics_staging.rejected_rows",
)
denied(
    "support cannot read customer staging, where email has no mask",
    SUPPORT,
    "select email from lake.analytics_staging.stg_customers limit 1",
)

print("== 2. An analyst does not see personal data")
denied(
    "cannot read the confidential customer staging view",
    ANALYST,
    "select * from lake.analytics_staging.stg_customers limit 1",
)
result = run(ANALYST, "select * from lake.analytics_marts.dim_customer limit 1")
columns = [] if isinstance(result, str) else result[0]
check(
    "select * on the customer dimension leaves out name and email",
    columns == ["customer_id", "country"],
    columns or result,
)
denied(
    "cannot ask for the email column by name",
    ANALYST,
    "select email from lake.analytics_marts.dim_customer",
)
denied(
    "cannot reach email through a filter either",
    ANALYST,
    "select customer_id from lake.analytics_marts.dim_customer where email like '%ada%'",
)

print("== 3. Support sees customers, with the email partly masked")
rows = allowed(
    "reads name and email",
    SUPPORT,
    "select customer_id, full_name, email from lake.analytics_canonical.customer order by 1 limit 2",
)
emails = [row[2] for row in rows]
check(
    "email shows only the domain",
    bool(emails) and all(str(e).startswith("***@") for e in emails),
    emails,
)
rows = allowed(
    "can still filter on the masked column",
    SUPPORT,
    "select count(*) from lake.analytics_canonical.customer where email like '***@%'",
)

print("== 4. Nobody but the pipeline sees raw data")
denied("analyst cannot read a raw table", ANALYST, "select * from lake.raw.orders limit 1")
denied("analyst cannot even list the tables in raw", ANALYST, "show tables from lake.raw")
denied("support cannot read a raw table", SUPPORT, "select * from lake.raw.customers limit 1")

print("== 5. Roles cannot change anything")
denied(
    "analyst cannot insert",
    ANALYST,
    "insert into lake.analytics_marts.agg_daily_revenue values (date '2026-01-01', 'XX', 1, 1.00)",
)
denied("analyst cannot delete", ANALYST, "delete from lake.analytics_marts.fct_orders")
denied("analyst cannot drop a table", ANALYST, "drop table lake.analytics_marts.fct_orders")
denied(
    "analyst cannot create a table", ANALYST, "create table lake.analytics_marts.mine (x integer)"
)
denied("support cannot update", SUPPORT, "update lake.analytics_canonical.customer set email = 'x'")

print("== 6. The platform's own account can do nothing except act as a role")
refused("reads no table as itself", query(connect("svc_datalake"), PROBE))
refused(
    "cannot act as the pipeline",
    query(connect("svc_datalake", act_as="svc_pipeline"), PROBE),
)
refused(
    "cannot act as the catalog reader",
    query(connect("svc_datalake", act_as="svc_catalog"), PROBE),
)
refused(
    "cannot act as a role that does not exist",
    query(connect("svc_datalake", act_as="role_admin"), PROBE),
)
refused(
    "the catalog reader cannot act as a role",
    query(connect("svc_catalog", act_as=ANALYST), PROBE),
)
refused(
    "the pipeline cannot act as a role",
    query(connect("svc_pipeline", act_as=ANALYST), PROBE),
)

print("== 7. The catalog reader reads everything and changes nothing")
allowed("reads a raw table", "svc_catalog", "select count(*) from lake.raw.orders")
allowed(
    "reads confidential columns (to profile and describe them)",
    "svc_catalog",
    "select email from lake.analytics_canonical.customer limit 1",
)
denied("cannot delete", "svc_catalog", "delete from lake.raw.orders")

print("== 8. A table nobody has tagged is hidden from every role")
allowed(
    "pipeline creates an untagged table",
    "svc_pipeline",
    "create or replace table lake.analytics_marts.zz_untagged as select 1 as x",
)
denied("analyst cannot read it", ANALYST, "select * from lake.analytics_marts.zz_untagged")
denied("support cannot read it", SUPPORT, "select * from lake.analytics_marts.zz_untagged")
allowed("pipeline drops it again", "svc_pipeline", "drop table lake.analytics_marts.zz_untagged")

print("== 9. A policy change takes effect without restarting Trino")
original = Path("/opt/datalake/dbt/access.yml").read_text(encoding="utf-8")
revoked = WORK / "access-revoked.yml"
revoked.write_text(
    original.replace(
        "  analyst_sales:\n    domains: [sales]", "  analyst_sales:\n    domains: [finance]"
    ),
    encoding="utf-8",
)
probe = "select count(*) from lake.analytics_marts.fct_orders"
generate_rules(revoked)
started = time.time()
check(
    "analyst loses the sales domain",
    wait_until(lambda: isinstance(run(ANALYST, probe), str)),
    f"after {time.time() - started:.0f}s",
)
generate_rules(Path("/opt/datalake/dbt/access.yml"))
started = time.time()
check(
    "and gets it back when the policy is restored",
    wait_until(lambda: not isinstance(run(ANALYST, probe), str)),
    f"after {time.time() - started:.0f}s",
)
revoked.unlink()

print()
print(f"{len(failures)} failed" if failures else "all access scenarios passed")
sys.exit(1 if failures else 0)
