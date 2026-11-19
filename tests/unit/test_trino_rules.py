from __future__ import annotations

import re
from typing import Any

import pytest

from datalake.core.catalog import Column, Dataset
from datalake.core.errors import PolicyError
from datalake.core.security import Classification
from datalake.policy.policy_file import parse_access_rules
from datalake.policy.trino_rules import build_trino_rules, trino_user_for

POLICY = """
roles:
  analyst_sales:
    domains: [sales]
    max_classification: internal
    masks:
      - column: dim_customer.country
        expression: "'??'"
  steward_sales:
    domains: [sales]
    max_classification: confidential
"""

DATASETS = [
    Dataset(
        urn="model.p.dim_customer",
        name="dim_customer",
        domain="sales",
        schema="analytics_marts",
        columns=(
            Column("customer_id", "varchar"),
            Column("email", "varchar", classification=Classification.CONFIDENTIAL),
            Column("country", "varchar"),
        ),
    ),
    Dataset(
        urn="model.p.stg_customers",
        name="stg_customers",
        domain="sales",
        schema="analytics_staging",
        classification=Classification.CONFIDENTIAL,
    ),
    Dataset(urn="model.p.payroll", name="payroll", domain="hr", schema="analytics_marts"),
    Dataset(urn="source.p.raw.x", name="x", domain="", schema="raw"),
]


def _build(policy: str = POLICY, datasets: list[Dataset] | None = None) -> dict[str, Any]:
    return build_trino_rules(
        rules=parse_access_rules(policy),
        datasets=DATASETS if datasets is None else datasets,
        catalog="lake",
        service_user="svc_datalake",
        pipeline_user="svc_pipeline",
    )


def _grants(document: dict[str, Any], user: str) -> dict[str, dict[str, Any]]:
    return {rule["table"]: rule for rule in document["tables"] if rule.get("user") == user}


def test_role_gets_select_only_on_tables_in_its_domain_and_ceiling() -> None:
    grants = _grants(_build(), "role_analyst_sales")

    assert set(grants) == {"dim_customer"}
    assert grants["dim_customer"]["privileges"] == ["SELECT"]
    assert grants["dim_customer"]["catalog"] == "lake"
    assert grants["dim_customer"]["schema"] == "analytics_marts"


def test_higher_ceiling_reaches_more_tables() -> None:
    assert set(_grants(_build(), "role_steward_sales")) == {"dim_customer", "stg_customers"}


def test_a_hidden_schema_is_granted_to_no_role() -> None:
    document = _build("hidden_schemas: [analytics_staging]\n" + POLICY)

    assert set(_grants(document, "role_steward_sales")) == {"dim_customer"}
    # The pipeline still owns the whole catalog.
    assert document["tables"][-2]["user"] == "svc_pipeline"


def test_sensitive_columns_are_hidden_and_masks_applied() -> None:
    columns = _grants(_build(), "role_analyst_sales")["dim_customer"]["columns"]

    assert columns == [
        {"name": "email", "allow": False},
        {"name": "country", "mask": "'??'"},
    ]


def test_role_that_may_see_every_column_has_no_column_rules() -> None:
    assert "columns" not in _grants(_build(), "role_steward_sales")["dim_customer"]


def test_hiding_a_column_beats_masking_it() -> None:
    policy = POLICY.replace("dim_customer.country", "dim_customer.email")

    columns = _grants(_build(policy), "role_analyst_sales")["dim_customer"]["columns"]

    assert columns == [{"name": "email", "allow": False}]


def test_untagged_and_other_domain_tables_are_granted_to_no_role() -> None:
    granted = {rule["table"] for rule in _build()["tables"] if "table" in rule}

    assert granted == {"dim_customer", "stg_customers"}


def test_last_table_rule_denies_everything_and_pipeline_rule_precedes_it() -> None:
    tables = _build()["tables"]

    assert tables[-1] == {"privileges": []}
    assert tables[-2]["user"] == "svc_pipeline"
    assert "OWNERSHIP" in tables[-2]["privileges"]


def test_catalog_rules_are_read_only_for_roles_and_closed_by_default() -> None:
    catalogs = _build()["catalogs"]

    assert {"user": "role_analyst_sales", "catalog": "lake", "allow": "read-only"} in catalogs
    assert {"user": "svc_pipeline", "catalog": "lake", "allow": "all"} in catalogs
    assert catalogs[-1] == {"allow": "none"}


def test_service_user_may_impersonate_only_role_users() -> None:
    first, last = _build()["impersonation"]

    assert first["original_user"] == "svc_datalake"
    assert first["allow"] is True
    pattern = re.compile(first["new_user"])
    assert pattern.fullmatch("role_analyst_sales")
    assert pattern.fullmatch("role_steward_sales")
    assert not pattern.fullmatch("svc_pipeline")
    assert not pattern.fullmatch("role_analyst_sales_admin")
    assert last == {"original_user": ".*", "new_user": ".*", "allow": False}


def test_table_names_are_escaped_as_regex_literals() -> None:
    odd = [Dataset(urn="m", name="a.b", domain="sales", schema="s")]
    no_masks = POLICY.split("    masks:")[0] + POLICY.split("expression: \"'??'\"")[1]

    rule = _grants(_build(no_masks, odd), "role_steward_sales")[r"a\.b"]

    assert re.fullmatch(rule["table"], "a.b")
    assert not re.fullmatch(rule["table"], "axb")


@pytest.mark.parametrize(
    ("target", "message"),
    [("no_such_model.country", "unknown model"), ("dim_customer.nope", "unknown column")],
)
def test_mask_on_something_that_does_not_exist_is_an_error(target: str, message: str) -> None:
    with pytest.raises(PolicyError, match=message):
        _build(POLICY.replace("dim_customer.country", target))


@pytest.mark.parametrize("name", ["Analyst", "sales-team", "1st", "a.*"])
def test_role_names_that_are_unsafe_as_trino_users_are_rejected(name: str) -> None:
    with pytest.raises(PolicyError, match="role name"):
        trino_user_for(name)


def test_catalog_user_reads_everything_in_the_lake_and_changes_nothing() -> None:
    document = build_trino_rules(
        rules=parse_access_rules(POLICY),
        datasets=DATASETS,
        catalog="lake",
        service_user="svc_datalake",
        pipeline_user="svc_pipeline",
        catalog_user="svc_catalog",
    )

    tables = document["tables"]
    assert tables[-1] == {"privileges": []}
    assert tables[-2] == {"user": "svc_catalog", "catalog": "lake", "privileges": ["SELECT"]}
    assert tables[-3]["user"] == "svc_pipeline"
    assert {"user": "svc_catalog", "catalog": "lake", "allow": "read-only"} in document["catalogs"]
    # It is not a role, so the app can never act as it.
    pattern = re.compile(document["impersonation"][0]["new_user"])
    assert not pattern.fullmatch("svc_catalog")


def test_pipeline_may_let_others_read_its_views() -> None:
    pipeline = next(
        rule
        for rule in _build()["tables"]
        if rule.get("user") == "svc_pipeline" and rule["catalog"] == "lake"
    )

    assert "GRANT_SELECT" in pipeline["privileges"]


def test_only_the_pipeline_reads_the_system_catalog() -> None:
    document = _build()

    system_tables = [rule for rule in document["tables"] if rule.get("catalog") == "system"]
    system_catalogs = [rule for rule in document["catalogs"] if rule.get("catalog") == "system"]
    assert system_tables == [
        {"user": "svc_pipeline", "catalog": "system", "privileges": ["SELECT"]}
    ]
    assert system_catalogs == [{"user": "svc_pipeline", "catalog": "system", "allow": "read-only"}]
