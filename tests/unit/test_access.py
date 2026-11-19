from __future__ import annotations

from pathlib import Path

import pytest

from datalake.core.errors import PolicyError
from datalake.core.security import Classification, Principal
from datalake.policy.model import ColumnMask
from datalake.policy.policy_file import load_access_rules, parse_access_rules
from datalake.policy.service import RoleAccessPolicy

POLICY = """
roles:
  analyst_sales:
    domains: [sales]
    max_classification: internal
    masks:
      - column: dim_customer.email
        expression: "'***'"
  steward_finance:
    domains: [finance, sales]
    max_classification: confidential
"""


def _principal(*roles: str) -> Principal:
    return Principal("alice", frozenset(roles))


def test_parses_roles_domains_and_masks() -> None:
    rules = parse_access_rules(POLICY)

    analyst = rules.roles["analyst_sales"]
    assert analyst.domains == frozenset({"sales"})
    assert analyst.max_classification is Classification.INTERNAL
    assert analyst.masks == (ColumnMask("dim_customer", "email", "'***'"),)
    assert rules.roles["steward_finance"].masks == ()


def test_role_limits_domain_and_classification() -> None:
    scope = parse_access_rules(POLICY).search_filter(_principal("analyst_sales"))

    assert scope.allows("sales", Classification.PUBLIC)
    assert scope.allows("sales", Classification.INTERNAL)
    assert not scope.allows("sales", Classification.CONFIDENTIAL)
    assert not scope.allows("finance", Classification.PUBLIC)


def test_several_roles_combine_to_the_most_permissive_per_domain() -> None:
    scope = parse_access_rules(POLICY).search_filter(_principal("analyst_sales", "steward_finance"))

    assert scope.allows("sales", Classification.CONFIDENTIAL)
    assert scope.allows("finance", Classification.CONFIDENTIAL)
    assert not scope.allows("sales", Classification.RESTRICTED)


@pytest.mark.parametrize("roles", [(), ("unknown_role",)])
def test_no_known_role_denies_everything(roles: tuple[str, ...]) -> None:
    scope = parse_access_rules(POLICY).search_filter(_principal(*roles))

    assert scope.denies_everything
    assert not scope.allows("sales", Classification.PUBLIC)


def test_role_access_policy_delegates_to_the_rules() -> None:
    policy = RoleAccessPolicy(parse_access_rules(POLICY))

    assert policy.search_filter(_principal("analyst_sales")).allows(
        "sales", Classification.INTERNAL
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("roles: [", "not valid YAML"),
        ("- just\n- a list", "must be a mapping"),
        ("roles: {}", "roles"),
        ("roles:\n  r:\n    domains: []\n    max_classification: internal", "domains"),
        (
            "roles:\n  r:\n    domains: [a]\n    max_classification: secret",
            "unknown classification",
        ),
        ("roles:\n  r:\n    domains: [a]\n    max_classification: internal\n    extra: 1", "extra"),
        (
            "roles:\n  r:\n    domains: [a]\n    max_classification: internal\n"
            "    masks:\n      - column: no_dot\n        expression: x",
            "<model>.<column>",
        ),
    ],
)
def test_invalid_policies_are_rejected_with_a_reason(text: str, expected: str) -> None:
    with pytest.raises(PolicyError, match=expected):
        parse_access_rules(text)


def test_loads_from_a_file(tmp_path: Path) -> None:
    path = tmp_path / "access.yml"
    path.write_text(POLICY, encoding="utf-8")

    assert set(load_access_rules(path).roles) == {"analyst_sales", "steward_finance"}


def test_missing_file_is_a_policy_error(tmp_path: Path) -> None:
    with pytest.raises(PolicyError, match="cannot read"):
        load_access_rules(tmp_path / "missing.yml")


def test_classification_parse_is_case_insensitive() -> None:
    assert Classification.parse(" Confidential ") is Classification.CONFIDENTIAL
