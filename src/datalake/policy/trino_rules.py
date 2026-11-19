"""Turns the access policy into Trino file-based access control rules.

Each role in access.yml becomes one Trino user, `role_<name>`. The SDK's service user
may impersonate those users and nothing else, so every query runs with exactly one
role's grants. Trino evaluates rules top to bottom and the first match wins, so the
specific grants come first and a deny-everything rule comes last.

Format: https://trino.io/docs/current/security/file-system-access-control.html
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from datalake.core.catalog import Dataset
from datalake.core.errors import PolicyError
from datalake.policy.model import AccessRules, RoleRule

_ROLE_NAME = re.compile(r"[a-z][a-z0-9_]*")
# GRANT_SELECT lets other users read the views the pipeline creates over its tables.
_PIPELINE_PRIVILEGES = ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP", "GRANT_SELECT"]


def trino_user_for(role: str) -> str:
    """The Trino user a query runs as for the given role."""
    if not _ROLE_NAME.fullmatch(role):
        raise PolicyError(
            f"role name {role!r} must be lower-case letters, digits and underscores, "
            "starting with a letter"
        )
    return f"role_{role}"


def build_trino_rules(
    rules: AccessRules,
    datasets: Sequence[Dataset],
    catalog: str,
    service_user: str,
    pipeline_user: str,
    catalog_user: str | None = None,
) -> dict[str, Any]:
    """Build the rules document.

    `service_user` is the platform's app: it may only impersonate role users.
    `pipeline_user` is dbt and ingestion: it owns the lake catalog.
    `catalog_user`, if given, is the metadata catalog's reader (OpenMetadata): it may
    read every table's definition and data in the lake, and change nothing.
    """
    _check_masks(rules, datasets)
    roles = [rules.roles[name] for name in sorted(rules.roles)]
    role_users = [trino_user_for(role.name) for role in roles]
    readers = [re.escape(user) for user in role_users]
    if catalog_user is not None:
        readers.append(re.escape(catalog_user))

    table_rules: list[dict[str, Any]] = []
    for role in roles:
        for dataset in sorted(datasets, key=lambda d: (d.schema, d.name)):
            rule = _table_rule(role, dataset, catalog, rules.hidden_schemas)
            if rule is not None:
                table_rules.append(rule)
    # dbt reads table comments from Trino's built-in `system` catalog.
    table_rules.append(
        {"user": re.escape(pipeline_user), "catalog": "system", "privileges": ["SELECT"]}
    )
    table_rules.append(
        {"user": re.escape(pipeline_user), "catalog": catalog, "privileges": _PIPELINE_PRIVILEGES}
    )
    if catalog_user is not None:
        table_rules.append(
            {"user": re.escape(catalog_user), "catalog": catalog, "privileges": ["SELECT"]}
        )
    table_rules.append({"privileges": []})

    catalog_rules: list[dict[str, Any]] = [
        {"user": user, "catalog": catalog, "allow": "read-only"} for user in readers
    ]
    catalog_rules.append({"user": re.escape(pipeline_user), "catalog": catalog, "allow": "all"})
    catalog_rules.append(
        {"user": re.escape(pipeline_user), "catalog": "system", "allow": "read-only"}
    )
    catalog_rules.append({"allow": "none"})

    impersonation: list[dict[str, Any]] = []
    if role_users:
        impersonation.append(
            {
                "original_user": re.escape(service_user),
                "new_user": "|".join(re.escape(user) for user in role_users),
                "allow": True,
            }
        )
    impersonation.append({"original_user": ".*", "new_user": ".*", "allow": False})

    return {
        "catalogs": catalog_rules,
        "schemas": [
            {"user": re.escape(pipeline_user), "catalog": catalog, "owner": True},
            {"owner": False},
        ],
        "tables": table_rules,
        "impersonation": impersonation,
    }


def _table_rule(
    role: RoleRule, dataset: Dataset, catalog: str, hidden_schemas: frozenset[str]
) -> dict[str, Any] | None:
    """SELECT on one table for one role, or None if the role may not see the table."""
    if dataset.schema in hidden_schemas:
        return None
    if dataset.domain not in role.domains or dataset.classification > role.max_classification:
        return None

    masks = {mask.column: mask.expression for mask in role.masks if mask.model == dataset.name}
    columns: list[dict[str, Any]] = []
    for column in dataset.columns:
        if column.classification > role.max_classification:
            # Too sensitive for this role: hidden outright, which beats any mask.
            columns.append({"name": column.name, "allow": False})
        elif column.name in masks:
            columns.append({"name": column.name, "mask": masks[column.name]})

    rule: dict[str, Any] = {
        "user": re.escape(trino_user_for(role.name)),
        "catalog": catalog,
        "schema": re.escape(dataset.schema),
        "table": re.escape(dataset.name),
        "privileges": ["SELECT"],
    }
    if columns:
        rule["columns"] = columns
    return rule


def _check_masks(rules: AccessRules, datasets: Sequence[Dataset]) -> None:
    """A mask naming a model or column that does not exist would silently do nothing."""
    known = {dataset.name: {column.name for column in dataset.columns} for dataset in datasets}
    for role in rules.roles.values():
        for mask in role.masks:
            if mask.model not in known:
                raise PolicyError(f"role {role.name!r} masks unknown model {mask.model!r}")
            if mask.column not in known[mask.model]:
                raise PolicyError(
                    f"role {role.name!r} masks unknown column {mask.model}.{mask.column}"
                )
