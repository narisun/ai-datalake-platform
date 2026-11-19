"""Checks that every dbt model carries the tags the platform relies on.

The access policy and OpenMetadata both read these tags. A model that is missing one is
hidden from everyone, and a stray tag would show up in OpenMetadata as a data
classification, so both are caught here, before they reach the lake.
"""

# The manifest is untyped JSON, so unknown-type checks are relaxed for this module.
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from datalake.core.security import Classification
from datalake.transform.dbt_manifest import ManifestError

_CLASSIFICATIONS = {member.name.lower() for member in Classification}


def _tags(entry: Mapping[str, Any]) -> set[str]:
    config_tags = (entry.get("config") or {}).get("tags") or []
    if isinstance(config_tags, str):
        config_tags = [config_tags]
    return {str(tag).lower() for tag in [*(entry.get("tags") or []), *config_tags]}


def _tag_problems(where: str, tags: set[str], required: bool) -> list[str]:
    problems: list[str] = []
    classifications = sorted(tags & _CLASSIFICATIONS)
    if required and not classifications:
        allowed = ", ".join(member.name.lower() for member in Classification)
        problems.append(f"{where}: no classification tag (one of: {allowed})")
    if len(classifications) > 1:
        problems.append(f"{where}: more than one classification tag: {', '.join(classifications)}")
    for tag in sorted(tags - _CLASSIFICATIONS):
        problems.append(
            f"{where}: tag {tag!r} is not allowed; dbt tags are for classification only"
        )
    return problems


def check_tags(manifest: Mapping[str, Any]) -> list[str]:
    """Every problem found, one line each. An empty list means the project is clean."""
    problems: list[str] = []
    nodes: Mapping[str, Any] = manifest.get("nodes", {})
    for unique_id in sorted(nodes):
        node = nodes[unique_id]
        if node.get("resource_type") != "model":
            continue
        name = str(node.get("name", unique_id))
        meta = (node.get("config") or {}).get("meta") or node.get("meta") or {}
        problems += _tag_problems(name, _tags(node), required=True)
        if not (meta.get("openmetadata") or {}).get("domain"):
            problems.append(f"{name}: no domain (meta.openmetadata.domain)")
        if not meta.get("owner"):
            problems.append(f"{name}: no owner (meta.owner)")
        for column_name, column in (node.get("columns") or {}).items():
            problems += _tag_problems(f"{name}.{column_name}", _tags(column), required=False)
    return problems


def check_manifest_file(path: Path) -> list[str]:
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read dbt manifest {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError(f"dbt manifest {path} is not a JSON object")
    return check_tags(raw)
