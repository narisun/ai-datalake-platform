"""Reads dbt's manifest.json into domain datasets and lineage.

Only the artifact is read, never dbt's Python API, so this works with any dbt version
that writes a manifest.

The tags read here are the same ones OpenMetadata's dbt ingestion reads, so the access
policy and the catalog cannot drift apart:

- classification: the dbt tag `public`, `internal`, `confidential` or `restricted`
- domain: `meta.openmetadata.domain`
- owner: `meta.owner`

Safe defaults: a model with no domain gets an empty one, which no role can match, and
a model with no classification tag is treated as restricted. Data is therefore hidden
until someone tags it.
"""

# The manifest is untyped JSON, so unknown-type checks are relaxed for this module.
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datalake.core.catalog import Column, Dataset, LineageEdge
from datalake.core.errors import DatalakeError
from datalake.core.security import Classification

_DATASET_TYPES = {"model", "source", "seed", "snapshot"}
_CLASSIFICATION_TAGS = {member.name.lower() for member in Classification}


class ManifestError(DatalakeError):
    """The dbt manifest is missing, unreadable or carries an invalid tag."""


@dataclass(frozen=True, slots=True)
class DbtManifest:
    datasets: tuple[Dataset, ...]
    lineage: tuple[LineageEdge, ...]


def read_manifest(path: Path) -> DbtManifest:
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read dbt manifest {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError(f"dbt manifest {path} is not a JSON object")
    return parse_manifest(raw)


def parse_manifest(raw: Mapping[str, Any]) -> DbtManifest:
    entries: dict[str, Any] = {**raw.get("nodes", {}), **raw.get("sources", {})}
    nodes = {
        unique_id: node
        for unique_id, node in entries.items()
        if node.get("resource_type") in _DATASET_TYPES
    }

    datasets = tuple(_dataset(unique_id, nodes[unique_id]) for unique_id in sorted(nodes))
    lineage = tuple(
        LineageEdge(upstream, unique_id)
        for unique_id in sorted(nodes)
        for upstream in sorted(nodes[unique_id].get("depends_on", {}).get("nodes", []))
        if upstream in nodes
    )
    return DbtManifest(datasets, lineage)


def _meta(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    """dbt keeps meta under `config.meta` (newer) and `meta` (older); prefer the newer."""
    return (entry.get("config") or {}).get("meta") or entry.get("meta") or {}


def _tags(entry: Mapping[str, Any]) -> set[str]:
    """dbt keeps tags under `tags` and `config.tags`, depending on version and level."""
    config_tags = (entry.get("config") or {}).get("tags") or []
    if isinstance(config_tags, str):
        config_tags = [config_tags]
    return {str(tag).lower() for tag in [*(entry.get("tags") or []), *config_tags]}


def _classification(
    entry: Mapping[str, Any], default: Classification, where: str
) -> Classification:
    found = sorted(_tags(entry) & _CLASSIFICATION_TAGS)
    if not found:
        return default
    if len(found) > 1:
        raise ManifestError(f"{where}: more than one classification tag: {', '.join(found)}")
    return Classification.parse(found[0])


def _dataset(unique_id: str, node: Mapping[str, Any]) -> Dataset:
    meta = _meta(node)
    classification = _classification(node, Classification.RESTRICTED, unique_id)
    columns = tuple(
        Column(
            name=str(column.get("name", name)),
            data_type=str(column.get("data_type") or ""),
            description=str(column.get("description") or ""),
            # A column is at least as sensitive as its table.
            classification=max(
                classification,
                _classification(column, classification, f"{unique_id}.{name}"),
            ),
        )
        for name, column in (node.get("columns") or {}).items()
    )
    return Dataset(
        urn=unique_id,
        name=str(node.get("name", "")),
        domain=str((meta.get("openmetadata") or {}).get("domain") or ""),
        description=str(node.get("description") or ""),
        owner=str(meta.get("owner") or ""),
        classification=classification,
        columns=columns,
        schema=str(node.get("schema") or ""),
    )
