from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from datalake.core.catalog import LineageEdge
from datalake.core.security import Classification
from datalake.transform.dbt_manifest import (
    ManifestError,
    parse_manifest,
    read_manifest,
)


def _model(name: str, depends_on: list[str] | None = None, **extra: Any) -> dict[str, Any]:
    return {
        "resource_type": "model",
        "name": name,
        "schema": "marts",
        "depends_on": {"nodes": depends_on or []},
        **extra,
    }


def _om(domain: str, **meta: Any) -> dict[str, Any]:
    return {"meta": {"openmetadata": {"domain": domain}, **meta}}


MANIFEST: dict[str, Any] = {
    "nodes": {
        "model.p.dim_customer": _model(
            "dim_customer",
            ["model.p.stg_customers"],
            description="One row per customer.",
            tags=["internal"],
            config=_om("sales", owner="team"),
            columns={
                "customer_id": {"name": "customer_id", "data_type": "varchar"},
                "email": {
                    "name": "email",
                    "description": "Email.",
                    "config": {"tags": ["confidential"]},
                },
            },
        ),
        # Older dbt keeps meta at the top level; no classification tag at all.
        "model.p.stg_customers": _model(
            "stg_customers", ["source.p.raw.customers"], **_om("sales")
        ),
        "test.p.not_null": {"resource_type": "test", "name": "not_null"},
    },
    "sources": {
        "source.p.raw.customers": {"resource_type": "source", "name": "customers", "schema": "raw"}
    },
}


def test_reads_models_and_sources_but_not_tests() -> None:
    manifest = parse_manifest(MANIFEST)

    assert [d.urn for d in manifest.datasets] == [
        "model.p.dim_customer",
        "model.p.stg_customers",
        "source.p.raw.customers",
    ]


def test_reads_domain_owner_and_classification() -> None:
    dim = parse_manifest(MANIFEST).datasets[0]

    assert (dim.name, dim.schema, dim.domain, dim.owner) == (
        "dim_customer",
        "marts",
        "sales",
        "team",
    )
    assert dim.description == "One row per customer."
    assert dim.classification is Classification.INTERNAL


def test_column_inherits_or_raises_the_table_classification() -> None:
    columns = {c.name: c for c in parse_manifest(MANIFEST).datasets[0].columns}

    assert columns["customer_id"].classification is Classification.INTERNAL
    assert columns["customer_id"].data_type == "varchar"
    assert columns["email"].classification is Classification.CONFIDENTIAL


def test_column_cannot_be_less_sensitive_than_its_table() -> None:
    raw = {
        "nodes": {
            "model.p.m": _model(
                "m",
                tags=["confidential"],
                columns={"c": {"name": "c", "tags": ["public"]}},
            )
        }
    }

    assert parse_manifest(raw).datasets[0].columns[0].classification is Classification.CONFIDENTIAL


def test_classification_is_read_from_tags_or_config_tags_in_any_case() -> None:
    raw = {
        "nodes": {
            "model.p.a": _model("a", tags=["Internal"]),
            "model.p.b": _model("b", config={"tags": ["public"]}),
            "model.p.c": _model("c", config={"tags": "confidential"}),
        }
    }

    found = [d.classification for d in parse_manifest(raw).datasets]

    assert found == [Classification.INTERNAL, Classification.PUBLIC, Classification.CONFIDENTIAL]


def test_other_tags_are_ignored() -> None:
    raw = {"nodes": {"model.p.m": _model("m", tags=["nightly", "internal", "finance"])}}

    assert parse_manifest(raw).datasets[0].classification is Classification.INTERNAL


def test_untagged_data_is_hidden_by_default() -> None:
    by_urn = {d.urn: d for d in parse_manifest(MANIFEST).datasets}

    assert by_urn["model.p.stg_customers"].domain == "sales"
    assert by_urn["model.p.stg_customers"].classification is Classification.RESTRICTED
    # No tags at all: no domain, so no role can match it.
    assert by_urn["source.p.raw.customers"].domain == ""
    assert by_urn["source.p.raw.customers"].classification is Classification.RESTRICTED


def test_lineage_follows_depends_on_between_datasets() -> None:
    assert parse_manifest(MANIFEST).lineage == (
        LineageEdge("model.p.stg_customers", "model.p.dim_customer"),
        LineageEdge("source.p.raw.customers", "model.p.stg_customers"),
    )


def test_two_classification_tags_is_an_error_naming_the_model() -> None:
    raw = {"nodes": {"model.p.m": _model("m", tags=["internal", "restricted"])}}

    with pytest.raises(ManifestError, match=r"model\.p\.m: more than one classification"):
        parse_manifest(raw)


def test_reads_from_a_file(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(MANIFEST), encoding="utf-8")

    assert len(read_manifest(path).datasets) == 3


@pytest.mark.parametrize("content", [None, "{not json", "[1, 2]"])
def test_unreadable_manifest_is_an_error(tmp_path: Path, content: str | None) -> None:
    path = tmp_path / "manifest.json"
    if content is not None:
        path.write_text(content, encoding="utf-8")

    with pytest.raises(ManifestError):
        read_manifest(path)
