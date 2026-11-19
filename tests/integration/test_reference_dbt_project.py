"""Runs real dbt-core against the reference project. No warehouse is needed to parse."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from datalake.cli import main
from datalake.core.security import Classification
from datalake.transform.dbt_core_runner import DbtCoreRunner
from datalake.transform.dbt_manifest import read_manifest

PROJECT = Path(__file__).parents[2] / "dbt"


@pytest.fixture(scope="module")
def manifest_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    work = tmp_path_factory.mktemp("dbt")
    runner = DbtCoreRunner(
        project_dir=PROJECT,
        profiles_dir=PROJECT,
        target_dir=work / "target",
        log_dir=work / "logs",
    )
    result = asyncio.run(runner.parse())
    assert result.success
    return Path(result.artifacts_dir) / "manifest.json"


async def test_reference_project_parses_and_every_model_is_tagged(manifest_path: Path) -> None:
    manifest = read_manifest(manifest_path)
    models = {d.name: d for d in manifest.datasets if d.urn.startswith("model.")}

    assert set(models) == {"stg_orders", "stg_customers", "dim_customer", "fct_orders"}
    assert all(model.domain == "sales" for model in models.values())
    assert models["stg_customers"].classification is Classification.CONFIDENTIAL
    email = next(c for c in models["dim_customer"].columns if c.name == "email")
    assert email.classification is Classification.CONFIDENTIAL


async def test_lineage_runs_from_sources_to_marts(manifest_path: Path) -> None:
    edges = {(e.upstream_urn, e.downstream_urn) for e in read_manifest(manifest_path).lineage}

    assert ("source.datalake.raw.orders", "model.datalake.stg_orders") in edges
    assert ("model.datalake.stg_orders", "model.datalake.fct_orders") in edges
    assert ("model.datalake.dim_customer", "model.datalake.fct_orders") in edges


async def test_cli_generates_trino_rules_for_the_reference_project(
    manifest_path: Path, tmp_path: Path
) -> None:
    out = tmp_path / "rules.json"

    code = main(
        [
            "policy",
            "trino-rules",
            "--policy",
            str(PROJECT / "access.yml"),
            "--manifest",
            str(manifest_path),
            "--out",
            str(out),
        ]
    )

    assert code == 0
    tables = json.loads(out.read_text(encoding="utf-8"))["tables"]
    analyst = {r["table"]: r for r in tables if r.get("user") == "role_analyst_sales"}
    # Confidential staging table is out of reach; email and name are hidden in the mart.
    assert set(analyst) == {"stg_orders", "dim_customer", "fct_orders"}
    assert analyst["dim_customer"]["columns"] == [
        {"name": "full_name", "allow": False},
        {"name": "email", "allow": False},
    ]
