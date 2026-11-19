from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from datalake.cli import main
from datalake.transform.tag_check import check_tags


def _model(name: str, **extra: Any) -> dict[str, Any]:
    return {"resource_type": "model", "name": name, **extra}


GOOD = _model(
    "customer",
    tags=["internal"],
    config={"meta": {"owner": "sales-data", "openmetadata": {"domain": "sales"}}},
    columns={
        "email": {"name": "email", "config": {"tags": ["confidential"]}},
        "id": {"name": "id"},
    },
)


def _manifest(*models: dict[str, Any]) -> dict[str, Any]:
    nodes = {f"model.p.{m['name']}": m for m in models}
    nodes["test.p.t"] = {"resource_type": "test", "name": "t"}  # tests need no tags
    return {"nodes": nodes}


def test_a_properly_tagged_model_has_no_problems() -> None:
    assert check_tags(_manifest(GOOD)) == []


def test_untagged_model_lists_everything_missing() -> None:
    assert check_tags(_manifest(_model("orphan"))) == [
        "orphan: no classification tag (one of: public, internal, confidential, restricted)",
        "orphan: no domain (meta.openmetadata.domain)",
        "orphan: no owner (meta.owner)",
    ]


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"tags": ["internal", "restricted"]}, "customer: more than one classification tag"),
        ({"tags": ["internal", "nightly"]}, "customer: tag 'nightly' is not allowed"),
        (
            {"columns": {"email": {"name": "email", "tags": ["pii"]}}},
            "customer.email: tag 'pii' is not allowed",
        ),
        (
            {"columns": {"email": {"name": "email", "tags": ["public", "internal"]}}},
            "customer.email: more than one classification tag: internal, public",
        ),
        ({"config": {"meta": {"owner": "x"}}}, "customer: no domain"),
        ({"config": {"meta": {"openmetadata": {"domain": "sales"}}}}, "customer: no owner"),
    ],
)
def test_each_rule_is_reported(change: dict[str, Any], problem: str) -> None:
    problems = check_tags(_manifest({**GOOD, **change}))

    assert len(problems) == 1
    assert problems[0].startswith(problem)


def test_cli_passes_and_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    good = tmp_path / "good.json"
    good.write_text(json.dumps(_manifest(GOOD)), encoding="utf-8")
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(_manifest(GOOD, _model("orphan"))), encoding="utf-8")

    assert main(["check-tags", "--manifest", str(good)]) == 0
    assert "1 model(s) checked" in capsys.readouterr().out

    assert main(["check-tags", "--manifest", str(bad)]) == 1
    output = capsys.readouterr()
    assert "orphan: no domain" in output.out
    assert "3 problem(s)" in output.err

    assert main(["check-tags", "--manifest", str(tmp_path / "missing.json")]) == 1
