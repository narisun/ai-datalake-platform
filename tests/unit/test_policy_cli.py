from __future__ import annotations

from pathlib import Path

import pytest

from datalake.cli import main
from datalake.core.security import Principal
from datalake.policy.policy_file import load_access_rules
from datalake.policy.service import RoleAccessPolicy
from datalake.search.retriever import HybridRetriever
from datalake.search.testing import (
    HashingEmbedder,
    InMemoryLexicalIndex,
    InMemoryVectorIndex,
    PassThroughReranker,
)
from tests.unit.helpers import chunk

POLICY = """
roles:
  analyst_sales:
    domains: [sales]
    max_classification: internal
"""


@pytest.fixture
def policy_path(tmp_path: Path) -> Path:
    path = tmp_path / "access.yml"
    path.write_text(POLICY, encoding="utf-8")
    return path


async def test_policy_file_drives_what_search_returns(policy_path: Path) -> None:
    lexical = InMemoryLexicalIndex()
    vector = InMemoryVectorIndex(HashingEmbedder())
    for index in (lexical, vector):
        await index.upsert([chunk("orders", "revenue by order"), chunk("pay", "revenue", "hr")])
    retriever = HybridRetriever(
        lexical=lexical,
        vector=vector,
        reranker=PassThroughReranker(),
        policy=RoleAccessPolicy(load_access_rules(policy_path)),
    )

    found = await retriever.retrieve("revenue", Principal("alice", frozenset({"analyst_sales"})))

    assert [p.chunk.chunk_id for p in found] == ["orders"]
    assert await retriever.retrieve("revenue", Principal("bob")) == []


def test_cli_validates_a_good_policy(policy_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["policy", "validate", str(policy_path)]) == 0
    out = capsys.readouterr().out
    assert "valid, 1 role(s)" in out
    assert "analyst_sales: domains [sales], up to internal, 0 mask(s)" in out


def test_cli_reports_a_bad_policy(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bad = tmp_path / "access.yml"
    bad.write_text("roles: {}", encoding="utf-8")

    assert main(["policy", "validate", str(bad)]) == 1
    assert "error: access policy is invalid" in capsys.readouterr().err
