from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from datalake.search.rrf import rrf
from tests.unit.helpers import hit

_ids = st.lists(st.sampled_from("abcdefgh"), unique=True, max_size=8)


def test_scores_follow_the_formula() -> None:
    fused = rrf([hit("a"), hit("b")], [hit("b"), hit("c")], k=60)

    scores = {h.chunk.chunk_id: h.score for h in fused}
    assert scores["a"] == pytest.approx(1 / 61)
    assert scores["b"] == pytest.approx(1 / 62 + 1 / 61)
    assert scores["c"] == pytest.approx(1 / 62)
    assert [h.chunk.chunk_id for h in fused] == ["b", "a", "c"]


def test_ties_are_broken_by_chunk_id() -> None:
    fused = rrf([hit("z")], [hit("a")])

    assert [h.chunk.chunk_id for h in fused] == ["a", "z"]


def test_duplicate_in_one_ranking_counts_once() -> None:
    fused = rrf([hit("a"), hit("a"), hit("b")], k=60)

    scores = {h.chunk.chunk_id: h.score for h in fused}
    assert scores["a"] == pytest.approx(1 / 61)
    assert scores["b"] == pytest.approx(1 / 62)


def test_no_rankings_gives_no_hits() -> None:
    assert rrf() == []
    assert rrf([], []) == []


def test_k_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        rrf([hit("a")], k=0)


@given(first=_ids, second=_ids)
def test_result_is_the_union_sorted_by_score(first: list[str], second: list[str]) -> None:
    fused = rrf([hit(i) for i in first], [hit(i) for i in second])

    assert {h.chunk.chunk_id for h in fused} == set(first) | set(second)
    assert len(fused) == len(set(first) | set(second))
    scores = [h.score for h in fused]
    assert scores == sorted(scores, reverse=True)


@given(ids=_ids.filter(bool))
def test_top_of_every_ranking_is_top_of_the_result(ids: list[str]) -> None:
    ranking = [hit(i) for i in ids]

    fused = rrf(ranking, ranking, ranking)

    assert fused[0].chunk.chunk_id == ids[0]
    assert [h.chunk.chunk_id for h in fused] == ids
