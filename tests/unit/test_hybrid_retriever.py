from __future__ import annotations

from collections.abc import Sequence

import pytest

from datalake.core.security import Classification, Principal
from datalake.policy.testing import StaticAccessPolicy
from datalake.search.model import Chunk, Query, SearchHit
from datalake.search.retriever import HybridRetriever
from datalake.search.testing import (
    HashingEmbedder,
    InMemoryLexicalIndex,
    InMemoryVectorIndex,
    PassThroughReranker,
)
from tests.unit.helpers import chunk

ALICE = Principal("alice", frozenset({"analyst_sales"}))

CHUNKS = [
    chunk("orders", "monthly revenue by customer order", domain="sales"),
    chunk("customers", "customer master data", domain="sales"),
    chunk("margins", "revenue margin detail", "sales", Classification.CONFIDENTIAL),
    chunk("salaries", "employee salary and revenue share", domain="hr"),
]


class LeakyIndex:
    """An index that ignores the query scope, to prove the retriever re-checks."""

    def __init__(self, chunks: Sequence[Chunk]) -> None:
        self._chunks = chunks
        self.queries: list[Query] = []

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]:
        self.queries.append(query)
        return [SearchHit(c, 1.0) for c in self._chunks][:top_k]


async def _retriever(
    scopes: dict[str, Classification] | None,
    reranker: PassThroughReranker | None = None,
    final_n: int = 8,
) -> HybridRetriever:
    lexical = InMemoryLexicalIndex()
    vector = InMemoryVectorIndex(HashingEmbedder())
    await lexical.upsert(CHUNKS)
    await vector.upsert(CHUNKS)
    return HybridRetriever(
        lexical=lexical,
        vector=vector,
        reranker=reranker or PassThroughReranker(),
        policy=StaticAccessPolicy(scopes),
        final_n=final_n,
    )


async def test_finds_matching_chunks_in_the_callers_scope() -> None:
    retriever = await _retriever({"sales": Classification.INTERNAL})

    passages = await retriever.retrieve("revenue", ALICE)

    assert [p.chunk.chunk_id for p in passages] == ["orders"]


async def test_higher_ceiling_reveals_more() -> None:
    retriever = await _retriever({"sales": Classification.CONFIDENTIAL})

    passages = await retriever.retrieve("revenue", ALICE)

    assert {p.chunk.chunk_id for p in passages} == {"orders", "margins"}


async def test_other_domains_are_never_returned() -> None:
    retriever = await _retriever({"sales": Classification.RESTRICTED})

    passages = await retriever.retrieve("salary revenue", ALICE)

    assert "salaries" not in {p.chunk.chunk_id for p in passages}


async def test_caller_with_no_access_gets_nothing_and_no_index_is_queried() -> None:
    leaky = LeakyIndex(CHUNKS)
    reranker = PassThroughReranker()
    retriever = HybridRetriever(leaky, leaky, reranker, StaticAccessPolicy())

    assert await retriever.retrieve("revenue", ALICE) == []
    assert leaky.queries == []
    assert reranker.seen == []


async def test_hits_outside_the_scope_never_reach_the_reranker() -> None:
    leaky = LeakyIndex(CHUNKS)
    reranker = PassThroughReranker()
    retriever = HybridRetriever(
        leaky, leaky, reranker, StaticAccessPolicy({"sales": Classification.INTERNAL})
    )

    passages = await retriever.retrieve("revenue", ALICE)

    assert reranker.seen == [["orders", "customers"]]
    assert {p.chunk.chunk_id for p in passages} == {"customers", "orders"}
    assert all(q.scope is not None for q in leaky.queries)


async def test_final_n_limits_the_result() -> None:
    retriever = await _retriever({"sales": Classification.RESTRICTED}, final_n=1)

    assert len(await retriever.retrieve("revenue customer", ALICE)) == 1


async def test_no_match_skips_the_reranker() -> None:
    reranker = PassThroughReranker()
    retriever = await _retriever({"sales": Classification.INTERNAL}, reranker)

    assert await retriever.retrieve("zzzz", ALICE) == []
    assert reranker.seen == []


@pytest.mark.parametrize("bad", [{"candidate_k": 0}, {"final_n": 0}, {"rrf_k": -1}])
def test_rejects_non_positive_sizes(bad: dict[str, int]) -> None:
    index = InMemoryLexicalIndex()
    with pytest.raises(ValueError, match="positive"):
        HybridRetriever(index, index, PassThroughReranker(), StaticAccessPolicy(), **bad)
