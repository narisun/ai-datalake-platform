from __future__ import annotations

import asyncio
from collections.abc import Sequence

from datalake.core.security import Principal
from datalake.policy.ports import AccessPolicy
from datalake.search.model import Query, ScoredPassage
from datalake.search.ports import LexicalIndex, Reranker, VectorIndex
from datalake.search.rrf import rrf


class HybridRetriever:
    """BM25 + vector search, fused with RRF, then reranked.

    Every search is scoped to what the caller may see before it reaches an index.
    """

    def __init__(
        self,
        lexical: LexicalIndex,
        vector: VectorIndex,
        reranker: Reranker,
        policy: AccessPolicy,
        candidate_k: int = 50,
        final_n: int = 8,
        rrf_k: int = 60,
    ) -> None:
        if candidate_k <= 0 or final_n <= 0 or rrf_k <= 0:
            raise ValueError("candidate_k, final_n and rrf_k must be positive")
        self._lexical = lexical
        self._vector = vector
        self._reranker = reranker
        self._policy = policy
        self._candidate_k = candidate_k
        self._final_n = final_n
        self._rrf_k = rrf_k

    async def retrieve(self, text: str, principal: Principal) -> Sequence[ScoredPassage]:
        scope = self._policy.search_filter(principal)
        if scope.denies_everything:
            return []

        query = Query(text).scoped_to(scope)
        lexical, vector = await asyncio.gather(
            self._lexical.search(query, self._candidate_k),
            self._vector.search(query, self._candidate_k),
        )
        # Defence in depth: drop anything an index returned outside the scope.
        fused = [hit for hit in rrf(lexical, vector, k=self._rrf_k) if query.permits(hit.chunk)]
        if not fused:
            return []
        return await self._reranker.rerank(query, fused, self._final_n)
