from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from datalake.search.model import Chunk, Query, ScoredPassage, SearchHit


class LexicalIndex(Protocol):
    """Keyword (BM25) search. Must honour `query.scope`."""

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]: ...


class VectorIndex(Protocol):
    """Embedding search. Must honour `query.scope`."""

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]: ...


class ChunkStore(Protocol):
    """Write side of the search index."""

    async def upsert(self, chunks: Sequence[Chunk]) -> None: ...

    async def delete(self, chunk_ids: Sequence[str]) -> None: ...

    async def content_hashes(self) -> dict[str, str]:
        """chunk_id -> content hash, used to skip unchanged chunks."""
        ...


class Embedder(Protocol):
    async def embed(self, texts: Sequence[str]) -> Sequence[Sequence[float]]: ...


class Reranker(Protocol):
    """Cross-encoder: rescore hits against the query and keep the best `top_n`."""

    async def rerank(
        self, query: Query, hits: Sequence[SearchHit], top_n: int
    ) -> Sequence[ScoredPassage]: ...
