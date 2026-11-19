from __future__ import annotations

import math
import re
import zlib
from collections.abc import Sequence

from datalake.search.model import Chunk, Query, ScoredPassage, SearchHit
from datalake.search.ports import Embedder

_TOKEN = re.compile(r"\w+")


def _tokens(text: str) -> list[str]:
    return _TOKEN.findall(text.lower())


def _top(hits: list[SearchHit], top_k: int) -> list[SearchHit]:
    hits.sort(key=lambda hit: (-hit.score, hit.chunk.chunk_id))
    return hits[:top_k]


class _ChunkMemory:
    def __init__(self) -> None:
        self._chunks: dict[str, Chunk] = {}

    async def upsert(self, chunks: Sequence[Chunk]) -> None:
        for chunk in chunks:
            self._chunks[chunk.chunk_id] = chunk

    async def delete(self, chunk_ids: Sequence[str]) -> None:
        for chunk_id in chunk_ids:
            self._chunks.pop(chunk_id, None)

    async def content_hashes(self) -> dict[str, str]:
        return {chunk_id: chunk.content_hash for chunk_id, chunk in self._chunks.items()}


class InMemoryLexicalIndex(_ChunkMemory):
    """Keyword search by term overlap. Not BM25, but ranks the same way for simple tests."""

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]:
        wanted = set(_tokens(query.text))
        hits: list[SearchHit] = []
        for chunk in self._chunks.values():
            if not query.permits(chunk):
                continue
            score = float(sum(1 for token in _tokens(chunk.text) if token in wanted))
            if score > 0:
                hits.append(SearchHit(chunk, score))
        return _top(hits, top_k)


class HashingEmbedder:
    """Deterministic bag-of-words vectors. Texts sharing words get similar vectors."""

    def __init__(self, dimensions: int = 64) -> None:
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = dimensions

    async def embed(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        vector = [0.0] * self._dimensions
        for token in _tokens(text):
            vector[zlib.crc32(token.encode()) % self._dimensions] += 1.0
        norm = math.sqrt(sum(value * value for value in vector))
        return [value / norm for value in vector] if norm else vector


class InMemoryVectorIndex(_ChunkMemory):
    """Cosine-similarity search over vectors from the injected embedder."""

    def __init__(self, embedder: Embedder) -> None:
        super().__init__()
        self._embedder = embedder
        self._vectors: dict[str, Sequence[float]] = {}

    async def upsert(self, chunks: Sequence[Chunk]) -> None:
        await super().upsert(chunks)
        vectors = await self._embedder.embed([chunk.text for chunk in chunks])
        for chunk, vector in zip(chunks, vectors, strict=True):
            self._vectors[chunk.chunk_id] = vector

    async def delete(self, chunk_ids: Sequence[str]) -> None:
        await super().delete(chunk_ids)
        for chunk_id in chunk_ids:
            self._vectors.pop(chunk_id, None)

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]:
        (query_vector,) = await self._embedder.embed([query.text])
        hits: list[SearchHit] = []
        for chunk in self._chunks.values():
            if not query.permits(chunk):
                continue
            vector = self._vectors[chunk.chunk_id]
            score = sum(a * b for a, b in zip(query_vector, vector, strict=True))
            if score > 0:
                hits.append(SearchHit(chunk, score))
        return _top(hits, top_k)


class PassThroughReranker:
    """Keeps the incoming order and records what it was asked to rerank."""

    def __init__(self) -> None:
        self.seen: list[list[str]] = []

    async def rerank(
        self, query: Query, hits: Sequence[SearchHit], top_n: int
    ) -> Sequence[ScoredPassage]:
        self.seen.append([hit.chunk.chunk_id for hit in hits])
        return [ScoredPassage(hit.chunk, hit.score) for hit in hits[:top_n]]
