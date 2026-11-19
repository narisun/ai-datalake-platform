"""Embedding and reranking through Hugging Face Text Embeddings Inference (TEI)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import httpx

from datalake.core.errors import DatalakeError
from datalake.search.model import Query, ScoredPassage, SearchHit

_BATCH = 16


class ModelServiceError(DatalakeError):
    """The embedding or reranking service refused or failed a request."""


async def _post(http: httpx.AsyncClient, url: str, body: dict[str, Any]) -> Any:
    try:
        response = await http.post(url, json=body)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise ModelServiceError(f"model service request to {url} failed: {exc}") from exc
    return response.json()


class TeiEmbedder:
    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self._http = http
        self._url = base_url.rstrip("/") + "/embed"

    async def embed(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        vectors: list[Sequence[float]] = []
        for start in range(0, len(texts), _BATCH):
            batch = list(texts[start : start + _BATCH])
            # Long catalog entries are cut to the model's limit, not rejected.
            vectors.extend(await _post(self._http, self._url, {"inputs": batch, "truncate": True}))
        return vectors

    async def dimensions(self) -> int:
        (vector,) = await self.embed(["dimension probe"])
        return len(vector)


class TeiReranker:
    """A cross-encoder: scores each passage against the query, together."""

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self._http = http
        self._url = base_url.rstrip("/") + "/rerank"

    async def rerank(
        self, query: Query, hits: Sequence[SearchHit], top_n: int
    ) -> Sequence[ScoredPassage]:
        if not hits:
            return []
        scored: list[tuple[int, float]] = []
        for start in range(0, len(hits), _BATCH):
            batch = hits[start : start + _BATCH]
            results = await _post(
                self._http,
                self._url,
                {"query": query.text, "texts": [h.chunk.text for h in batch], "truncate": True},
            )
            scored.extend((start + int(r["index"]), float(r["score"])) for r in results)
        scored.sort(key=lambda item: (-item[1], hits[item[0]].chunk.chunk_id))
        return [ScoredPassage(hits[index].chunk, score) for index, score in scored[:top_n]]
