"""The search index on OpenSearch: keyword (BM25) search, vector search, and writes.

One index holds each chunk's text, its vector, and the two fields access is decided on
(domain and classification). Every search carries the caller's scope as a filter, so
OpenSearch never returns a chunk the caller may not see.
"""

# Responses are untyped JSON, so unknown-type checks are relaxed for this module.
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from datalake.core.errors import DatalakeError
from datalake.core.security import Classification
from datalake.policy.model import SearchFilter
from datalake.search.model import Chunk, Query, SearchHit
from datalake.search.ports import Embedder

_SOURCE = ["entity_urn", "text", "domain", "classification", "content_hash"]


class SearchIndexError(DatalakeError):
    """OpenSearch refused or failed a request."""


def scope_filter(scope: SearchFilter | None) -> dict[str, Any] | None:
    """The caller's scope as an OpenSearch filter. None means no restriction at all.

    An empty scope becomes a filter nothing can match, never "no filter".
    """
    if scope is None:
        return None
    return {
        "bool": {
            "should": [
                {
                    "bool": {
                        "filter": [
                            {"term": {"domain": domain}},
                            {"range": {"classification": {"lte": int(ceiling)}}},
                        ]
                    }
                }
                for domain, ceiling in sorted(scope.scopes.items())
            ],
            "minimum_should_match": 1,
        }
    }


class OpenSearchChunkIndex:
    """Implements LexicalIndex, VectorIndex and ChunkStore over one index."""

    def __init__(
        self, http: httpx.AsyncClient, base_url: str, index: str, embedder: Embedder
    ) -> None:
        self._http = http
        self._url = f"{base_url.rstrip('/')}/{index}"
        self._bulk_url = f"{base_url.rstrip('/')}/_bulk"
        self._index = index
        self._embedder = embedder

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        try:
            response = await self._http.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise SearchIndexError(f"cannot reach the search index: {exc}") from exc
        if response.status_code >= 400 and response.status_code != 404:
            raise SearchIndexError(
                f"search index returned {response.status_code}: {response.text[:300]}"
            )
        return response

    async def ensure_index(self, dimensions: int) -> None:
        """Create the index if it does not exist."""
        if (await self._request("HEAD", self._url)).status_code == 200:
            return
        await self._request(
            "PUT",
            self._url,
            json={
                "settings": {
                    "index": {"knn": True, "number_of_shards": 1, "number_of_replicas": 0}
                },
                "mappings": {
                    "properties": {
                        "text": {"type": "text"},
                        "entity_urn": {"type": "keyword"},
                        "domain": {"type": "keyword"},
                        "classification": {"type": "integer"},
                        "content_hash": {"type": "keyword"},
                        "embedding": {
                            "type": "knn_vector",
                            "dimension": dimensions,
                            "method": {
                                "name": "hnsw",
                                "engine": "lucene",
                                "space_type": "cosinesimil",
                            },
                        },
                    }
                },
            },
        )

    # --- searching ---

    async def _hits(self, body: Mapping[str, Any]) -> list[SearchHit]:
        response = await self._request("POST", f"{self._url}/_search", json=body)
        if response.status_code == 404:
            return []  # nothing has been indexed yet
        hits: list[SearchHit] = []
        for hit in response.json()["hits"]["hits"]:
            source = hit["_source"]
            chunk = Chunk(
                chunk_id=hit["_id"],
                entity_urn=source["entity_urn"],
                text=source["text"],
                domain=source["domain"],
                classification=Classification(source["classification"]),
                content_hash=source.get("content_hash", ""),
            )
            hits.append(SearchHit(chunk, float(hit["_score"] or 0.0)))
        return hits

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]:
        """Keyword search (BM25)."""
        clause: dict[str, Any] = {"must": [{"match": {"text": query.text}}]}
        scope = scope_filter(query.scope)
        if scope is not None:
            clause["filter"] = [scope]
        return await self._hits({"size": top_k, "_source": _SOURCE, "query": {"bool": clause}})

    async def vector_search(self, query: Query, top_k: int) -> Sequence[SearchHit]:
        (vector,) = await self._embedder.embed([query.text])
        knn: dict[str, Any] = {"vector": list(vector), "k": top_k}
        scope = scope_filter(query.scope)
        if scope is not None:
            knn["filter"] = scope
        return await self._hits(
            {"size": top_k, "_source": _SOURCE, "query": {"knn": {"embedding": knn}}}
        )

    # --- writing ---

    async def _bulk(self, lines: list[dict[str, Any]]) -> None:
        if not lines:
            return
        payload = "\n".join(json.dumps(line) for line in lines) + "\n"
        response = await self._request(
            "POST",
            self._bulk_url,
            content=payload,
            params={"refresh": "true"},
            headers={"Content-Type": "application/x-ndjson"},
        )
        body = response.json()
        if body.get("errors"):
            failed = [
                item
                for item in body["items"]
                if next(iter(item.values())).get("status", 200) >= 400
            ]
            failed = [
                item
                for item in failed
                if not ("delete" in item and item["delete"].get("status") == 404)
            ]
            if failed:
                raise SearchIndexError(f"bulk write failed: {json.dumps(failed[0])[:300]}")

    async def upsert(self, chunks: Sequence[Chunk]) -> None:
        if not chunks:
            return
        vectors = await self._embedder.embed([chunk.text for chunk in chunks])
        await self.ensure_index(len(vectors[0]))
        lines: list[dict[str, Any]] = []
        for chunk, vector in zip(chunks, vectors, strict=True):
            lines.append({"index": {"_index": self._index, "_id": chunk.chunk_id}})
            lines.append(
                {
                    "entity_urn": chunk.entity_urn,
                    "text": chunk.text,
                    "domain": chunk.domain,
                    "classification": int(chunk.classification),
                    "content_hash": chunk.content_hash,
                    "embedding": list(vector),
                }
            )
        await self._bulk(lines)

    async def delete(self, chunk_ids: Sequence[str]) -> None:
        await self._bulk(
            [{"delete": {"_index": self._index, "_id": chunk_id}} for chunk_id in chunk_ids]
        )

    async def content_hashes(self) -> dict[str, str]:
        response = await self._request(
            "POST",
            f"{self._url}/_search",
            json={"size": 10_000, "_source": ["content_hash"], "query": {"match_all": {}}},
        )
        if response.status_code == 404:
            return {}
        return {
            hit["_id"]: hit["_source"].get("content_hash", "")
            for hit in response.json()["hits"]["hits"]
        }


class VectorSearch:
    """The vector side of the index, as its own VectorIndex.

    The retriever takes a keyword index and a vector index. Both live in one
    OpenSearch index here, so this adapts `vector_search` to the `search` it expects.
    """

    def __init__(self, index: OpenSearchChunkIndex) -> None:
        self._index = index

    async def search(self, query: Query, top_k: int) -> Sequence[SearchHit]:
        return await self._index.vector_search(query, top_k)
