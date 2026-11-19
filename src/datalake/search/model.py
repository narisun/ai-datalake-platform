from __future__ import annotations

from dataclasses import dataclass, replace

from datalake.core.security import Classification
from datalake.policy.model import SearchFilter


@dataclass(frozen=True, slots=True)
class Chunk:
    """One indexed piece of text describing a catalog entity."""

    chunk_id: str
    entity_urn: str
    text: str
    domain: str
    classification: Classification
    content_hash: str = ""


@dataclass(frozen=True, slots=True)
class Query:
    """A search request. `scope` of None means unfiltered; services always set it."""

    text: str
    scope: SearchFilter | None = None

    def scoped_to(self, scope: SearchFilter) -> Query:
        return replace(self, scope=scope)

    def permits(self, chunk: Chunk) -> bool:
        return self.scope is None or self.scope.allows(chunk.domain, chunk.classification)


@dataclass(frozen=True, slots=True)
class SearchHit:
    chunk: Chunk
    score: float


@dataclass(frozen=True, slots=True)
class ScoredPassage:
    """A hit after reranking, ready to be used as context."""

    chunk: Chunk
    score: float
