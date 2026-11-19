from __future__ import annotations

from datalake.core.security import Classification
from datalake.search.model import Chunk, SearchHit


def chunk(
    chunk_id: str,
    text: str = "",
    domain: str = "sales",
    classification: Classification = Classification.INTERNAL,
) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        entity_urn=f"urn:{chunk_id}",
        text=text or chunk_id,
        domain=domain,
        classification=classification,
    )


def hit(chunk_id: str, score: float = 1.0) -> SearchHit:
    return SearchHit(chunk(chunk_id), score)
