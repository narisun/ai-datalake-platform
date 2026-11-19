from __future__ import annotations

from collections.abc import Sequence

from datalake.search.model import Chunk, SearchHit


def rrf(*rankings: Sequence[SearchHit], k: int = 60) -> list[SearchHit]:
    """Reciprocal Rank Fusion: score(d) = sum over rankings of 1 / (k + rank(d)).

    Ranks start at 1. A chunk repeated inside one ranking counts once, at its best rank.
    Ties are broken by chunk id so the result is deterministic.
    """
    if k <= 0:
        raise ValueError("k must be positive")

    scores: dict[str, float] = {}
    chunks: dict[str, Chunk] = {}
    for ranking in rankings:
        seen: set[str] = set()
        for hit in ranking:
            chunk_id = hit.chunk.chunk_id
            if chunk_id in seen:
                continue
            seen.add(chunk_id)
            scores[chunk_id] = scores.get(chunk_id, 0.0) + 1.0 / (k + len(seen))
            chunks.setdefault(chunk_id, hit.chunk)

    ordered = sorted(scores, key=lambda chunk_id: (-scores[chunk_id], chunk_id))
    return [SearchHit(chunks[chunk_id], scores[chunk_id]) for chunk_id in ordered]
