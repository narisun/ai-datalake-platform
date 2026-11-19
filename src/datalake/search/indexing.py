"""The `index` process: keep the agent's search index in step with the catalog."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel

from datalake.catalog.ports import CatalogReader
from datalake.control.service import Control
from datalake.search.chunks import build_chunks
from datalake.search.ports import ChunkStore

PROCESS = "index"


class SearchSettings(BaseModel):
    """What the index process and every search need to reach their services."""

    # The policy scopes what a role may find, and names schemas that are never indexed.
    access_policy_file: Path
    openmetadata_url: str
    openmetadata_token: str
    catalog_service: str = "lake_trino"
    opensearch_url: str
    search_index: str = "datalake-chunks"
    embed_url: str
    rerank_url: str


@dataclass(frozen=True, slots=True)
class IndexDependencies:
    catalog: CatalogReader
    store: ChunkStore
    hidden_schemas: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class IndexSummary:
    total: int
    written: int
    deleted: int
    unchanged: int


async def run_index(
    control: Control, build: Callable[[SearchSettings], IndexDependencies]
) -> IndexSummary:
    """Write new and changed chunks, and remove chunks whose source is gone.

    Removal matters as much as writing: a table that was dropped, retagged to a higher
    classification or moved out of a domain or into a hidden schema must stop being findable.
    """
    with control.start(PROCESS) as run:
        dependencies = build(run.config(SearchSettings))

        with run.step("read_catalog") as step:
            tables = await dependencies.catalog.tables()
            terms = await dependencies.catalog.glossary_terms()
            chunks = build_chunks(tables, terms, dependencies.hidden_schemas)
            step.rows_read = len(tables) + len(terms)

        with run.step("write_index") as step:
            existing = await dependencies.store.content_hashes()
            wanted = {chunk.chunk_id: chunk for chunk in chunks}
            changed = [
                chunk for chunk in chunks if existing.get(chunk.chunk_id) != chunk.content_hash
            ]
            gone = sorted(set(existing) - set(wanted))
            await dependencies.store.upsert(changed)
            await dependencies.store.delete(gone)
            step.rows_written = len(changed)

        return IndexSummary(
            total=len(chunks),
            written=len(changed),
            deleted=len(gone),
            unchanged=len(chunks) - len(changed),
        )
