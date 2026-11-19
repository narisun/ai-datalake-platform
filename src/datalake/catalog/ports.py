from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from datalake.core.catalog import Dataset, LineageEdge, Term


class MetadataCatalog(Protocol):
    """The metadata system of record (OpenMetadata in production)."""

    async def upsert_datasets(self, datasets: Sequence[Dataset]) -> None: ...

    async def upsert_terms(self, terms: Sequence[Term]) -> None: ...

    async def add_lineage(self, edges: Sequence[LineageEdge]) -> None: ...

    async def get_dataset(self, urn: str) -> Dataset:
        """Raise NotFoundError if the dataset is unknown."""
        ...

    async def list_datasets(self) -> Sequence[Dataset]: ...

    async def get_term(self, name: str) -> Term:
        """Raise NotFoundError if the term is unknown."""
        ...

    async def upstream(self, urn: str) -> Sequence[str]:
        """URNs this dataset is built from, one hop."""
        ...

    async def downstream(self, urn: str) -> Sequence[str]:
        """URNs built from this dataset, one hop."""
        ...
