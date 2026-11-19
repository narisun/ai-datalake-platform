from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from datalake.catalog.model import CatalogTable, GlossaryTerm


class CatalogReader(Protocol):
    """Reads the metadata catalog (OpenMetadata in production)."""

    async def tables(self) -> Sequence[CatalogTable]:
        """Every table in the lake, tagged or not."""
        ...

    async def glossary_terms(self) -> Sequence[GlossaryTerm]: ...
