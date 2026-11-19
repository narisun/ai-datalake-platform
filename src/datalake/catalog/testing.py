from __future__ import annotations

from collections.abc import Sequence

from datalake.catalog.model import CatalogTable, GlossaryTerm


class InMemoryCatalogReader:
    """A catalog whose contents a test can change between reads."""

    def __init__(
        self,
        tables: Sequence[CatalogTable] = (),
        terms: Sequence[GlossaryTerm] = (),
    ) -> None:
        self.table_list = list(tables)
        self.term_list = list(terms)

    async def tables(self) -> Sequence[CatalogTable]:
        return list(self.table_list)

    async def glossary_terms(self) -> Sequence[GlossaryTerm]:
        return list(self.term_list)
