from __future__ import annotations

from collections.abc import Sequence

from datalake.core.catalog import Dataset, LineageEdge, Term
from datalake.core.errors import NotFoundError


class InMemoryMetadataCatalog:
    def __init__(self) -> None:
        self._datasets: dict[str, Dataset] = {}
        self._terms: dict[str, Term] = {}
        self._edges: set[LineageEdge] = set()

    async def upsert_datasets(self, datasets: Sequence[Dataset]) -> None:
        for dataset in datasets:
            self._datasets[dataset.urn] = dataset

    async def upsert_terms(self, terms: Sequence[Term]) -> None:
        for term in terms:
            self._terms[term.name] = term

    async def add_lineage(self, edges: Sequence[LineageEdge]) -> None:
        self._edges.update(edges)

    async def get_dataset(self, urn: str) -> Dataset:
        try:
            return self._datasets[urn]
        except KeyError:
            raise NotFoundError(f"no dataset {urn}") from None

    async def list_datasets(self) -> Sequence[Dataset]:
        return [self._datasets[urn] for urn in sorted(self._datasets)]

    async def get_term(self, name: str) -> Term:
        try:
            return self._terms[name]
        except KeyError:
            raise NotFoundError(f"no glossary term {name!r}") from None

    async def upstream(self, urn: str) -> Sequence[str]:
        return sorted(e.upstream_urn for e in self._edges if e.downstream_urn == urn)

    async def downstream(self, urn: str) -> Sequence[str]:
        return sorted(e.downstream_urn for e in self._edges if e.upstream_urn == urn)
