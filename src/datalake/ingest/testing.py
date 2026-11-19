from __future__ import annotations

from collections.abc import Mapping

from datalake.core.errors import NotFoundError
from datalake.ingest.model import IngestResult


class FakeIngestor:
    """Returns a canned result per source and records what was asked for."""

    def __init__(self, results: Mapping[str, IngestResult] | None = None) -> None:
        self._results = dict(results or {})
        self.ingested: list[str] = []

    async def ingest(self, source: str) -> IngestResult:
        self.ingested.append(source)
        try:
            return self._results[source]
        except KeyError:
            raise NotFoundError(f"unknown source {source!r}") from None
