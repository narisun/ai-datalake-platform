from __future__ import annotations

from typing import Protocol

from datalake.ingest.model import IngestResult


class Ingestor(Protocol):
    """Loads one named source into the raw layer (dlt in production)."""

    async def ingest(self, source: str) -> IngestResult: ...
