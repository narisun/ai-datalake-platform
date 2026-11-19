from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IngestResult:
    source: str
    tables_loaded: tuple[str, ...]
    rows_loaded: int
