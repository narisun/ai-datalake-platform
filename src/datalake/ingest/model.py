from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FileFormat = Literal["csv", "excel", "parquet"]
LoadMode = Literal["append", "replace"]
Outcome = Literal["loaded", "rejected", "superseded", "unchanged"]


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    """One kind of file to ingest, as declared in sources.yml.

    `append`: every file's rows are kept. A corrected re-upload of a file replaces
    that file's rows only.
    `replace`: each file is a full snapshot. The newest file replaces the whole table.
    """

    name: str
    landing_prefix: str
    table: str  # schema.table in the lake, for example raw.orders
    format: FileFormat
    mode: LoadMode = "append"
    pattern: str = "*"
    delimiter: str = ","
    encoding: str = "utf-8"
    sheet: str | None = None  # Excel only; None means the first sheet
    required_columns: tuple[str, ...] = ()

    @property
    def schema(self) -> str:
        return self.table.split(".")[0]

    @property
    def table_name(self) -> str:
        return self.table.split(".")[1]


@dataclass(frozen=True, slots=True)
class ParsedFile:
    """A file's contents as text. Typing is left to the staging models in dbt."""

    columns: tuple[str, ...]
    rows: tuple[tuple[str | None, ...], ...]


@dataclass(frozen=True, slots=True)
class FileOutcome:
    source: str
    path: str
    outcome: Outcome
    rows: int = 0
    error: str | None = None


@dataclass(frozen=True, slots=True)
class IngestSummary:
    outcomes: tuple[FileOutcome, ...]

    @property
    def rows_loaded(self) -> int:
        return sum(outcome.rows for outcome in self.outcomes if outcome.outcome == "loaded")

    def count(self, outcome: Outcome) -> int:
        return sum(1 for item in self.outcomes if item.outcome == outcome)
