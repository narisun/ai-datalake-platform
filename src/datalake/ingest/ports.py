from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol


class RawTableWriter(Protocol):
    """Writes text rows into raw lake tables (through Trino in production).

    Every table has the file's own columns, all text, plus four the platform adds:
    `_source_file`, `_row_number`, `_loaded_at` and `_run_id`.
    """

    async def ensure_table(self, schema: str, table: str, columns: Sequence[str]) -> None:
        """Create the schema and table if missing, and add any column not yet there.

        Columns are never dropped or retyped, so a file with fewer columns still loads.
        """
        ...

    async def delete_file(self, schema: str, table: str, source_file: str) -> None:
        """Remove every row that came from this file. Removing none is not an error."""
        ...

    async def truncate(self, schema: str, table: str) -> None: ...

    async def insert(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[str | None]],
        source_file: str,
        run_id: str,
        loaded_at: datetime,
    ) -> None: ...
