from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

Row = dict[str, object]


class InMemoryRawTableWriter:
    """Keeps raw tables in memory, with the same column rules as the real writer."""

    METADATA = ("_source_file", "_row_number", "_loaded_at", "_run_id")

    def __init__(self) -> None:
        self.columns: dict[str, list[str]] = {}
        self.rows: dict[str, list[Row]] = {}

    def table(self, schema: str, table: str) -> list[Row]:
        return self.rows.get(f"{schema}.{table}", [])

    async def ensure_table(self, schema: str, table: str, columns: Sequence[str]) -> None:
        key = f"{schema}.{table}"
        existing = self.columns.setdefault(key, list(self.METADATA))
        self.rows.setdefault(key, [])
        for column in columns:
            if column not in existing:
                existing.append(column)
                for row in self.rows[key]:  # as in a real table: old rows read as null
                    row[column] = None

    async def delete_file(self, schema: str, table: str, source_file: str) -> None:
        key = f"{schema}.{table}"
        self.rows[key] = [row for row in self.rows[key] if row["_source_file"] != source_file]

    async def truncate(self, schema: str, table: str) -> None:
        self.rows[f"{schema}.{table}"] = []

    async def insert(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[str | None]],
        source_file: str,
        run_id: str,
        loaded_at: datetime,
    ) -> None:
        key = f"{schema}.{table}"
        unknown = [column for column in columns if column not in self.columns[key]]
        if unknown:
            raise AssertionError(f"insert into {key} names columns never ensured: {unknown}")
        for number, row in enumerate(rows, start=1):
            record: Row = {column: None for column in self.columns[key]}
            record.update(zip(columns, row, strict=True))
            record.update(
                _source_file=source_file, _row_number=number, _loaded_at=loaded_at, _run_id=run_id
            )
            self.rows[key].append(record)
