"""RawTableWriter on Trino.

All lake writes go through Trino, so it is the only component that needs access to
lake storage. Rows are sent as SQL literals in batches, which suits the small files
this platform ingests; it is not a bulk-load path.
"""

# The trino client ships no type information.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import trino

from datalake.core.errors import DatalakeError
from datalake.ingest.naming import SAFE_NAME

_METADATA = (
    ("_source_file", "varchar"),
    ("_row_number", "bigint"),
    ("_loaded_at", "timestamp(6) with time zone"),
    ("_run_id", "varchar"),
)
_MAX_BATCH_ROWS = 1000
_MAX_BATCH_CHARS = 400_000  # well under Trino's default query length limit


class UnsafeNameError(DatalakeError):
    """A schema, table or column name that must not be put into SQL."""


def _identifier(name: str) -> str:
    if not SAFE_NAME.fullmatch(name):
        raise UnsafeNameError(f"unsafe identifier: {name!r}")
    return f'"{name}"'


def _literal(value: str | None) -> str:
    """A Trino string literal. Standard SQL strings have one escape: a doubled quote."""
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


class TrinoRawTableWriter:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        catalog: str,
        scheme: str = "http",
        password: str | None = None,
        ca_file: str | None = None,
    ) -> None:
        """With a password, signs in over HTTPS; `ca_file` is the certificate to trust."""
        self._host = host
        self._port = port
        self._user = user
        self._scheme = scheme
        self._password = password
        self._ca_file = ca_file
        self._catalog = _identifier(catalog)
        self._catalog_name = catalog

    def _execute(self, sql: str) -> list[Any]:
        options: dict[str, Any] = {}
        if self._password is not None:
            options["auth"] = trino.auth.BasicAuthentication(self._user, self._password)
        if self._ca_file is not None:
            options["verify"] = self._ca_file
        connection = trino.dbapi.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            http_scheme=self._scheme,
            **options,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            return list(cursor.fetchall())
        finally:
            connection.close()

    async def _run(self, sql: str) -> list[Any]:
        return await asyncio.to_thread(self._execute, sql)

    def _table(self, schema: str, table: str) -> str:
        return f"{self._catalog}.{_identifier(schema)}.{_identifier(table)}"

    async def ensure_table(self, schema: str, table: str, columns: Sequence[str]) -> None:
        target = self._table(schema, table)
        await self._run(f"create schema if not exists {self._catalog}.{_identifier(schema)}")
        metadata = ", ".join(f'"{name}" {kind}' for name, kind in _METADATA)
        await self._run(f"create table if not exists {target} ({metadata})")

        existing = {
            row[0]
            for row in await self._run(
                f"select column_name from {self._catalog}.information_schema.columns "
                f"where table_schema = {_literal(schema)} and table_name = {_literal(table)}"
            )
        }
        for column in columns:
            if column not in existing:
                await self._run(f"alter table {target} add column {_identifier(column)} varchar")

    async def delete_file(self, schema: str, table: str, source_file: str) -> None:
        await self._run(
            f"delete from {self._table(schema, table)} "
            f'where "_source_file" = {_literal(source_file)}'
        )

    async def truncate(self, schema: str, table: str) -> None:
        await self._run(f"delete from {self._table(schema, table)}")

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
        if not rows:
            return
        names = ", ".join(
            [*(_identifier(column) for column in columns), *(f'"{name}"' for name, _ in _METADATA)]
        )
        stamp = loaded_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        tail = f"{_literal(source_file)}, {{row}}, timestamp '{stamp} UTC', {_literal(run_id)}"
        prefix = f"insert into {self._table(schema, table)} ({names}) values "

        batch: list[str] = []
        size = 0
        for number, row in enumerate(rows, start=1):
            values = ", ".join(_literal(value) for value in row)
            text = (
                f"({values}, {tail.format(row=number)})"
                if values
                else f"({tail.format(row=number)})"
            )
            if batch and (len(batch) >= _MAX_BATCH_ROWS or size + len(text) > _MAX_BATCH_CHARS):
                await self._run(prefix + ", ".join(batch))
                batch, size = [], 0
            batch.append(text)
            size += len(text)
        if batch:
            await self._run(prefix + ", ".join(batch))
