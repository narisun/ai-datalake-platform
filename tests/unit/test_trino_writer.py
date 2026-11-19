"""The SQL the Trino writer sends. The real Trino is exercised by the dev stack."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from datalake.ingest.trino_writer import TrinoRawTableWriter, UnsafeNameError

LOADED_AT = datetime(2026, 11, 19, 2, 0, 5, 123456, tzinfo=UTC)


class RecordingWriter(TrinoRawTableWriter):
    """Captures SQL instead of sending it."""

    def __init__(self, existing_columns: list[str] | None = None) -> None:
        super().__init__(host="trino", port=8080, user="svc_pipeline", catalog="lake")
        self.sql: list[str] = []
        self._existing = existing_columns or []

    def _execute(self, sql: str) -> list[Any]:
        self.sql.append(sql)
        if "information_schema.columns" in sql:
            return [[name] for name in self._existing]
        return []


async def test_ensure_table_creates_schema_table_and_missing_columns() -> None:
    writer = RecordingWriter(existing_columns=["_source_file", "order_id"])

    await writer.ensure_table("raw", "orders", ["order_id", "amount"])

    assert writer.sql[0] == 'create schema if not exists "lake"."raw"'
    assert writer.sql[1] == (
        'create table if not exists "lake"."raw"."orders" ("_source_file" varchar, '
        '"_row_number" bigint, "_loaded_at" timestamp(6) with time zone, "_run_id" varchar)'
    )
    assert "table_schema = 'raw' and table_name = 'orders'" in writer.sql[2]
    # order_id is already there; only amount is added, as text.
    assert writer.sql[3:] == ['alter table "lake"."raw"."orders" add column "amount" varchar']


async def test_insert_quotes_values_and_adds_load_metadata() -> None:
    writer = RecordingWriter()

    await writer.insert(
        "raw",
        "customers",
        ["full_name", "note"],
        [("Grace O'Brien", None), ("Zoë; DROP TABLE x --", "line1\nline2")],
        source_file="shop/customers/c.xlsx",
        run_id="run-1",
        loaded_at=LOADED_AT,
    )

    (sql,) = writer.sql
    assert sql.startswith(
        'insert into "lake"."raw"."customers" '
        '("full_name", "note", "_source_file", "_row_number", "_loaded_at", "_run_id") values '
    )
    assert (
        "('Grace O''Brien', NULL, 'shop/customers/c.xlsx', 1, "
        "timestamp '2026-11-19 02:00:05.123456 UTC', 'run-1')"
    ) in sql
    assert "('Zoë; DROP TABLE x --', 'line1\nline2', 'shop/customers/c.xlsx', 2, " in sql


async def test_insert_with_no_rows_sends_nothing() -> None:
    writer = RecordingWriter()

    await writer.insert("raw", "t", ["a"], [], "f.csv", "run-1", LOADED_AT)

    assert writer.sql == []


async def test_large_loads_are_split_into_batches_with_continuous_row_numbers() -> None:
    writer = RecordingWriter()
    rows = [(str(n),) for n in range(2500)]

    await writer.insert("raw", "t", ["a"], rows, "f.csv", "run-1", LOADED_AT)

    assert len(writer.sql) == 3
    assert "'0', 'f.csv', 1," in writer.sql[0]
    assert "'1000', 'f.csv', 1001," in writer.sql[1]
    assert "'2499', 'f.csv', 2500," in writer.sql[2]


async def test_delete_and_truncate() -> None:
    writer = RecordingWriter()

    await writer.delete_file("raw", "orders", "shop/o'1.csv")
    await writer.truncate("raw", "orders")

    assert writer.sql == [
        'delete from "lake"."raw"."orders" where "_source_file" = \'shop/o\'\'1.csv\'',
        'delete from "lake"."raw"."orders"',
    ]


@pytest.mark.parametrize("name", ['bad"name', "Raw", "has space", "1st", "x;drop"])
async def test_unsafe_names_never_reach_sql(name: str) -> None:
    writer = RecordingWriter()

    with pytest.raises(UnsafeNameError):
        await writer.ensure_table(name, "t", ["a"])
    with pytest.raises(UnsafeNameError):
        await writer.insert("raw", "t", [name], [("v",)], "f.csv", "run-1", LOADED_AT)
    assert writer.sql == []


def test_sign_in_options_reach_the_trino_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import trino

    calls: list[dict[str, Any]] = []

    class FakeCursor:
        def execute(self, sql: str) -> None:
            pass

        def fetchall(self) -> list[Any]:
            return []

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            pass

    def fake_connect(**kwargs: Any) -> FakeConnection:
        calls.append(kwargs)
        return FakeConnection()

    monkeypatch.setattr(trino.dbapi, "connect", fake_connect)

    open_writer = TrinoRawTableWriter("trino", 8080, "svc_pipeline", "lake")
    open_writer._execute("select 1")  # pyright: ignore[reportPrivateUsage]
    signed_in = TrinoRawTableWriter(
        "trino", 8443, "svc_pipeline", "lake", scheme="https", password="pw", ca_file="/tls/ca.crt"
    )
    signed_in._execute("select 1")  # pyright: ignore[reportPrivateUsage]

    assert "auth" not in calls[0]
    assert "verify" not in calls[0]
    assert calls[1]["http_scheme"] == "https"
    assert calls[1]["verify"] == "/tls/ca.crt"
    assert isinstance(calls[1]["auth"], trino.auth.BasicAuthentication)
