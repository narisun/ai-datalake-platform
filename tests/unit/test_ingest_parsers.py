"""File parsing, with the kinds of mess real files arrive in."""

# pyarrow ships no type information.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import io
from dataclasses import replace
from datetime import date, datetime
from typing import Any

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datalake.ingest.model import FileFormat, SourceDefinition
from datalake.ingest.naming import normalize_columns
from datalake.ingest.parsers import FileRejectedError, parse_file
from datalake.ingest.sources_file import SourcesFileError, parse_sources

BASE = SourceDefinition(
    name="orders", landing_prefix="shop/orders/", table="raw.orders", format="csv"
)


def _source(fmt: FileFormat = "csv", **overrides: Any) -> SourceDefinition:
    return replace(BASE, format=fmt, **overrides)


def _xlsx(rows: list[list[object]], sheet: str = "Sheet1", extra_sheet: str | None = None) -> bytes:
    workbook = openpyxl.Workbook()
    active = workbook.active
    assert active is not None
    active.title = sheet
    for row in rows:
        active.append(row)
    if extra_sheet:
        workbook.create_sheet(extra_sheet).append(["other"])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


# --- column names ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (
            ["Order ID", "Customer-Name", "  Amount (GBP) "],
            ("order_id", "customer_name", "amount_gbp"),
        ),
        (["amount", "Amount", "AMOUNT"], ("amount", "amount_2", "amount_3")),
        (["2024 Sales", "", None, "ünïcödé"], ("c_2024_sales", "column_2", "column_3", "n_c_d")),
        (["_source_file", "__x__"], ("source_file", "x")),
    ],
)
def test_headers_become_safe_unique_names(headers: list[object], expected: tuple[str, ...]) -> None:
    assert normalize_columns(headers) == expected


# --- CSV ------------------------------------------------------------------------------


def test_csv_values_stay_text_and_blanks_become_null() -> None:
    parsed = parse_file(_source(), b"Order ID,Amount,Note\n10,120.50,\n11, 80 ,late\n")

    assert parsed.columns == ("order_id", "amount", "note")
    assert parsed.rows == (("10", "120.50", None), ("11", " 80 ", "late"))


def test_csv_from_excel_with_bom_quotes_commas_and_line_breaks() -> None:
    data = '﻿id,address\r\n1,"12 High St, Leeds"\r\n2,"Flat 3\nKing Rd"\r\n'.encode()

    parsed = parse_file(_source(), data)

    assert parsed.columns == ("id", "address")
    assert parsed.rows == (("1", "12 High St, Leeds"), ("2", "Flat 3\nKing Rd"))


def test_csv_with_semicolons_and_windows_encoding() -> None:
    data = "id;name\n1;Zoë Müller\n".encode("cp1252")

    parsed = parse_file(_source(delimiter=";", encoding="cp1252"), data)

    assert parsed.rows == (("1", "Zoë Müller"),)


def test_csv_blank_lines_are_skipped_and_short_rows_padded() -> None:
    parsed = parse_file(_source(), b"\n\nid,name,country\n1,Ada\n\n,,\n2,Alan,GB\n")

    assert parsed.rows == (("1", "Ada", None), ("2", "Alan", "GB"))


def test_csv_trailing_empty_values_beyond_the_header_are_ignored() -> None:
    parsed = parse_file(_source(), b"id,name\n1,Ada,,\n")

    assert parsed.rows == (("1", "Ada"),)


def test_csv_header_only_is_a_valid_empty_file() -> None:
    assert parse_file(_source(), b"id,name\n").rows == ()


@pytest.mark.parametrize(
    ("data", "kwargs", "message"),
    [
        (b"", {}, "the file is empty"),
        (b"\n\n  \n", {}, "no header row"),
        (b"id,name\n1,Ada,extra\n", {}, "row 2 has 3 values but the header has 2"),
        ("id,name\n1,Zoë\n".encode("cp1252"), {}, "not valid utf-8"),
        (b"id,name\n1,Ada\n", {"required_columns": ("id", "amount")}, "missing required column"),
        (b"id,name\n1,Ada\n", {"encoding": "no-such-encoding"}, "not valid no-such-encoding"),
    ],
)
def test_csv_problems_are_rejected_with_a_reason(
    data: bytes, kwargs: dict[str, Any], message: str
) -> None:
    with pytest.raises(FileRejectedError, match=message):
        parse_file(_source(**kwargs), data)


# --- Excel ----------------------------------------------------------------------------


def test_excel_cells_become_readable_text() -> None:
    data = _xlsx(
        [
            ["Customer ID", "Joined", "Signed Up", "Spend", "Active", None],
            [1, date(2026, 1, 5), datetime(2026, 1, 5, 9, 30), 120.5, True, None],
            [2.0, None, None, 80, False, None],
        ]
    )

    parsed = parse_file(_source("excel"), data)

    assert parsed.columns == ("customer_id", "joined", "signed_up", "spend", "active")
    assert parsed.rows == (
        ("1", "2026-01-05 00:00:00", "2026-01-05 09:30:00", "120.5", "true"),
        ("2", None, None, "80", "false"),
    )


def test_excel_title_rows_above_the_header_are_not_supported_but_blank_ones_are() -> None:
    data = _xlsx([[None, None], ["id", "name"], [1, "Ada"], [None, None], [2, "Alan"]])

    assert parse_file(_source("excel"), data).rows == (("1", "Ada"), ("2", "Alan"))


def test_excel_named_sheet() -> None:
    data = _xlsx([["id"], [1]], sheet="Data", extra_sheet="Notes")

    assert parse_file(_source("excel", sheet="Data"), data).rows == (("1",),)
    with pytest.raises(FileRejectedError, match="no sheet named 'Missing'; found: Data, Notes"):
        parse_file(_source("excel", sheet="Missing"), data)


def test_excel_garbage_is_rejected() -> None:
    with pytest.raises(FileRejectedError, match="not a readable Excel workbook"):
        parse_file(_source("excel"), b"this is not a workbook")


# --- Parquet --------------------------------------------------------------------------


def test_parquet_typed_values_become_text() -> None:
    table = pa.table(
        {
            "Product ID": [1, 2],
            "price": [9.99, None],
            "added": [datetime(2026, 1, 5, 9, 30), datetime(2026, 2, 1)],
        }
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    parsed = parse_file(_source("parquet"), buffer.getvalue())

    assert parsed.columns == ("product_id", "price", "added")
    assert parsed.rows == (
        ("1", "9.99", "2026-01-05 09:30:00"),
        ("2", None, "2026-02-01 00:00:00"),
    )


def test_parquet_garbage_is_rejected() -> None:
    with pytest.raises(FileRejectedError, match="not readable Parquet"):
        parse_file(_source("parquet"), b"PAR1 but not really")


# --- sources.yml ----------------------------------------------------------------------

SOURCES = """
sources:
  - name: orders
    landing_prefix: shop/orders/
    pattern: "orders_*.csv"
    table: raw.orders
    format: csv
    required_columns: [order_id]
  - name: customers
    landing_prefix: shop/customers/
    table: raw.customers
    format: excel
    mode: replace
    sheet: Customers
"""


def test_sources_file_is_read_with_defaults() -> None:
    orders, customers = parse_sources(SOURCES)

    assert (orders.name, orders.schema, orders.table_name) == ("orders", "raw", "orders")
    assert (orders.mode, orders.delimiter, orders.encoding) == ("append", ",", "utf-8")
    assert orders.required_columns == ("order_id",)
    assert (customers.mode, customers.sheet, customers.pattern) == ("replace", "Customers", "*")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("table: raw.orders", "table: orders"), "<schema>.<table>"),
        (("table: raw.orders", "table: Raw.Orders"), "<schema>.<table>"),
        (("landing_prefix: shop/orders/", "landing_prefix: /shop/orders"), "folder path"),
        (("format: csv", "format: json"), "format"),
        (("name: customers", "name: orders"), "used more than once"),
        (("format: csv", "format: csv\n    sheet: X"), "excel sources only"),
        (("required_columns: [order_id]", "colour: blue"), "colour"),
    ],
)
def test_invalid_sources_files_are_rejected(change: tuple[str, str], message: str) -> None:
    with pytest.raises(SourcesFileError, match=message):
        parse_sources(SOURCES.replace(*change))


def test_sources_file_must_be_a_mapping_with_sources() -> None:
    for text in ("- a\n- b", "sources: []", "sources: ["):
        with pytest.raises(SourcesFileError):
            parse_sources(text)
