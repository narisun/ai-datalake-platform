"""Reads CSV, Excel and Parquet files into text rows.

Every value becomes text or null. Files in the real world carry dates as text, numbers
with stray spaces and mixed types in one column; the raw layer keeps exactly what
arrived, and the dbt staging models do the typing where it can be tested.
"""

# openpyxl and pyarrow ship no type information.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Sequence
from datetime import date, datetime, time
from decimal import Decimal

from datalake.core.errors import DatalakeError
from datalake.ingest.model import ParsedFile, SourceDefinition
from datalake.ingest.naming import normalize_columns


class FileRejectedError(DatalakeError):
    """The file cannot be loaded as it is. The message says why, for whoever sent it."""


def parse_file(source: SourceDefinition, data: bytes) -> ParsedFile:
    if not data:
        raise FileRejectedError("the file is empty")
    if source.format == "csv":
        header, rows = _read_csv(source, data)
    elif source.format == "excel":
        header, rows = _read_excel(source, data)
    else:
        header, rows = _read_parquet(data)

    columns = normalize_columns(header)
    missing = [name for name in source.required_columns if name not in columns]
    if missing:
        raise FileRejectedError(
            f"missing required column(s): {', '.join(missing)}; found: {', '.join(columns)}"
        )
    return ParsedFile(columns, tuple(rows))


def _text(value: object) -> str | None:
    """One cell as text. Empty cells are null."""
    if value is None:
        return None
    if isinstance(value, str):
        return value if value != "" else None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, float):
        # Excel stores whole numbers as floats: show 10, not 10.0.
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _shape(
    header: Sequence[object], records: Iterable[Sequence[object]], first_line: int
) -> tuple[Sequence[object], list[tuple[str | None, ...]]]:
    """Check each record against the header; skip blank ones; pad short ones."""
    if not header or all(cell is None or str(cell).strip() == "" for cell in header):
        raise FileRejectedError("the header row is empty")
    width = len(header)
    rows: list[tuple[str | None, ...]] = []
    for line, record in enumerate(records, start=first_line):
        cells = [_text(cell) for cell in record]
        if all(cell is None for cell in cells):
            continue
        if len(cells) > width:
            extra = [cell for cell in cells[width:] if cell is not None]
            if extra:
                raise FileRejectedError(
                    f"row {line} has {len(cells)} values but the header has {width} columns"
                )
            cells = cells[:width]
        cells += [None] * (width - len(cells))
        rows.append(tuple(cells))
    return header, rows


def _read_csv(
    source: SourceDefinition, data: bytes
) -> tuple[Sequence[object], list[tuple[str | None, ...]]]:
    # utf-8-sig also reads plain UTF-8 and drops the byte-order mark Excel adds.
    encoding = (
        "utf-8-sig" if source.encoding.lower().replace("_", "-") == "utf-8" else source.encoding
    )
    try:
        text = data.decode(encoding)
    except (UnicodeDecodeError, LookupError) as exc:
        raise FileRejectedError(f"the file is not valid {source.encoding}: {exc}") from exc
    try:
        records = [
            record
            for record in csv.reader(io.StringIO(text, newline=""), delimiter=source.delimiter)
        ]
    except csv.Error as exc:
        raise FileRejectedError(f"the file is not valid CSV: {exc}") from exc

    start = next((i for i, record in enumerate(records) if any(c.strip() for c in record)), None)
    if start is None:
        raise FileRejectedError("the file has no header row")
    return _shape(records[start], records[start + 1 :], first_line=start + 2)


def _read_excel(
    source: SourceDefinition, data: bytes
) -> tuple[Sequence[object], list[tuple[str | None, ...]]]:
    import openpyxl

    try:
        workbook = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    except Exception as exc:
        raise FileRejectedError(f"the file is not a readable Excel workbook: {exc}") from exc
    try:
        if source.sheet is None:
            sheet = workbook.worksheets[0]
        elif source.sheet in workbook.sheetnames:
            sheet = workbook[source.sheet]
        else:
            raise FileRejectedError(
                f"no sheet named {source.sheet!r}; found: {', '.join(workbook.sheetnames)}"
            )
        records = [tuple(row) for row in sheet.iter_rows(values_only=True)]
    finally:
        workbook.close()

    start = next(
        (i for i, record in enumerate(records) if any(cell is not None for cell in record)), None
    )
    if start is None:
        raise FileRejectedError("the sheet is empty")
    header = list(records[start])
    while header and header[-1] is None:  # trailing blank columns from formatting
        header.pop()
    return _shape(header, records[start + 1 :], first_line=start + 2)


def _read_parquet(data: bytes) -> tuple[Sequence[object], list[tuple[str | None, ...]]]:
    import pyarrow.parquet as parquet

    try:
        table = parquet.read_table(io.BytesIO(data))
    except Exception as exc:
        raise FileRejectedError(f"the file is not readable Parquet: {exc}") from exc
    columns = [table.column(name).to_pylist() for name in table.column_names]
    records = zip(*columns, strict=True) if columns else []
    return _shape(table.column_names, records, first_line=1)
