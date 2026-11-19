"""Loads sources.yml: the list of files the platform ingests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from datalake.core.errors import DatalakeError
from datalake.ingest.model import SourceDefinition
from datalake.ingest.naming import SAFE_NAME


class SourcesFileError(DatalakeError):
    """sources.yml is missing, malformed or inconsistent."""


class _Source(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    landing_prefix: str = Field(min_length=1)
    table: str
    format: Literal["csv", "excel", "parquet"]
    mode: Literal["append", "replace"] = "append"
    pattern: str = "*"
    delimiter: str = Field(default=",", min_length=1, max_length=1)
    encoding: str = "utf-8"
    sheet: str | None = None
    required_columns: list[str] = []

    @field_validator("name")
    @classmethod
    def _safe_name(cls, value: str) -> str:
        if not SAFE_NAME.fullmatch(value):
            raise ValueError("must be lower-case letters, digits and underscores")
        return value

    @field_validator("table")
    @classmethod
    def _schema_dot_table(cls, value: str) -> str:
        parts = value.split(".")
        if len(parts) != 2 or not all(SAFE_NAME.fullmatch(part) for part in parts):
            raise ValueError("must be <schema>.<table> in lower-case snake_case")
        return value

    @field_validator("landing_prefix")
    @classmethod
    def _folder(cls, value: str) -> str:
        if value.startswith("/") or not value.endswith("/"):
            raise ValueError("must be a folder path without a leading slash, ending in /")
        return value

    @model_validator(mode="after")
    def _sheet_is_excel_only(self) -> _Source:
        if self.sheet is not None and self.format != "excel":
            raise ValueError("`sheet` applies to excel sources only")
        return self


class _SourcesFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sources: list[_Source] = Field(min_length=1)


def parse_sources(text: str) -> list[SourceDefinition]:
    try:
        raw: Any = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise SourcesFileError(f"sources file is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise SourcesFileError("sources file must be a mapping with a top-level 'sources' key")
    try:
        parsed = _SourcesFile.model_validate(raw)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors()
        )
        raise SourcesFileError(f"sources file is invalid: {problems}") from exc

    names = [source.name for source in parsed.sources]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise SourcesFileError(f"source name(s) used more than once: {', '.join(repeated)}")

    return [
        SourceDefinition(
            name=source.name,
            landing_prefix=source.landing_prefix,
            table=source.table,
            format=source.format,
            mode=source.mode,
            pattern=source.pattern,
            delimiter=source.delimiter,
            encoding=source.encoding,
            sheet=source.sheet,
            required_columns=tuple(source.required_columns),
        )
        for source in parsed.sources
    ]


def load_sources(path: Path) -> list[SourceDefinition]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SourcesFileError(f"cannot read sources file {path}: {exc}") from exc
    return parse_sources(text)
