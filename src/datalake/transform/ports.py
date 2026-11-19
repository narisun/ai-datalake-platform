from __future__ import annotations

from typing import Protocol

from datalake.transform.model import RunResult


class TransformEngine(Protocol):
    """Builds models (dbt in production). `select` of None builds everything."""

    async def build(self, select: str | None = None) -> RunResult: ...
