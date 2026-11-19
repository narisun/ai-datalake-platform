from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RunResult:
    """Outcome of a transformation run. `artifacts_dir` holds the dbt artifacts."""

    success: bool
    models_built: tuple[str, ...]
    artifacts_dir: str
