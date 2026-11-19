from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RunResult:
    """Outcome of a transformation run. `artifacts_dir` holds the dbt artifacts."""

    success: bool
    models_built: tuple[str, ...]
    artifacts_dir: str
    # One line per model, test or source that failed, for the person reading the run.
    problems: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
