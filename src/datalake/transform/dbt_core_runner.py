"""TransformEngine port on top of dbt-core 1.x, run in-process through dbtRunner."""

# dbt ships no type stubs, so this module opts out of strict checks on dbt's types.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from dbt.cli.main import dbtRunner

from datalake.core.errors import DatalakeError
from datalake.transform.model import RunResult

_BUILT = {"success", "pass"}


class DbtRunError(DatalakeError):
    """dbt could not run at all (bad project, bad profile, no connection)."""


class DbtCoreRunner:
    """Runs dbt against one project. dbt is not re-entrant, so calls are serialised."""

    def __init__(
        self,
        project_dir: Path,
        profiles_dir: Path,
        target_dir: Path,
        log_dir: Path,
        target: str | None = None,
    ) -> None:
        self._project_dir = project_dir
        self._profiles_dir = profiles_dir
        self._target_dir = target_dir
        self._log_dir = log_dir
        self._target = target
        self._lock = asyncio.Lock()

    async def build(self, select: str | None = None) -> RunResult:
        """Run and test models. Model failures give success=False, not an exception."""
        args = ["build"]
        if select is not None:
            args += ["--select", select]
        return await self._invoke(args)

    async def parse(self) -> RunResult:
        """Write manifest.json without connecting to the warehouse."""
        return await self._invoke(["parse"])

    async def _invoke(self, command: list[str]) -> RunResult:
        args = [
            *command,
            "--project-dir",
            str(self._project_dir),
            "--profiles-dir",
            str(self._profiles_dir),
            "--target-path",
            str(self._target_dir),
            "--log-path",
            str(self._log_dir),
            "--no-use-colors",
            "--quiet",
        ]
        if self._target is not None:
            args += ["--target", self._target]

        async with self._lock:
            outcome = await asyncio.to_thread(dbtRunner().invoke, args)

        if outcome.exception is not None:
            raise DbtRunError(
                f"dbt {command[0]} failed: {outcome.exception}"
            ) from outcome.exception
        return RunResult(
            success=bool(outcome.success),
            models_built=_models_built(outcome.result),
            artifacts_dir=str(self._target_dir),
        )


def _models_built(result: Any) -> tuple[str, ...]:
    """Names of models that ran successfully. `parse` has no per-model results."""
    names: list[str] = []
    for item in getattr(result, "results", None) or ():
        node = getattr(item, "node", None)
        if getattr(node, "resource_type", None) != "model":
            continue
        if str(getattr(item, "status", "")).lower() in _BUILT:
            names.append(str(getattr(node, "name", "")))
    return tuple(names)
