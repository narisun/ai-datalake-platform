"""The `transform` process: build the dbt project as a registered, recorded run."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel

from datalake.control.service import Control
from datalake.core.errors import DatalakeError
from datalake.transform.model import RunResult
from datalake.transform.ports import TransformEngine

PROCESS = "transform"


class TransformFailedError(DatalakeError):
    """dbt ran, but a model or a test failed."""


class TransformSettings(BaseModel):
    dbt_project_dir: Path
    dbt_target_dir: Path
    dbt_log_dir: Path
    dbt_profiles_dir: Path | None = None  # defaults to the project directory
    trino_host: str
    trino_port: int = 8080
    trino_scheme: str = "http"
    trino_user: str = "svc_pipeline"
    trino_password: str | None = None  # set where Trino requires sign-in
    trino_ca_file: Path | None = None  # the certificate to trust, if not a public one
    trino_catalog: str = "lake"

    def dbt_environment(self) -> dict[str, str]:
        """The variables the project's profiles.yml reads with env_var()."""
        environment = {
            "TRINO_HOST": self.trino_host,
            "TRINO_PORT": str(self.trino_port),
            "TRINO_SCHEME": self.trino_scheme,
            "TRINO_USER": self.trino_user,
            "TRINO_CATALOG": self.trino_catalog,
        }
        if self.trino_password is not None:
            environment["TRINO_PASSWORD"] = self.trino_password
        if self.trino_ca_file is not None:
            environment["TRINO_CERT"] = str(self.trino_ca_file)
        return environment


class DocsEngine(TransformEngine, Protocol):
    """A transform engine that can also write the catalog artifact."""

    async def docs(self) -> RunResult: ...

    async def freshness(self) -> RunResult: ...


EngineFactory = Callable[[TransformSettings, str], DocsEngine]


async def run_transform(control: Control, engine_factory: EngineFactory) -> RunResult:
    """Generate docs, check source freshness, then build and test every model.

    Docs come first because `dbt docs generate` overwrites run_results.json, and the
    catalog needs the test results from `build`.

    A stale source does not stop the build: what did arrive is still published. But
    the run then ends as failed, so someone is told.
    """
    with control.start(PROCESS) as run:
        settings = run.config(TransformSettings)
        engine = engine_factory(settings, run.run_id)

        with run.step("docs"):
            docs = await engine.docs()
            if not docs.success:
                raise TransformFailedError(_explain("dbt docs generate failed", docs))

        with run.step("freshness") as step:
            freshness = await engine.freshness()
            step.rows_read = len(freshness.problems)

        with run.step("build") as step:
            result = await engine.build()
            step.rows_written = len(result.models_built)
            if not result.success:
                raise TransformFailedError(_explain("dbt build failed", result))

        if freshness.problems:
            raise TransformFailedError(
                _explain("models were built, but sources are stale", freshness)
            )
        return result


def _explain(summary: str, result: RunResult) -> str:
    if not result.problems:
        return f"{summary}; see run_results.json in {result.artifacts_dir}"
    return f"{summary}: " + "; ".join(result.problems)
