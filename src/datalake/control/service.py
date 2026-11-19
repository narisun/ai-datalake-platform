"""The entry point every process uses: register check, configuration, run records."""

from __future__ import annotations

from collections.abc import Generator, Mapping
from contextlib import contextmanager

from pydantic import BaseModel

from datalake.control.config import ConfigResolver
from datalake.control.errors import ProcessDisabledError, ProcessNotRegisteredError
from datalake.control.model import FileStatus, IngestedFile, Process, RunStart
from datalake.control.ports import Clock, ControlStore, IdGenerator


class StepContext:
    """Counters a step may fill in; they are saved when the step ends."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.rows_read: int | None = None
        self.rows_written: int | None = None


class RunContext:
    """One execution of a registered process."""

    def __init__(
        self,
        run_id: str,
        process: Process,
        database_values: Mapping[str, str],
        store: ControlStore,
        resolver: ConfigResolver,
        clock: Clock,
    ) -> None:
        self.run_id = run_id
        self.process = process
        self._database_values = database_values
        self._store = store
        self._resolver = resolver
        self._clock = clock

    def config[T: BaseModel](self, model: type[T]) -> T:
        """This run's configuration as a typed object. Raises ConfigError naming bad keys.

        The values were read once, when the run started, so a change made to the
        database during the run does not affect it.
        """
        settings, snapshot = self._resolver.resolve(model, self._database_values)
        self._store.save_config_snapshot(self.run_id, snapshot)
        return settings

    @contextmanager
    def step(self, name: str) -> Generator[StepContext]:
        """Record one step. An exception marks the step failed and is re-raised."""
        step_run_id = self._store.start_step(self.run_id, name, self._clock.now())
        context = StepContext(name)
        try:
            yield context
        except BaseException as exc:
            self._store.finish_step(
                step_run_id,
                "failed",
                context.rows_read,
                context.rows_written,
                _describe(exc),
                self._clock.now(),
            )
            raise
        self._store.finish_step(
            step_run_id,
            "succeeded",
            context.rows_read,
            context.rows_written,
            None,
            self._clock.now(),
        )

    def get_watermark(self, object_name: str) -> str | None:
        return self._store.get_watermark(self.process.process_id, object_name)

    def set_watermark(self, object_name: str, value: str) -> None:
        """Call only after the work up to `value` has succeeded."""
        self._store.set_watermark(self.process.process_id, object_name, value)

    def ingested_file(self, source: str, path: str) -> IngestedFile | None:
        """What an earlier run recorded for this file, if anything."""
        return self._store.get_ingested_file(self.process.process_id, source, path)

    def record_file(
        self,
        source: str,
        path: str,
        content_hash: str,
        status: FileStatus,
        rows_loaded: int | None = None,
        error: str | None = None,
    ) -> None:
        self._store.record_ingested_file(
            self.process.process_id,
            IngestedFile(source, path, content_hash, status, rows_loaded, error, self.run_id),
        )


class Control:
    def __init__(
        self,
        store: ControlStore,
        resolver: ConfigResolver,
        clock: Clock,
        ids: IdGenerator,
        environment: Mapping[str, str],
        host: str = "",
    ) -> None:
        self._store = store
        self._resolver = resolver
        self._clock = clock
        self._ids = ids
        self._environment = environment
        self._host = host

    @contextmanager
    def start(self, process_name: str) -> Generator[RunContext]:
        """Begin a run of a registered process.

        Raises ProcessNotRegisteredError or ProcessDisabledError before anything is
        recorded. Otherwise the run is marked succeeded when the block ends, or failed
        if it raises.
        """
        process = self._store.find_process(process_name)
        if process is None:
            raise ProcessNotRegisteredError(process_name)
        if not process.is_enabled:
            raise ProcessDisabledError(process_name)

        run_id = self._ids.new_id()
        self._store.start_run(
            RunStart(
                run_id=run_id,
                process_id=process.process_id,
                started_at=self._clock.now(),
                host=self._host,
                git_commit=self._environment.get("DL_GIT_COMMIT", ""),
                # Airflow sets these for every task it runs.
                airflow_dag_id=self._environment.get("AIRFLOW_CTX_DAG_ID"),
                airflow_run_id=self._environment.get("AIRFLOW_CTX_DAG_RUN_ID"),
                airflow_task_id=self._environment.get("AIRFLOW_CTX_TASK_ID"),
            )
        )
        context = RunContext(
            run_id=run_id,
            process=process,
            database_values=self._store.get_config(process.process_id),
            store=self._store,
            resolver=self._resolver,
            clock=self._clock,
        )
        try:
            yield context
        except BaseException as exc:
            self._store.finish_run(run_id, "failed", _describe(exc), self._clock.now())
            raise
        self._store.finish_run(run_id, "succeeded", None, self._clock.now())


def _describe(exc: BaseException) -> str:
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__
