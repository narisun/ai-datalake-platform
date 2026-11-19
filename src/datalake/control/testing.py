"""In-memory fakes for the control ports, shipped for tests of any module."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from datalake.control.errors import ProcessNotRegisteredError, SecretNotFoundError
from datalake.control.model import ConfigEntry, Process, RunRecord, RunStart, RunStatus

_PROCESS_NAME = re.compile(r"[a-z][a-z0-9-]*")
_CONFIG_KEY = re.compile(r"[a-z][a-z0-9_]*")


class InMemoryControlStore:
    def __init__(self) -> None:
        self._processes: dict[str, Process] = {}
        self._config: dict[tuple[int | None, str], str] = {}
        self._history: list[tuple[str, str, str | None, str | None, str]] = []
        self._runs: dict[str, dict[str, Any]] = {}
        self._steps: dict[int, dict[str, Any]] = {}
        self._watermarks: dict[tuple[int, str], str] = {}

    # Processes

    def find_process(self, name: str) -> Process | None:
        return self._processes.get(name)

    def register_process(self, name: str, description: str = "") -> Process:
        if not _PROCESS_NAME.fullmatch(name):
            raise ValueError(f"invalid process name {name!r}")
        existing = self._processes.get(name)
        process = Process(
            process_id=existing.process_id if existing else len(self._processes) + 1,
            name=name,
            description=description,
            is_enabled=existing.is_enabled if existing else True,
        )
        self._processes[name] = process
        return process

    def set_process_enabled(self, name: str, enabled: bool) -> None:
        process = self._processes.get(name)
        if process is None:
            raise ProcessNotRegisteredError(name)
        self._processes[name] = Process(
            process.process_id, process.name, process.description, enabled
        )

    def list_processes(self) -> Sequence[Process]:
        return [self._processes[name] for name in sorted(self._processes)]

    # Configuration

    def get_config(self, process_id: int) -> dict[str, str]:
        shared = {key: value for (owner, key), value in self._config.items() if owner is None}
        own = {key: value for (owner, key), value in self._config.items() if owner == process_id}
        return {**shared, **own}

    def list_config(self) -> Sequence[ConfigEntry]:
        names = {process.process_id: process.name for process in self._processes.values()}
        entries = [
            ConfigEntry(key, value, names[owner] if owner is not None else None)
            for (owner, key), value in self._config.items()
        ]
        return sorted(entries, key=lambda entry: (entry.process or "", entry.key))

    def set_config(self, key: str, value: str, process_id: int | None, actor: str) -> None:
        if not _CONFIG_KEY.fullmatch(key):
            raise ValueError(f"invalid configuration key {key!r}")
        old = self._config.get((process_id, key))
        if old is None:
            self._history.append((key, "insert", None, value, actor))
        elif old != value:
            self._history.append((key, "update", old, value, actor))
        self._config[(process_id, key)] = value

    def unset_config(self, key: str, process_id: int | None, actor: str) -> bool:
        old = self._config.pop((process_id, key), None)
        if old is None:
            return False
        self._history.append((key, "delete", old, None, actor))
        return True

    # Runs

    def start_run(self, run: RunStart) -> None:
        self._runs[run.run_id] = {
            "process_id": run.process_id,
            "status": "running",
            "started_at": run.started_at,
            "ended_at": None,
            "host": run.host,
            "git_commit": run.git_commit,
            "config_snapshot": {},
            "error": None,
            "airflow_dag_id": run.airflow_dag_id,
            "airflow_run_id": run.airflow_run_id,
            "airflow_task_id": run.airflow_task_id,
        }

    def save_config_snapshot(self, run_id: str, snapshot: Mapping[str, Mapping[str, str]]) -> None:
        self._runs[run_id]["config_snapshot"].update({k: dict(v) for k, v in snapshot.items()})

    def finish_run(
        self, run_id: str, status: RunStatus, error: str | None, ended_at: datetime
    ) -> None:
        self._runs[run_id].update(status=status, error=error, ended_at=ended_at)

    def start_step(self, run_id: str, name: str, started_at: datetime) -> int:
        step_run_id = len(self._steps) + 1
        self._steps[step_run_id] = {
            "run_id": run_id,
            "name": name,
            "status": "running",
            "rows_read": None,
            "rows_written": None,
            "error": None,
        }
        return step_run_id

    def finish_step(
        self,
        step_run_id: int,
        status: RunStatus,
        rows_read: int | None,
        rows_written: int | None,
        error: str | None,
        ended_at: datetime,
    ) -> None:
        self._steps[step_run_id].update(
            status=status, rows_read=rows_read, rows_written=rows_written, error=error
        )

    def list_runs(self, process: str | None, limit: int) -> Sequence[RunRecord]:
        names = {p.process_id: p.name for p in self._processes.values()}
        records = [
            RunRecord(
                run_id=run_id,
                process=names[run["process_id"]],
                status=run["status"],
                started_at=run["started_at"],
                ended_at=run["ended_at"],
                error=run["error"],
                airflow_run_id=run["airflow_run_id"],
            )
            for run_id, run in self._runs.items()
        ]
        if process is not None:
            records = [record for record in records if record.process == process]
        records.sort(key=lambda record: (record.started_at, record.run_id), reverse=True)
        return records[:limit]

    # Watermarks

    def get_watermark(self, process_id: int, object_name: str) -> str | None:
        return self._watermarks.get((process_id, object_name))

    def set_watermark(self, process_id: int, object_name: str, value: str) -> None:
        self._watermarks[(process_id, object_name)] = value

    # For tests

    def run_details(self, run_id: str) -> dict[str, Any]:
        run = self._runs.get(run_id)
        if run is None:
            return {}
        keys = (
            "status",
            "host",
            "git_commit",
            "config_snapshot",
            "error",
            "airflow_dag_id",
            "airflow_run_id",
            "airflow_task_id",
        )
        step_keys = ("name", "status", "rows_read", "rows_written", "error")
        return {
            **{key: run[key] for key in keys},
            "steps": [
                {key: step[key] for key in step_keys}
                for step in self._steps.values()
                if step["run_id"] == run_id
            ],
        }

    def config_history(self) -> list[tuple[str, str, str | None, str | None, str]]:
        return list(self._history)


class StaticSecrets:
    def __init__(self, secrets: Mapping[str, str] | None = None) -> None:
        self._secrets = dict(secrets or {})

    def get(self, name: str) -> str:
        try:
            return self._secrets[name]
        except KeyError:
            raise SecretNotFoundError(f"secret {name!r} not found") from None


class TickingClock:
    """Starts at a fixed time and advances one second per reading."""

    def __init__(self, start: datetime | None = None) -> None:
        self._next = start or datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        current = self._next
        self._next = current + timedelta(seconds=1)
        return current


class SequentialUuids:
    """Predictable, valid UUIDs: ...0001, ...0002, and so on."""

    def __init__(self) -> None:
        self._count = 0

    def new_id(self) -> str:
        self._count += 1
        return f"00000000-0000-7000-8000-{self._count:012d}"
