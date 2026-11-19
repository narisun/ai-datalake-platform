from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

RunStatus = Literal["running", "succeeded", "failed"]
FileStatus = Literal["loaded", "rejected", "superseded"]


@dataclass(frozen=True, slots=True)
class Process:
    """A registered process. Only registered, enabled processes may run."""

    process_id: int
    name: str
    description: str
    is_enabled: bool


@dataclass(frozen=True, slots=True)
class ConfigEntry:
    """One configuration value. `process` of None means shared by every process."""

    key: str
    value: str
    process: str | None


@dataclass(frozen=True, slots=True)
class RunStart:
    """Everything recorded when a run begins."""

    run_id: str
    process_id: int
    started_at: datetime
    host: str = ""
    git_commit: str = ""
    airflow_dag_id: str | None = None
    airflow_run_id: str | None = None
    airflow_task_id: str | None = None


@dataclass(frozen=True, slots=True)
class RunRecord:
    """A run as shown to an operator."""

    run_id: str
    process: str
    status: RunStatus
    started_at: datetime
    ended_at: datetime | None
    error: str | None
    airflow_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class IngestedFile:
    """What ingestion did with one file. `content_hash` detects a corrected re-upload."""

    source: str
    path: str
    content_hash: str
    status: FileStatus
    rows_loaded: int | None = None
    error: str | None = None
    run_id: str | None = None
