"""What the control service needs from the outside world.

These calls are synchronous on purpose. Each is one short statement at the start or
end of a run or step, and a blocking call keeps `with control.start(...)` usable from
any process, async or not.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol

from datalake.control.model import (
    ConfigEntry,
    IngestedFile,
    Process,
    RunRecord,
    RunStart,
    RunStatus,
)


class ControlStore(Protocol):
    """The control database."""

    # Processes
    def find_process(self, name: str) -> Process | None: ...

    def register_process(self, name: str, description: str = "") -> Process:
        """Add the process, or update its description if it exists. Never re-enables."""
        ...

    def set_process_enabled(self, name: str, enabled: bool) -> None:
        """Raise ProcessNotRegisteredError if the process is unknown."""
        ...

    def list_processes(self) -> Sequence[Process]: ...

    # Configuration
    def get_config(self, process_id: int) -> dict[str, str]:
        """Shared values overlaid with this process's own. Nothing from other processes."""
        ...

    def list_config(self) -> Sequence[ConfigEntry]: ...

    def set_config(self, key: str, value: str, process_id: int | None, actor: str) -> None: ...

    def unset_config(self, key: str, process_id: int | None, actor: str) -> bool:
        """Return whether a value was removed."""
        ...

    # Runs
    def start_run(self, run: RunStart) -> None: ...

    def save_config_snapshot(self, run_id: str, snapshot: Mapping[str, Mapping[str, str]]) -> None:
        """Merge into the run's snapshot of the configuration it resolved."""
        ...

    def finish_run(
        self, run_id: str, status: RunStatus, error: str | None, ended_at: datetime
    ) -> None: ...

    def start_step(self, run_id: str, name: str, started_at: datetime) -> int: ...

    def finish_step(
        self,
        step_run_id: int,
        status: RunStatus,
        rows_read: int | None,
        rows_written: int | None,
        error: str | None,
        ended_at: datetime,
    ) -> None: ...

    def list_runs(self, process: str | None, limit: int) -> Sequence[RunRecord]:
        """Most recent first."""
        ...

    # Watermarks
    def get_watermark(self, process_id: int, object_name: str) -> str | None: ...

    def set_watermark(self, process_id: int, object_name: str, value: str) -> None: ...

    # File ledger
    def get_ingested_file(self, process_id: int, source: str, path: str) -> IngestedFile | None: ...

    def record_ingested_file(self, process_id: int, file: IngestedFile) -> None:
        """Insert, or replace the earlier record for the same source and path."""
        ...

    def list_ingested_files(self, process_id: int, source: str | None) -> Sequence[IngestedFile]:
        """Ordered by source, then path."""
        ...


class SecretResolver(Protocol):
    """Fetches a secret by name (Azure Key Vault in production)."""

    def get(self, name: str) -> str:
        """Raise SecretNotFoundError if there is no such secret."""
        ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class IdGenerator(Protocol):
    def new_id(self) -> str: ...
