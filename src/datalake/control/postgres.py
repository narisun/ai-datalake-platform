"""The control database on Postgres."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from importlib import resources
from typing import Any, cast

import psycopg

from datalake.control.errors import ProcessNotRegisteredError
from datalake.control.model import (
    ConfigEntry,
    IngestedFile,
    Process,
    RunRecord,
    RunStart,
    RunStatus,
)

_VERSION_TABLE = """
create table if not exists public.control_schema_version (
    version     integer primary key,
    applied_at  timestamptz not null default now()
)
"""


def apply_migrations(database_url: str) -> list[int]:
    """Apply every migration not yet applied, in order. Returns the versions applied."""
    migrations = resources.files("datalake.control").joinpath("migrations")
    files = sorted(
        (entry for entry in migrations.iterdir() if entry.name.endswith(".sql")),
        key=lambda entry: entry.name,
    )
    applied: list[int] = []
    with psycopg.connect(database_url) as connection:
        connection.execute(_VERSION_TABLE)
        connection.commit()
        rows = connection.execute("select version from public.control_schema_version")
        done = {row[0] for row in rows}
        for entry in files:
            version = int(entry.name.split("_", 1)[0])
            if version in done:
                continue
            # One transaction per migration: it applies completely or not at all.
            with connection.transaction():
                connection.execute(cast(Any, entry.read_text(encoding="utf-8")))
                connection.execute(
                    "insert into public.control_schema_version (version) values (%s)", (version,)
                )
            applied.append(version)
    return applied


class PostgresControlStore:
    """Each method is one statement on an autocommit connection, so it is atomic."""

    def __init__(self, connection: psycopg.Connection[tuple[Any, ...]]) -> None:
        self._connection = connection

    @classmethod
    def connect(cls, database_url: str) -> PostgresControlStore:
        return cls(psycopg.connect(database_url, autocommit=True))

    def close(self) -> None:
        self._connection.close()

    # Processes

    def find_process(self, name: str) -> Process | None:
        row = self._connection.execute(
            "select process_id, name, description, is_enabled from control.process where name = %s",
            (name,),
        ).fetchone()
        return Process(*row) if row else None

    def register_process(self, name: str, description: str = "") -> Process:
        row = self._connection.execute(
            """
            insert into control.process (name, description) values (%s, %s)
            on conflict (name) do update
                set description = excluded.description, updated_at = now()
            returning process_id, name, description, is_enabled
            """,
            (name, description),
        ).fetchone()
        assert row is not None
        return Process(*row)

    def set_process_enabled(self, name: str, enabled: bool) -> None:
        cursor = self._connection.execute(
            "update control.process set is_enabled = %s, updated_at = now() where name = %s",
            (enabled, name),
        )
        if cursor.rowcount == 0:
            raise ProcessNotRegisteredError(name)

    def list_processes(self) -> Sequence[Process]:
        rows = self._connection.execute(
            "select process_id, name, description, is_enabled from control.process order by name"
        ).fetchall()
        return [Process(*row) for row in rows]

    # Configuration

    def get_config(self, process_id: int) -> dict[str, str]:
        rows = self._connection.execute(
            """
            select key, value from control.config
            where process_id is null or process_id = %s
            order by process_id nulls first
            """,
            (process_id,),
        ).fetchall()
        # Shared rows come first, so a process's own value overwrites the shared one.
        return {key: value for key, value in rows}

    def list_config(self) -> Sequence[ConfigEntry]:
        rows = self._connection.execute(
            """
            select c.key, c.value, p.name
            from control.config c left join control.process p using (process_id)
            order by p.name nulls first, c.key
            """
        ).fetchall()
        return [ConfigEntry(*row) for row in rows]

    def set_config(self, key: str, value: str, process_id: int | None, actor: str) -> None:
        self._connection.execute(
            """
            insert into control.config (process_id, key, value, updated_by)
            values (%s, %s, %s, %s)
            on conflict (coalesce(process_id, 0), key) do update
                set value = excluded.value, updated_by = excluded.updated_by, updated_at = now()
            """,
            (process_id, key, value, actor),
        )

    def unset_config(self, key: str, process_id: int | None, actor: str) -> bool:
        # The history trigger reads the actor from this transaction-local setting.
        with self._connection.transaction():
            self._connection.execute("select set_config('control.actor', %s, true)", (actor,))
            cursor = self._connection.execute(
                "delete from control.config where key = %s and process_id is not distinct from %s",
                (key, process_id),
            )
            return cursor.rowcount > 0

    # Runs

    def start_run(self, run: RunStart) -> None:
        self._connection.execute(
            """
            insert into control.run (run_id, process_id, status, started_at, host, git_commit,
                                     airflow_dag_id, airflow_run_id, airflow_task_id)
            values (%s, %s, 'running', %s, %s, %s, %s, %s, %s)
            """,
            (
                run.run_id,
                run.process_id,
                run.started_at,
                run.host,
                run.git_commit,
                run.airflow_dag_id,
                run.airflow_run_id,
                run.airflow_task_id,
            ),
        )

    def save_config_snapshot(self, run_id: str, snapshot: Mapping[str, Mapping[str, str]]) -> None:
        self._connection.execute(
            """
            update control.run set config_snapshot = config_snapshot || %s::jsonb
            where run_id = %s
            """,
            (json.dumps(snapshot), run_id),
        )

    def finish_run(
        self, run_id: str, status: RunStatus, error: str | None, ended_at: datetime
    ) -> None:
        self._connection.execute(
            "update control.run set status = %s, error = %s, ended_at = %s where run_id = %s",
            (status, error, ended_at, run_id),
        )

    def start_step(self, run_id: str, name: str, started_at: datetime) -> int:
        row = self._connection.execute(
            """
            insert into control.step_run (run_id, name, status, started_at)
            values (%s, %s, 'running', %s) returning step_run_id
            """,
            (run_id, name, started_at),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def finish_step(
        self,
        step_run_id: int,
        status: RunStatus,
        rows_read: int | None,
        rows_written: int | None,
        error: str | None,
        ended_at: datetime,
    ) -> None:
        self._connection.execute(
            """
            update control.step_run
            set status = %s, rows_read = %s, rows_written = %s, error = %s, ended_at = %s
            where step_run_id = %s
            """,
            (status, rows_read, rows_written, error, ended_at, step_run_id),
        )

    def list_runs(self, process: str | None, limit: int) -> Sequence[RunRecord]:
        rows = self._connection.execute(
            """
            select r.run_id::text, p.name, r.status, r.started_at, r.ended_at, r.error,
                   r.airflow_run_id
            from control.run r join control.process p using (process_id)
            where %s::text is null or p.name = %s
            order by r.started_at desc, r.run_id desc
            limit %s
            """,
            (process, process, limit),
        ).fetchall()
        return [RunRecord(*row) for row in rows]

    # Watermarks

    def get_watermark(self, process_id: int, object_name: str) -> str | None:
        row = self._connection.execute(
            "select value from control.watermark where process_id = %s and object_name = %s",
            (process_id, object_name),
        ).fetchone()
        return row[0] if row else None

    def set_watermark(self, process_id: int, object_name: str, value: str) -> None:
        self._connection.execute(
            """
            insert into control.watermark (process_id, object_name, value) values (%s, %s, %s)
            on conflict (process_id, object_name) do update
                set value = excluded.value, updated_at = now()
            """,
            (process_id, object_name, value),
        )

    # File ledger

    def get_ingested_file(self, process_id: int, source: str, path: str) -> IngestedFile | None:
        row = self._connection.execute(
            """
            select source, path, content_hash, status, rows_loaded, error, run_id::text
            from control.ingested_file
            where process_id = %s and source = %s and path = %s
            """,
            (process_id, source, path),
        ).fetchone()
        return IngestedFile(*row) if row else None

    def record_ingested_file(self, process_id: int, file: IngestedFile) -> None:
        self._connection.execute(
            """
            insert into control.ingested_file
                (process_id, source, path, content_hash, status, rows_loaded, error, run_id)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (process_id, source, path) do update set
                content_hash = excluded.content_hash, status = excluded.status,
                rows_loaded = excluded.rows_loaded, error = excluded.error,
                run_id = excluded.run_id, recorded_at = now()
            """,
            (
                process_id,
                file.source,
                file.path,
                file.content_hash,
                file.status,
                file.rows_loaded,
                file.error,
                file.run_id,
            ),
        )

    def list_ingested_files(self, process_id: int, source: str | None) -> Sequence[IngestedFile]:
        rows = self._connection.execute(
            """
            select source, path, content_hash, status, rows_loaded, error, run_id::text
            from control.ingested_file
            where process_id = %s and (%s::text is null or source = %s)
            order by source, path
            """,
            (process_id, source, source),
        ).fetchall()
        return [IngestedFile(*row) for row in rows]

    # For tests and operators

    def run_details(self, run_id: str) -> dict[str, Any]:
        """The full run row and its steps, as plain data."""
        run = self._connection.execute(
            """
            select status, host, git_commit, config_snapshot, error,
                   airflow_dag_id, airflow_run_id, airflow_task_id
            from control.run where run_id = %s
            """,
            (run_id,),
        ).fetchone()
        if run is None:
            return {}
        steps = self._connection.execute(
            """
            select name, status, rows_read, rows_written, error
            from control.step_run where run_id = %s order by step_run_id
            """,
            (run_id,),
        ).fetchall()
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
            **dict(zip(keys, run, strict=True)),
            "steps": [dict(zip(step_keys, step, strict=True)) for step in steps],
        }

    def config_history(self) -> list[tuple[str, str, str | None, str | None, str]]:
        """(key, operation, old value, new value, changed by), oldest first."""
        rows = self._connection.execute(
            """
            select key, operation, old_value, new_value, changed_by
            from control.config_history order by history_id
            """
        ).fetchall()
        return [cast(tuple[str, str, str | None, str | None, str], row) for row in rows]
