"""One suite for the control store, run against the fake and real Postgres.

The Postgres case runs when DL_TEST_DATABASE_URL points at an empty, disposable
database, and is skipped otherwise. It applies the real migrations first.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import psycopg
import pytest

from datalake.control.errors import ProcessNotRegisteredError
from datalake.control.model import IngestedFile, RunStart
from datalake.control.ports import ControlStore
from datalake.control.postgres import PostgresControlStore, apply_migrations
from datalake.control.testing import InMemoryControlStore

T0 = datetime(2026, 11, 19, 2, 0, tzinfo=UTC)


class InspectableStore(ControlStore, Protocol):
    def run_details(self, run_id: str) -> dict[str, Any]: ...

    def config_history(self) -> list[tuple[str, str, str | None, str | None, str]]: ...


def _run_id(n: int) -> str:
    return f"00000000-0000-7000-8000-{n:012d}"


@pytest.fixture(params=["fake", "postgres"])
def store(request: pytest.FixtureRequest) -> Iterator[InspectableStore]:
    if request.param == "fake":
        yield InMemoryControlStore()
        return

    url = os.environ.get("DL_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set DL_TEST_DATABASE_URL to run against Postgres")
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute("drop schema if exists control cascade")
        admin.execute("drop table if exists public.control_schema_version")
    assert apply_migrations(url) == [1, 2]
    assert apply_migrations(url) == []  # running again changes nothing
    postgres = PostgresControlStore.connect(url)
    yield postgres
    postgres.close()


# --- processes ------------------------------------------------------------------------


def test_register_find_and_list(store: InspectableStore) -> None:
    assert store.find_process("ingest") is None

    created = store.register_process("ingest", "Load sources")
    store.register_process("transform")

    assert created.is_enabled
    assert store.find_process("ingest") == created
    assert [p.name for p in store.list_processes()] == ["ingest", "transform"]


def test_registering_again_updates_the_description_but_never_re_enables(
    store: InspectableStore,
) -> None:
    first = store.register_process("ingest", "old")
    store.set_process_enabled("ingest", False)

    again = store.register_process("ingest", "new")

    assert again.process_id == first.process_id
    assert again.description == "new"
    assert not again.is_enabled


def test_enabling_an_unknown_process_is_an_error(store: InspectableStore) -> None:
    with pytest.raises(ProcessNotRegisteredError):
        store.set_process_enabled("nope", True)


@pytest.mark.parametrize("name", ["Ingest", "1st", "has space", ""])
def test_invalid_process_names_are_rejected(store: InspectableStore, name: str) -> None:
    with pytest.raises((ValueError, psycopg.Error)):
        store.register_process(name)


# --- configuration --------------------------------------------------------------------


def test_config_is_shared_plus_own_and_excludes_other_processes(store: InspectableStore) -> None:
    ingest = store.register_process("ingest")
    transform = store.register_process("transform")
    store.set_config("trino_host", "trino", None, "alice")
    store.set_config("batch_size", "100", None, "alice")
    store.set_config("batch_size", "5000", ingest.process_id, "alice")
    store.set_config("threads", "8", transform.process_id, "alice")

    assert store.get_config(ingest.process_id) == {"trino_host": "trino", "batch_size": "5000"}
    assert store.get_config(transform.process_id) == {
        "trino_host": "trino",
        "batch_size": "100",
        "threads": "8",
    }
    listed = [(e.process, e.key, e.value) for e in store.list_config()]
    assert listed == [
        (None, "batch_size", "100"),
        (None, "trino_host", "trino"),
        ("ingest", "batch_size", "5000"),
        ("transform", "threads", "8"),
    ]


def test_every_config_change_is_recorded_with_who_made_it(store: InspectableStore) -> None:
    store.set_config("trino_host", "a", None, "alice")
    store.set_config("trino_host", "b", None, "bob")
    store.set_config("trino_host", "b", None, "carol")  # no change, so no history row
    assert store.unset_config("trino_host", None, "dave")
    assert not store.unset_config("trino_host", None, "dave")

    assert store.config_history() == [
        ("trino_host", "insert", None, "a", "alice"),
        ("trino_host", "update", "a", "b", "bob"),
        ("trino_host", "delete", "b", None, "dave"),
    ]


def test_unset_removes_only_the_given_scope(store: InspectableStore) -> None:
    ingest = store.register_process("ingest")
    store.set_config("batch_size", "100", None, "alice")
    store.set_config("batch_size", "5000", ingest.process_id, "alice")

    assert store.unset_config("batch_size", ingest.process_id, "alice")

    assert store.get_config(ingest.process_id) == {"batch_size": "100"}


@pytest.mark.parametrize("key", ["Trino.Host", "trino-host", "9lives", ""])
def test_invalid_config_keys_are_rejected(store: InspectableStore, key: str) -> None:
    with pytest.raises((ValueError, psycopg.Error)):
        store.set_config(key, "x", None, "alice")


# --- runs, steps, watermarks ----------------------------------------------------------


def test_run_lifecycle(store: InspectableStore) -> None:
    ingest = store.register_process("ingest")
    store.start_run(
        RunStart(
            run_id=_run_id(1),
            process_id=ingest.process_id,
            started_at=T0,
            host="vm2",
            git_commit="abc123",
            airflow_dag_id="nightly",
            airflow_run_id="scheduled__1",
            airflow_task_id="ingest",
        )
    )
    assert store.run_details(_run_id(1))["status"] == "running"

    store.save_config_snapshot(_run_id(1), {"a": {"value": "1", "source": "database"}})
    store.save_config_snapshot(_run_id(1), {"b": {"value": "2", "source": "env"}})
    step = store.start_step(_run_id(1), "load_orders", T0)
    store.finish_step(step, "succeeded", 10, 9, None, T0 + timedelta(seconds=5))
    store.finish_run(_run_id(1), "failed", "RuntimeError: boom", T0 + timedelta(seconds=6))

    assert store.run_details(_run_id(1)) == {
        "status": "failed",
        "host": "vm2",
        "git_commit": "abc123",
        "config_snapshot": {
            "a": {"value": "1", "source": "database"},
            "b": {"value": "2", "source": "env"},
        },
        "error": "RuntimeError: boom",
        "airflow_dag_id": "nightly",
        "airflow_run_id": "scheduled__1",
        "airflow_task_id": "ingest",
        "steps": [
            {
                "name": "load_orders",
                "status": "succeeded",
                "rows_read": 10,
                "rows_written": 9,
                "error": None,
            }
        ],
    }


def test_runs_list_newest_first_filtered_and_limited(store: InspectableStore) -> None:
    ingest = store.register_process("ingest")
    transform = store.register_process("transform")
    for n, process in enumerate([ingest, transform, ingest], start=1):
        store.start_run(RunStart(_run_id(n), process.process_id, T0 + timedelta(minutes=n)))
        store.finish_run(_run_id(n), "succeeded", None, T0 + timedelta(minutes=n, seconds=30))

    assert [r.run_id for r in store.list_runs(None, 10)] == [_run_id(3), _run_id(2), _run_id(1)]
    assert [r.run_id for r in store.list_runs("ingest", 10)] == [_run_id(3), _run_id(1)]
    assert [r.run_id for r in store.list_runs(None, 1)] == [_run_id(3)]
    newest = store.list_runs(None, 1)[0]
    assert (newest.process, newest.status, newest.error) == ("ingest", "succeeded", None)
    assert newest.started_at == T0 + timedelta(minutes=3)


def test_watermarks_are_per_process_and_object(store: InspectableStore) -> None:
    ingest = store.register_process("ingest")
    transform = store.register_process("transform")

    assert store.get_watermark(ingest.process_id, "orders") is None
    store.set_watermark(ingest.process_id, "orders", "2026-11-18")
    store.set_watermark(ingest.process_id, "orders", "2026-11-19")
    store.set_watermark(ingest.process_id, "customers", "42")

    assert store.get_watermark(ingest.process_id, "orders") == "2026-11-19"
    assert store.get_watermark(ingest.process_id, "customers") == "42"
    assert store.get_watermark(transform.process_id, "orders") is None


# --- file ledger ----------------------------------------------------------------------


def test_file_ledger_records_replaces_and_lists(store: InspectableStore) -> None:
    ingest = store.register_process("ingest")
    other = store.register_process("transform")
    store.start_run(RunStart(_run_id(1), ingest.process_id, T0))

    assert store.get_ingested_file(ingest.process_id, "orders", "a.csv") is None

    first = IngestedFile("orders", "a.csv", "hash1", "loaded", 10, None, _run_id(1))
    store.record_ingested_file(ingest.process_id, first)
    store.record_ingested_file(
        ingest.process_id, IngestedFile("orders", "b.csv", "hash2", "rejected", None, "bad header")
    )
    store.record_ingested_file(
        ingest.process_id, IngestedFile("customers", "c.xlsx", "hash3", "loaded", 3)
    )
    assert store.get_ingested_file(ingest.process_id, "orders", "a.csv") == first

    corrected = IngestedFile("orders", "a.csv", "hash9", "loaded", 12, None, _run_id(1))
    store.record_ingested_file(ingest.process_id, corrected)

    assert store.get_ingested_file(ingest.process_id, "orders", "a.csv") == corrected
    assert [f.path for f in store.list_ingested_files(ingest.process_id, None)] == [
        "c.xlsx",
        "a.csv",
        "b.csv",
    ]
    assert [f.path for f in store.list_ingested_files(ingest.process_id, "orders")] == [
        "a.csv",
        "b.csv",
    ]
    assert store.list_ingested_files(other.process_id, None) == []
