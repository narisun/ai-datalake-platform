from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from datalake.cli import main
from datalake.control.model import RunStart
from datalake.control.testing import InMemoryControlStore

ENV = {"DL_ACTOR": "alice"}


@pytest.fixture
def store() -> InMemoryControlStore:
    return InMemoryControlStore()


def _dl(store: InMemoryControlStore, *argv: str) -> int:
    return main(list(argv), store=store, environment=ENV)


def test_register_disable_enable_and_list(
    store: InMemoryControlStore, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _dl(store, "process", "register", "ingest", "--description", "Load sources") == 0
    assert _dl(store, "process", "disable", "ingest") == 0
    assert _dl(store, "process", "list") == 0
    assert "DISABLED  ingest  Load sources" in capsys.readouterr().out

    assert _dl(store, "process", "enable", "ingest") == 0
    process = store.find_process("ingest")
    assert process is not None
    assert process.is_enabled


def test_disabling_an_unknown_process_reports_an_error(
    store: InMemoryControlStore, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _dl(store, "process", "disable", "nope") == 1
    assert "not registered" in capsys.readouterr().err


def test_config_set_shared_and_per_process(
    store: InMemoryControlStore, capsys: pytest.CaptureFixture[str]
) -> None:
    _dl(store, "process", "register", "ingest")

    assert _dl(store, "config", "set", "trino_host=trino:8080") == 0
    assert _dl(store, "config", "set", "batch_size=5000", "--process", "ingest") == 0
    assert _dl(store, "config", "set", "url=postgres://h/db?x=1") == 0  # value contains '='
    capsys.readouterr()

    assert _dl(store, "config", "list") == 0
    assert capsys.readouterr().out.splitlines() == [
        "*\ttrino_host=trino:8080",
        "*\turl=postgres://h/db?x=1",
        "ingest\tbatch_size=5000",
    ]
    assert store.config_history()[0] == ("trino_host", "insert", None, "trino:8080", "alice")


def test_config_set_rejects_bad_input(
    store: InMemoryControlStore, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _dl(store, "config", "set", "no-equals-sign") == 1
    assert _dl(store, "config", "set", "Bad.Key=x") == 1
    assert _dl(store, "config", "set", "a=b", "--process", "unknown") == 1
    err = capsys.readouterr().err
    assert "expected key=value" in err
    assert "invalid configuration key" in err
    assert "not registered" in err


def test_config_unset(store: InMemoryControlStore) -> None:
    _dl(store, "config", "set", "trino_host=trino")

    assert _dl(store, "config", "unset", "trino_host") == 0
    assert _dl(store, "config", "unset", "trino_host") == 1


def test_import_moves_dotenv_values_but_never_the_bootstrap_ones(
    store: InMemoryControlStore, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "DL_DATABASE_URL=postgresql://secret@db/control\n"
        "DL_TRINO_HOST=trino\n"
        "DL_BATCH_SIZE=100\n"
        "UNRELATED=1\n",
        encoding="utf-8",
    )

    assert _dl(store, "config", "doctor", str(dotenv)) == 1
    out = capsys.readouterr().out
    assert "DL_TRINO_HOST" in out
    assert "DL_BATCH_SIZE" in out
    assert "DATABASE_URL" not in out

    assert _dl(store, "config", "import", str(dotenv)) == 0
    assert {(e.key, e.value) for e in store.list_config()} == {
        ("trino_host", "trino"),
        ("batch_size", "100"),
    }

    assert _dl(store, "config", "doctor", str(dotenv)) == 0


def test_runs_lists_status_and_error(
    store: InMemoryControlStore, capsys: pytest.CaptureFixture[str]
) -> None:
    ingest = store.register_process("ingest")
    started = datetime(2026, 11, 19, 2, 0, tzinfo=UTC)
    store.start_run(RunStart("00000000-0000-7000-8000-000000000001", ingest.process_id, started))
    store.finish_run(
        "00000000-0000-7000-8000-000000000001", "failed", "RuntimeError: boom", started
    )

    assert _dl(store, "runs", "--process", "ingest") == 0
    line = capsys.readouterr().out.strip()
    assert line.startswith("2026-11-19T02:00:00+00:00  failed")
    assert line.endswith("RuntimeError: boom")


def test_missing_database_url_is_a_clear_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["process", "list"], environment={}) == 1
    assert "DL_DATABASE_URL is not set" in capsys.readouterr().err
