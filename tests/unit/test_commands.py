from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from datalake.cli import main
from datalake.control import ConfigResolver, Control, ProcessNotRegisteredError
from datalake.control.command import CommandFailedError, run_external
from datalake.control.testing import (
    InMemoryControlStore,
    SequentialUuids,
    StaticSecrets,
    TickingClock,
)
from datalake.transform.command import TransformFailedError, TransformSettings, run_transform
from datalake.transform.model import RunResult


def _control(store: InMemoryControlStore) -> Control:
    return Control(
        store=store,
        resolver=ConfigResolver({}, StaticSecrets(), strict=True),
        clock=TickingClock(),
        ids=SequentialUuids(),
        environment={},
    )


class FakeDbt:
    def __init__(
        self,
        build_ok: bool = True,
        docs_ok: bool = True,
        build_problems: tuple[str, ...] = (),
        stale: tuple[str, ...] = (),
    ) -> None:
        self._build_ok = build_ok
        self._docs_ok = docs_ok
        self._build_problems = build_problems
        self._stale = stale
        self.calls: list[str] = []

    async def docs(self) -> RunResult:
        self.calls.append("docs")
        return RunResult(self._docs_ok, (), "target")

    async def freshness(self) -> RunResult:
        self.calls.append("freshness")
        return RunResult(not self._stale, (), "target", problems=self._stale)

    async def build(self, select: str | None = None) -> RunResult:
        self.calls.append("build")
        return RunResult(
            self._build_ok, ("stg_orders", "fct_orders"), "target", problems=self._build_problems
        )


@pytest.fixture
def store() -> InMemoryControlStore:
    store = InMemoryControlStore()
    store.register_process("transform")
    store.register_process("catalog-sync")
    for key, value in {
        "dbt_project_dir": "/opt/dbt",
        "dbt_target_dir": "/work/target",
        "dbt_log_dir": "/work/logs",
        "trino_host": "trino",
    }.items():
        store.set_config(key, value, None, "tester")
    return store


# --- dl transform ---------------------------------------------------------------------


async def test_transform_runs_docs_before_build_and_records_both(
    store: InMemoryControlStore,
) -> None:
    dbt = FakeDbt()
    seen: list[tuple[TransformSettings, str]] = []

    def factory(settings: TransformSettings, run_id: str) -> FakeDbt:
        seen.append((settings, run_id))
        return dbt

    result = await run_transform(_control(store), factory)

    assert dbt.calls == ["docs", "freshness", "build"]
    assert result.models_built == ("stg_orders", "fct_orders")
    settings, run_id = seen[0]
    assert settings.dbt_project_dir == Path("/opt/dbt")
    assert settings.dbt_environment()["TRINO_HOST"] == "trino"
    assert settings.dbt_environment()["TRINO_PORT"] == "8080"
    details = store.run_details(run_id)
    assert details["status"] == "succeeded"
    assert [(s["name"], s["status"], s["rows_written"]) for s in details["steps"]] == [
        ("docs", "succeeded", None),
        ("freshness", "succeeded", None),
        ("build", "succeeded", 2),
    ]


def test_dbt_gets_sign_in_settings_only_when_configured() -> None:
    base = {
        "dbt_project_dir": "/p",
        "dbt_target_dir": "/t",
        "dbt_log_dir": "/l",
        "trino_host": "trino",
    }

    plain = TransformSettings.model_validate(base).dbt_environment()
    signed_in = TransformSettings.model_validate(
        {**base, "trino_password": "pw", "trino_ca_file": "/tls/ca.crt", "trino_scheme": "https"}
    ).dbt_environment()

    assert "TRINO_PASSWORD" not in plain
    assert "TRINO_CERT" not in plain
    assert signed_in["TRINO_PASSWORD"] == "pw"
    assert signed_in["TRINO_SCHEME"] == "https"
    assert signed_in["TRINO_CERT"].replace("\\", "/") == "/tls/ca.crt"


async def test_failed_build_fails_the_run(store: InMemoryControlStore) -> None:
    with pytest.raises(TransformFailedError, match="dbt build failed"):
        await run_transform(_control(store), lambda settings, run_id: FakeDbt(build_ok=False))

    (run,) = store.list_runs("transform", 5)
    assert run.status == "failed"
    assert store.run_details(run.run_id)["steps"][2]["status"] == "failed"


async def test_failure_message_names_what_failed(store: InMemoryControlStore) -> None:
    dbt = FakeDbt(
        build_ok=False,
        build_problems=(
            "unique_customer_customer_id: fail: Got 1 result",
            "fct_orders: error: boom",
        ),
    )

    with pytest.raises(TransformFailedError) as caught:
        await run_transform(_control(store), lambda settings, run_id: dbt)

    message = str(caught.value)
    assert "unique_customer_customer_id: fail: Got 1 result" in message
    assert "fct_orders: error: boom" in message
    (run,) = store.list_runs("transform", 5)
    assert run.error is not None
    assert "unique_customer_customer_id" in run.error


async def test_stale_source_still_builds_but_fails_the_run(store: InMemoryControlStore) -> None:
    dbt = FakeDbt(stale=("orders: error: last loaded 2026-11-01",))

    with pytest.raises(TransformFailedError, match="sources are stale: orders: error"):
        await run_transform(_control(store), lambda settings, run_id: dbt)

    assert dbt.calls == ["docs", "freshness", "build"]
    (run,) = store.list_runs("transform", 5)
    steps = store.run_details(run.run_id)["steps"]
    assert [(s["name"], s["status"]) for s in steps] == [
        ("docs", "succeeded"),
        ("freshness", "succeeded"),
        ("build", "succeeded"),
    ]
    assert run.status == "failed"


async def test_failed_docs_stops_before_build(store: InMemoryControlStore) -> None:
    dbt = FakeDbt(docs_ok=False)

    with pytest.raises(TransformFailedError, match="docs"):
        await run_transform(_control(store), lambda settings, run_id: dbt)

    assert dbt.calls == ["docs"]


async def test_transform_must_be_registered() -> None:
    with pytest.raises(ProcessNotRegisteredError):
        await run_transform(_control(InMemoryControlStore()), lambda s, r: FakeDbt())


# --- dl exec --------------------------------------------------------------------------


def test_exec_runs_the_command_as_a_recorded_run(store: InMemoryControlStore) -> None:
    calls: list[tuple[Sequence[str], Mapping[str, str]]] = []

    def runner(command: Sequence[str], environment: Mapping[str, str]) -> int:
        calls.append((command, environment))
        return 0

    run_external(
        _control(store), "catalog-sync", ["metadata", "ingest", "-c", "x.yaml"], {"A": "1"}, runner
    )

    ((command, environment),) = calls
    assert command == ["metadata", "ingest", "-c", "x.yaml"]
    assert environment["A"] == "1"
    (run,) = store.list_runs("catalog-sync", 5)
    assert environment["DL_RUN_ID"] == run.run_id
    assert run.status == "succeeded"
    assert store.run_details(run.run_id)["steps"][0]["name"] == "metadata"


def test_exec_marks_the_run_failed_when_the_command_fails(store: InMemoryControlStore) -> None:
    with pytest.raises(CommandFailedError, match="code 3") as caught:
        run_external(_control(store), "catalog-sync", ["false"], {}, lambda c, e: 3)

    assert caught.value.exit_code == 3
    (run,) = store.list_runs("catalog-sync", 5)
    assert run.status == "failed"


def test_exec_refuses_an_unregistered_process_without_running_anything(
    store: InMemoryControlStore,
) -> None:
    ran: list[Sequence[str]] = []

    def runner(command: Sequence[str], environment: Mapping[str, str]) -> int:
        ran.append(command)
        return 0

    with pytest.raises(ProcessNotRegisteredError):
        run_external(_control(store), "unknown", ["true"], {}, runner)

    assert ran == []


def test_exec_needs_a_command(store: InMemoryControlStore) -> None:
    with pytest.raises(Exception, match="no command given"):
        run_external(_control(store), "catalog-sync", [], {})


def test_cli_exec_runs_a_real_command(
    store: InMemoryControlStore, capfd: pytest.CaptureFixture[str]
) -> None:
    script = "import os; print(os.environ['DL_RUN_ID'])"

    code = main(
        ["exec", "catalog-sync", "--", sys.executable, "-c", script],
        store=store,
        environment={},
    )

    assert code == 0
    (run,) = store.list_runs("catalog-sync", 5)
    assert run.run_id in capfd.readouterr().out


def test_cli_exec_reports_failure(
    store: InMemoryControlStore, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        ["exec", "catalog-sync", "--", sys.executable, "-c", "raise SystemExit(4)"],
        store=store,
        environment={},
    )

    assert code == 1
    assert "exited with code 4" in capsys.readouterr().err
