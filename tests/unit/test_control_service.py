from __future__ import annotations

import logging
from collections.abc import Mapping

import pytest
from pydantic import BaseModel

from datalake.control import (
    ConfigError,
    ConfigResolver,
    Control,
    ProcessDisabledError,
    ProcessNotRegisteredError,
)
from datalake.control.runtime import EnvironmentSecrets, Uuid7Generator
from datalake.control.testing import (
    InMemoryControlStore,
    SequentialUuids,
    StaticSecrets,
    TickingClock,
)


class IngestSettings(BaseModel):
    trino_host: str
    trino_password: str
    batch_size: int = 1000


def _control(
    store: InMemoryControlStore,
    environment: Mapping[str, str] | None = None,
    secrets: Mapping[str, str] | None = None,
    strict: bool = False,
) -> Control:
    env = dict(environment or {})
    return Control(
        store=store,
        resolver=ConfigResolver(env, StaticSecrets(secrets), strict=strict),
        clock=TickingClock(),
        ids=SequentialUuids(),
        environment=env,
        host="test-host",
    )


@pytest.fixture
def store() -> InMemoryControlStore:
    store = InMemoryControlStore()
    ingest = store.register_process("ingest", "Load sources")
    store.register_process("transform")
    store.set_config("trino_host", "trino", None, "tester")
    store.set_config("trino_password", "${secret:trino-password}", None, "tester")
    store.set_config("batch_size", "5000", ingest.process_id, "tester")
    return store


# --- registration ---------------------------------------------------------------------


def test_unregistered_process_fails_before_anything_is_recorded(
    store: InMemoryControlStore,
) -> None:
    with pytest.raises(ProcessNotRegisteredError, match="dl process register nightly"):
        with _control(store).start("nightly"):
            pytest.fail("the block must not run")

    assert store.list_runs(None, 10) == []


def test_disabled_process_fails_before_anything_is_recorded(store: InMemoryControlStore) -> None:
    store.set_process_enabled("ingest", False)

    with pytest.raises(ProcessDisabledError, match="dl process enable ingest"):
        with _control(store).start("ingest"):
            pytest.fail("the block must not run")

    assert store.list_runs(None, 10) == []


# --- runs and steps -------------------------------------------------------------------


def test_successful_run_is_recorded_with_its_steps(store: InMemoryControlStore) -> None:
    with _control(store).start("ingest") as run, run.step("load_orders") as step:
        step.rows_read = 10
        step.rows_written = 9

    (record,) = store.list_runs("ingest", 10)
    assert record.status == "succeeded"
    assert record.error is None
    assert record.ended_at is not None
    assert record.ended_at > record.started_at
    details = store.run_details(run.run_id)
    assert details["host"] == "test-host"
    assert details["steps"] == [
        {
            "name": "load_orders",
            "status": "succeeded",
            "rows_read": 10,
            "rows_written": 9,
            "error": None,
        }
    ]


def test_failure_marks_step_and_run_failed_and_is_reraised(store: InMemoryControlStore) -> None:
    with pytest.raises(RuntimeError, match="source is down"):
        with _control(store).start("ingest") as run, run.step("load_orders") as step:
            step.rows_read = 3
            raise RuntimeError("source is down")

    details = store.run_details(store.list_runs("ingest", 1)[0].run_id)
    assert details["status"] == "failed"
    assert details["error"] == "RuntimeError: source is down"
    assert details["steps"][0]["status"] == "failed"
    assert details["steps"][0]["rows_read"] == 3
    assert details["steps"][0]["error"] == "RuntimeError: source is down"


def test_airflow_and_git_identifiers_are_recorded(store: InMemoryControlStore) -> None:
    environment = {
        "AIRFLOW_CTX_DAG_ID": "nightly",
        "AIRFLOW_CTX_DAG_RUN_ID": "scheduled__2026-11-19",
        "AIRFLOW_CTX_TASK_ID": "ingest",
        "DL_GIT_COMMIT": "abc123",
    }

    with _control(store, environment).start("ingest") as run:
        pass

    details = store.run_details(run.run_id)
    assert details["airflow_dag_id"] == "nightly"
    assert details["airflow_run_id"] == "scheduled__2026-11-19"
    assert details["airflow_task_id"] == "ingest"
    assert details["git_commit"] == "abc123"


def test_run_outside_airflow_has_no_airflow_identifiers(store: InMemoryControlStore) -> None:
    with _control(store).start("ingest") as run:
        pass

    assert store.run_details(run.run_id)["airflow_run_id"] is None


def test_watermarks_belong_to_the_process(store: InMemoryControlStore) -> None:
    control = _control(store)
    with control.start("ingest") as run:
        assert run.get_watermark("orders") is None
        run.set_watermark("orders", "2026-11-19T00:00:00")

    with control.start("ingest") as run:
        assert run.get_watermark("orders") == "2026-11-19T00:00:00"
    with control.start("transform") as run:
        assert run.get_watermark("orders") is None


# --- configuration --------------------------------------------------------------------


def test_config_is_typed_and_own_value_overrides_shared(store: InMemoryControlStore) -> None:
    store.set_config("batch_size", "100", None, "tester")

    with _control(store, secrets={"trino-password": "s3cret"}).start("ingest") as run:
        settings = run.config(IngestSettings)

    assert settings == IngestSettings(trino_host="trino", trino_password="s3cret", batch_size=5000)


def test_process_never_sees_another_process_config(store: InMemoryControlStore) -> None:
    with _control(store, secrets={"trino-password": "s3cret"}).start("transform") as run:
        settings = run.config(IngestSettings)

    assert settings.batch_size == 1000  # the default, not ingest's 5000


def test_snapshot_never_contains_a_secret_value(store: InMemoryControlStore) -> None:
    with _control(store, secrets={"trino-password": "s3cret"}).start("ingest") as run:
        run.config(IngestSettings)

    snapshot = store.run_details(run.run_id)["config_snapshot"]
    assert snapshot["trino_host"] == {"value": "trino", "source": "database"}
    assert snapshot["trino_password"]["value"] == "${secret:trino-password}"
    assert snapshot["batch_size"] == {"value": "5000", "source": "database"}
    assert "s3cret" not in str(snapshot)


def test_config_read_at_start_is_not_affected_by_later_changes(
    store: InMemoryControlStore,
) -> None:
    with _control(store, secrets={"trino-password": "x"}).start("ingest") as run:
        store.set_config("trino_host", "changed-mid-run", None, "tester")
        settings = run.config(IngestSettings)

    assert settings.trino_host == "trino"


def test_missing_key_falls_back_to_the_environment_with_a_warning(
    store: InMemoryControlStore, caplog: pytest.LogCaptureFixture
) -> None:
    store.unset_config("trino_host", None, "tester")
    environment = {"DL_TRINO_HOST": "from-env"}

    with caplog.at_level(logging.WARNING, logger="datalake.control"):
        with _control(store, environment, {"trino-password": "x"}).start("ingest") as run:
            settings = run.config(IngestSettings)

    assert settings.trino_host == "from-env"
    assert "trino_host" in caplog.text
    snapshot = store.run_details(run.run_id)["config_snapshot"]
    assert snapshot["trino_host"] == {"value": "from-env", "source": "env"}


def test_sensitive_value_from_the_environment_is_redacted(store: InMemoryControlStore) -> None:
    store.unset_config("trino_password", None, "tester")

    with _control(store, {"DL_TRINO_PASSWORD": "plain"}).start("ingest") as run:
        assert run.config(IngestSettings).trino_password == "plain"

    snapshot = store.run_details(run.run_id)["config_snapshot"]
    assert snapshot["trino_password"] == {"value": "***", "source": "env"}


def test_strict_mode_ignores_the_environment(store: InMemoryControlStore) -> None:
    store.unset_config("trino_host", None, "tester")
    control = _control(store, {"DL_TRINO_HOST": "from-env"}, {"trino-password": "x"}, strict=True)

    with pytest.raises(ConfigError, match="trino_host: missing"):
        with control.start("ingest") as run:
            run.config(IngestSettings)

    assert store.list_runs("ingest", 1)[0].status == "failed"


def test_every_problem_is_reported_at_once(store: InMemoryControlStore) -> None:
    store.unset_config("trino_host", None, "tester")

    with pytest.raises(ConfigError) as caught:
        with _control(store).start("ingest") as run:
            run.config(IngestSettings)

    message = str(caught.value)
    assert "trino_host: missing" in message
    assert "trino_password: secret 'trino-password' not found" in message


def test_invalid_value_names_the_key(store: InMemoryControlStore) -> None:
    ingest = store.find_process("ingest")
    assert ingest is not None
    store.set_config("batch_size", "lots", ingest.process_id, "tester")

    with pytest.raises(ConfigError, match="batch_size"):
        with _control(store, secrets={"trino-password": "x"}).start("ingest") as run:
            run.config(IngestSettings)


def test_env_reference_is_resolved_where_the_process_runs(store: InMemoryControlStore) -> None:
    store.set_config("trino_host", "${env:TRINO_HOST}:8080", None, "tester")

    with _control(store, {"TRINO_HOST": "10.0.1.4"}, {"trino-password": "x"}).start("ingest") as r:
        assert r.config(IngestSettings).trino_host == "10.0.1.4:8080"

    with pytest.raises(ConfigError, match="environment variable TRINO_HOST is not set"):
        with _control(store, {}, {"trino-password": "x"}).start("ingest") as r:
            r.config(IngestSettings)


# --- small runtime pieces -------------------------------------------------------------


def test_environment_secrets_map_names_to_variables() -> None:
    secrets = EnvironmentSecrets({"DL_SECRET_TRINO_PASSWORD": "pw"})

    assert secrets.get("trino-password") == "pw"
    with pytest.raises(Exception, match="DL_SECRET_MISSING_ONE"):
        secrets.get("missing.one")


def test_uuid7_ids_are_valid_and_sort_by_time() -> None:
    import time
    import uuid

    generator = Uuid7Generator()
    first = generator.new_id()
    time.sleep(0.003)
    second = generator.new_id()

    assert uuid.UUID(first).version == 7
    assert first < second
