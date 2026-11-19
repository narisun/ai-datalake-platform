"""Ingestion scenarios, run against in-memory storage, lake and control database."""

from __future__ import annotations

import pytest

from datalake.control import ConfigResolver, Control
from datalake.control.testing import (
    InMemoryControlStore,
    SequentialUuids,
    StaticSecrets,
    TickingClock,
)
from datalake.ingest.model import SourceDefinition
from datalake.ingest.service import (
    IngestDependencies,
    IngestRejectedError,
    IngestSettings,
    UnknownSourceError,
    run_ingest,
)
from datalake.ingest.testing import InMemoryRawTableWriter
from datalake.storage.testing import InMemoryObjectStore

ORDERS = SourceDefinition(
    name="orders",
    landing_prefix="shop/orders/",
    pattern="orders_*.csv",
    table="raw.orders",
    format="csv",
    required_columns=("order_id",),
)
CUSTOMERS = SourceDefinition(
    name="customers",
    landing_prefix="shop/customers/",
    table="raw.customers",
    format="csv",
    mode="replace",
)


class World:
    """Everything one test needs: landing zone, lake, control database, and a way to run."""

    def __init__(self) -> None:
        self.landing = InMemoryObjectStore()
        self.lake = InMemoryRawTableWriter()
        self.store = InMemoryControlStore()
        self.store.register_process("ingest")
        for key, value in {
            "sources_file": "sources.yml",
            "landing_container": "landing",
            "trino_host": "trino",
        }.items():
            self.store.set_config(key, value, None, "tester")
        self.control = Control(
            store=self.store,
            resolver=ConfigResolver({}, StaticSecrets(), strict=True),
            clock=TickingClock(),
            ids=SequentialUuids(),
            environment={},
        )

    def _build(self, settings: IngestSettings) -> IngestDependencies:
        return IngestDependencies([ORDERS, CUSTOMERS], self.landing, self.lake)

    async def ingest(self, only: str | None = None) -> dict[str, str]:
        summary = await run_ingest(self.control, self._build, TickingClock(), only)
        return {outcome.path: outcome.outcome for outcome in summary.outcomes}

    def orders(self) -> list[tuple[object, object, object]]:
        rows = self.lake.table("raw", "orders")
        return sorted((r["order_id"], r.get("amount"), r["_source_file"]) for r in rows)

    def ledger(self) -> dict[str, tuple[str, int | None]]:
        process = self.store.find_process("ingest")
        assert process is not None
        files = self.store.list_ingested_files(process.process_id, None)
        return {f.path: (f.status, f.rows_loaded) for f in files}


@pytest.fixture
def world() -> World:
    return World()


async def test_first_load_brings_in_every_file(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n11,80\n")
    await world.landing.put("shop/orders/orders_02.csv", b"Order ID,Amount\n12,42.25\n")

    outcomes = await world.ingest()

    assert outcomes == {
        "shop/orders/orders_01.csv": "loaded",
        "shop/orders/orders_02.csv": "loaded",
    }
    assert world.orders() == [
        ("10", "120.50", "shop/orders/orders_01.csv"),
        ("11", "80", "shop/orders/orders_01.csv"),
        ("12", "42.25", "shop/orders/orders_02.csv"),
    ]
    assert world.ledger() == {
        "shop/orders/orders_01.csv": ("loaded", 2),
        "shop/orders/orders_02.csv": ("loaded", 1),
    }


async def test_rows_carry_where_they_came_from(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID\n10\n11\n")

    await world.ingest()

    first, second = world.lake.table("raw", "orders")
    assert (first["_source_file"], first["_row_number"]) == ("shop/orders/orders_01.csv", 1)
    assert second["_row_number"] == 2
    (run,) = world.store.list_runs("ingest", 5)
    assert first["_run_id"] == run.run_id
    assert first["_loaded_at"] is not None


async def test_running_again_with_nothing_new_changes_nothing(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n")
    await world.ingest()

    outcomes = await world.ingest()

    assert outcomes == {"shop/orders/orders_01.csv": "unchanged"}
    assert len(world.orders()) == 1


async def test_only_new_files_are_loaded_on_later_runs(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n")
    await world.ingest()
    await world.landing.put("shop/orders/orders_02.csv", b"Order ID,Amount\n11,80\n")

    outcomes = await world.ingest()

    assert outcomes == {
        "shop/orders/orders_01.csv": "unchanged",
        "shop/orders/orders_02.csv": "loaded",
    }
    assert [order for order, _, _ in world.orders()] == ["10", "11"]


async def test_corrected_file_replaces_only_its_own_rows(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n11,80\n")
    await world.landing.put("shop/orders/orders_02.csv", b"Order ID,Amount\n12,42.25\n")
    await world.ingest()

    # The sender fixes an amount and removes a row, then re-uploads under the same name.
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,99.00\n")
    outcomes = await world.ingest()

    assert outcomes["shop/orders/orders_01.csv"] == "loaded"
    assert outcomes["shop/orders/orders_02.csv"] == "unchanged"
    assert world.orders() == [
        ("10", "99.00", "shop/orders/orders_01.csv"),
        ("12", "42.25", "shop/orders/orders_02.csv"),
    ]
    assert world.ledger()["shop/orders/orders_01.csv"] == ("loaded", 1)


async def test_new_column_is_added_and_old_rows_keep_null(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n")
    await world.ingest()
    await world.landing.put("shop/orders/orders_02.csv", b"Order ID,Amount,Channel\n11,80,web\n")

    await world.ingest()

    by_order = {r["order_id"]: r for r in world.lake.table("raw", "orders")}
    assert by_order["11"]["channel"] == "web"
    assert by_order["10"]["channel"] is None


async def test_file_missing_an_optional_column_still_loads(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n")
    await world.ingest()
    await world.landing.put("shop/orders/orders_02.csv", b"Order ID\n11\n")

    await world.ingest()

    by_order = {r["order_id"]: r for r in world.lake.table("raw", "orders")}
    assert by_order["11"]["amount"] is None


async def test_files_not_matching_the_pattern_are_ignored(world: World) -> None:
    await world.landing.put("shop/orders/README.txt", b"not data")
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID\n10\n")

    assert await world.ingest() == {"shop/orders/orders_01.csv": "loaded"}


async def test_bad_file_is_rejected_but_good_files_still_load_and_the_run_fails(
    world: World,
) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID,Amount\n10,120.50\n")
    await world.landing.put("shop/orders/orders_02.csv", b"Amount\n80\n")  # no order id
    await world.landing.put("shop/customers/customers.csv", b"id,name\n1,Ada\n")

    with pytest.raises(IngestRejectedError, match=r"1 file\(s\) rejected: shop/orders/orders_02"):
        await world.ingest()

    assert [order for order, _, _ in world.orders()] == ["10"]
    assert len(world.lake.table("raw", "customers")) == 1
    assert world.ledger()["shop/orders/orders_02.csv"] == ("rejected", None)
    (run,) = world.store.list_runs("ingest", 5)
    assert run.status == "failed"
    assert run.error is not None
    assert "missing required column" in run.error


async def test_rejected_file_is_not_retried_until_it_changes(world: World) -> None:
    await world.landing.put("shop/orders/orders_02.csv", b"Amount\n80\n")
    with pytest.raises(IngestRejectedError):
        await world.ingest()

    # Same bad bytes: nothing new to report, so the next run succeeds.
    assert await world.ingest() == {"shop/orders/orders_02.csv": "unchanged"}

    # The sender fixes it.
    await world.landing.put("shop/orders/orders_02.csv", b"Order ID,Amount\n11,80\n")
    assert await world.ingest() == {"shop/orders/orders_02.csv": "loaded"}
    assert world.ledger()["shop/orders/orders_02.csv"] == ("loaded", 1)


async def test_replace_source_keeps_only_the_newest_snapshot(world: World) -> None:
    await world.landing.put("shop/customers/customers_2026-11-01.csv", b"id,name\n1,Ada\n2,Alan\n")
    await world.ingest()
    assert len(world.lake.table("raw", "customers")) == 2

    await world.landing.put("shop/customers/customers_2026-11-02.csv", b"id,name\n1,Ada\n")
    await world.landing.put(
        "shop/customers/customers_2026-11-03.csv", b"id,name\n1,Ada\n2,Alan\n3,Grace\n"
    )
    outcomes = await world.ingest()

    assert outcomes == {
        "shop/customers/customers_2026-11-01.csv": "unchanged",
        "shop/customers/customers_2026-11-02.csv": "superseded",
        "shop/customers/customers_2026-11-03.csv": "loaded",
    }
    rows = world.lake.table("raw", "customers")
    assert sorted(str(r["name"]) for r in rows) == ["Ada", "Alan", "Grace"]
    assert {r["_source_file"] for r in rows} == {"shop/customers/customers_2026-11-03.csv"}


async def test_changed_older_snapshot_never_replaces_a_newer_one(world: World) -> None:
    await world.landing.put("shop/customers/customers_2026-11-01.csv", b"id,name\n1,Ada\n")
    await world.landing.put("shop/customers/customers_2026-11-02.csv", b"id,name\n1,Ada\n2,Alan\n")
    await world.ingest()
    assert len(world.lake.table("raw", "customers")) == 2

    # Someone re-saves the old file. The table must stay on the newer snapshot.
    await world.landing.put("shop/customers/customers_2026-11-01.csv", b"id,name\n1,Ada L\n")
    outcomes = await world.ingest()

    assert outcomes == {
        "shop/customers/customers_2026-11-01.csv": "superseded",
        "shop/customers/customers_2026-11-02.csv": "unchanged",
    }
    rows = world.lake.table("raw", "customers")
    assert sorted(str(r["name"]) for r in rows) == ["Ada", "Alan"]
    assert {r["_source_file"] for r in rows} == {"shop/customers/customers_2026-11-02.csv"}

    # And it is not looked at again until it changes again.
    assert set((await world.ingest()).values()) == {"unchanged"}


async def test_steps_record_files_seen_and_rows_written(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID\n10\n11\n")
    await world.landing.put("shop/customers/c.csv", b"id\n1\n")

    await world.ingest()

    (run,) = world.store.list_runs("ingest", 5)
    steps = world.store.run_details(run.run_id)["steps"]
    assert [(s["name"], s["status"], s["rows_read"], s["rows_written"]) for s in steps] == [
        ("orders", "succeeded", 1, 2),
        ("customers", "succeeded", 1, 1),
    ]


async def test_one_source_can_be_loaded_alone(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID\n10\n")
    await world.landing.put("shop/customers/c.csv", b"id\n1\n")

    assert await world.ingest(only="customers") == {"shop/customers/c.csv": "loaded"}
    assert world.orders() == []

    with pytest.raises(UnknownSourceError, match="no source named 'nope'"):
        await world.ingest(only="nope")


async def test_lake_failure_is_not_recorded_as_loaded_and_is_retried(world: World) -> None:
    await world.landing.put("shop/orders/orders_01.csv", b"Order ID\n10\n")
    real_insert = world.lake.insert

    async def broken(*args: object, **kwargs: object) -> None:
        raise ConnectionError("trino is down")

    world.lake.insert = broken  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        await world.ingest()
    assert world.ledger() == {}

    world.lake.insert = real_insert  # type: ignore[method-assign]
    assert await world.ingest() == {"shop/orders/orders_01.csv": "loaded"}
    assert len(world.orders()) == 1
