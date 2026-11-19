"""One suite, run against the fake and the real adapter, so they cannot drift apart.

The `azurite` case speaks the real Azure Blob protocol to Azure's emulator. It runs
when DL_TEST_AZURITE_CONTAINER names an existing container in an Azurite listening on
127.0.0.1:10000, and is skipped otherwise.
"""

from __future__ import annotations

import os
import uuid

import pytest
from obstore.store import MemoryStore

from datalake.core.errors import NotFoundError
from datalake.storage.obstore_store import (
    ObstoreObjectStore,
    create_adls_gen2_store,
    create_azurite_store,
)
from datalake.storage.ports import ObjectStore
from datalake.storage.testing import InMemoryObjectStore


@pytest.fixture(params=["fake", "obstore-memory", "azurite"])
def store(request: pytest.FixtureRequest) -> ObjectStore:
    if request.param == "fake":
        return InMemoryObjectStore()
    if request.param == "obstore-memory":
        return ObstoreObjectStore(MemoryStore())
    container = os.environ.get("DL_TEST_AZURITE_CONTAINER")
    if not container:
        pytest.skip("set DL_TEST_AZURITE_CONTAINER to run against Azurite")
    # Each test gets its own folder, so runs do not see each other's objects.
    return ObstoreObjectStore(create_azurite_store(container, prefix=uuid.uuid4().hex))


async def test_put_then_get_returns_the_same_bytes(store: ObjectStore) -> None:
    await store.put("raw/orders/part-0.parquet", b"\x00\x01data")

    assert await store.get("raw/orders/part-0.parquet") == b"\x00\x01data"


async def test_put_overwrites(store: ObjectStore) -> None:
    await store.put("a.txt", b"one")
    await store.put("a.txt", b"two")

    assert await store.get("a.txt") == b"two"


async def test_get_missing_raises_not_found(store: ObjectStore) -> None:
    with pytest.raises(NotFoundError):
        await store.get("missing.txt")


async def test_exists(store: ObjectStore) -> None:
    await store.put("a.txt", b"x")

    assert await store.exists("a.txt")
    assert not await store.exists("b.txt")


async def test_list_returns_sorted_paths_under_the_prefix(store: ObjectStore) -> None:
    for path in ["raw/b.parquet", "raw/a.parquet", "raw/sub/c.parquet", "marts/d.parquet"]:
        await store.put(path, b"x")

    assert await store.list("raw/") == ["raw/a.parquet", "raw/b.parquet", "raw/sub/c.parquet"]
    assert await store.list("nothing/") == []
    assert len(await store.list("")) == 4


async def test_list_prefix_need_not_end_at_a_path_segment(store: ObjectStore) -> None:
    await store.put("raw/orders_2026.parquet", b"x")
    await store.put("raw/customers.parquet", b"x")

    assert await store.list("raw/ord") == ["raw/orders_2026.parquet"]


async def test_delete_removes_and_is_idempotent(store: ObjectStore) -> None:
    await store.put("a.txt", b"x")

    await store.delete("a.txt")
    await store.delete("a.txt")

    assert not await store.exists("a.txt")


def test_adls_store_can_be_built_without_connecting() -> None:
    create_adls_gen2_store("account", "lake")
    create_adls_gen2_store("account", "lake", managed_identity_client_id="0000")
