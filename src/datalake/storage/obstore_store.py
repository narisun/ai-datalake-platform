"""ObjectStore port on top of the `obstore` library.

The obstore store is injected, so the same adapter serves ADLS Gen2 in production,
Azurite in the dev profile and an in-memory store in tests.
"""

from __future__ import annotations

from collections.abc import Sequence

import obstore
from obstore.store import AzureStore, ObjectStore

from datalake.core.errors import NotFoundError


class ObstoreObjectStore:
    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    async def put(self, path: str, data: bytes) -> None:
        await obstore.put_async(self._store, path, data)

    async def get(self, path: str) -> bytes:
        try:
            result = await obstore.get_async(self._store, path)
            return bytes(await result.bytes_async())
        except FileNotFoundError:
            raise NotFoundError(f"no object at {path}") from None

    async def exists(self, path: str) -> bool:
        try:
            await obstore.head_async(self._store, path)
        except FileNotFoundError:
            return False
        return True

    async def list(self, prefix: str) -> Sequence[str]:
        # Object stores only match whole path segments, so list the enclosing
        # directory and apply the exact string prefix here.
        directory = prefix.rpartition("/")[0]
        stream = obstore.list(  # pyright: ignore[reportUnknownMemberType]
            self._store, prefix=directory or None, return_arrow=False
        )
        paths: list[str] = []
        async for batch in stream:
            paths.extend(meta["path"] for meta in batch)
        return sorted(path for path in paths if path.startswith(prefix))

    async def delete(self, path: str) -> None:
        try:
            await obstore.delete_async(self._store, path)
        except FileNotFoundError:
            return


def create_adls_gen2_store(
    account: str,
    container: str,
    managed_identity_client_id: str | None = None,
) -> AzureStore:
    """An obstore store for ADLS Gen2.

    With a client id, a user-assigned managed identity is used. Without one, obstore
    falls back to its environment-based credential chain. No account key is accepted:
    shared-key access is disabled on the storage account by design.
    """
    if managed_identity_client_id is None:
        return AzureStore(container_name=container, account_name=account)
    return AzureStore(
        container_name=container,
        account_name=account,
        client_id=managed_identity_client_id,
        use_fabric_endpoint=False,
    )


def create_azurite_store(container: str, prefix: str | None = None) -> AzureStore:
    """An obstore store for Azurite, Azure's local emulator, on its default port.

    Azurite speaks the same Blob API as ADLS Gen2, so code tested against it uses the
    same adapter and the same wire protocol as production. It does not emulate the
    hierarchical namespace: renames and directory operations are not covered.
    """
    return AzureStore(container_name=container, prefix=prefix, use_emulator=True)


def create_landing_store(
    account: str,
    container: str,
    endpoint: str | None = None,
    account_key: str | None = None,
    managed_identity_client_id: str | None = None,
) -> AzureStore:
    """The store for a container, on real ADLS Gen2 or on Azurite.

    Azurite: pass its `endpoint` (for example http://azurite:10000/devstoreaccount1)
    and its account key. ADLS Gen2: pass neither, and the managed identity's client id.
    """
    if endpoint is None:
        return create_adls_gen2_store(account, container, managed_identity_client_id)
    if account_key is None:
        raise ValueError("an endpoint (Azurite) needs its account key")
    return AzureStore(
        container_name=container,
        account_name=account,
        endpoint=endpoint,
        account_key=account_key,
        client_options={"allow_http": endpoint.startswith("http://")},
    )
