from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class ObjectStore(Protocol):
    """Blob storage addressed by path (ADLS Gen2 in production)."""

    async def put(self, path: str, data: bytes) -> None: ...

    async def get(self, path: str) -> bytes:
        """Raise NotFoundError if the object does not exist."""
        ...

    async def exists(self, path: str) -> bool: ...

    async def list(self, prefix: str) -> Sequence[str]:
        """Paths under the prefix, sorted."""
        ...

    async def delete(self, path: str) -> None:
        """Deleting a missing object is not an error."""
        ...


class TableCatalog(Protocol):
    """The Iceberg catalog (Polaris in production)."""

    async def list_namespaces(self) -> Sequence[str]: ...

    async def list_tables(self, namespace: str) -> Sequence[str]: ...

    async def table_exists(self, namespace: str, table: str) -> bool: ...
