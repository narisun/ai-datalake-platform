from __future__ import annotations

from collections.abc import Mapping, Sequence

from datalake.core.errors import NotFoundError


class InMemoryObjectStore:
    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    async def put(self, path: str, data: bytes) -> None:
        self._objects[path] = data

    async def get(self, path: str) -> bytes:
        try:
            return self._objects[path]
        except KeyError:
            raise NotFoundError(f"no object at {path}") from None

    async def exists(self, path: str) -> bool:
        return path in self._objects

    async def list(self, prefix: str) -> Sequence[str]:
        return sorted(path for path in self._objects if path.startswith(prefix))

    async def delete(self, path: str) -> None:
        self._objects.pop(path, None)


class InMemoryTableCatalog:
    def __init__(self, tables: Mapping[str, Sequence[str]] | None = None) -> None:
        self._tables = {ns: set(names) for ns, names in (tables or {}).items()}

    def add_table(self, namespace: str, table: str) -> None:
        self._tables.setdefault(namespace, set()).add(table)

    async def list_namespaces(self) -> Sequence[str]:
        return sorted(self._tables)

    async def list_tables(self, namespace: str) -> Sequence[str]:
        return sorted(self._tables.get(namespace, set()))

    async def table_exists(self, namespace: str, table: str) -> bool:
        return table in self._tables.get(namespace, set())
