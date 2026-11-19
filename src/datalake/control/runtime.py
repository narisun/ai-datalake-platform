"""Small real implementations of the control ports that need no external system."""

from __future__ import annotations

import secrets
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from datalake.control.errors import SecretNotFoundError
from datalake.control.ports import SecretResolver


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class Uuid7Generator:
    """Time-ordered UUIDs (version 7), so run ids sort by when they started."""

    def new_id(self) -> str:
        milliseconds = time.time_ns() // 1_000_000
        value = (milliseconds & 0xFFFF_FFFF_FFFF) << 80
        value |= 0x7 << 76
        value |= secrets.randbits(12) << 64
        value |= 0b10 << 62
        value |= secrets.randbits(62)
        return str(uuid.UUID(int=value))


class EnvironmentSecrets:
    """Secrets from environment variables: `${secret:trino-password}` reads
    `DL_SECRET_TRINO_PASSWORD`. For development; production uses Azure Key Vault."""

    def __init__(self, environment: Mapping[str, str], prefix: str = "DL_SECRET_") -> None:
        self._environment = environment
        self._prefix = prefix

    def get(self, name: str) -> str:
        variable = self._prefix + name.upper().replace("-", "_").replace(".", "_")
        try:
            return self._environment[variable]
        except KeyError:
            raise SecretNotFoundError(
                f"secret {name!r} not found (expected environment variable {variable})"
            ) from None


class FileSecrets:
    """Secrets as files in one directory: `${secret:openmetadata-token}` reads the file
    `openmetadata-token`. For secrets that are created at run time, not known up front."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def get(self, name: str) -> str:
        if "/" in name or "\\" in name or name.startswith("."):
            raise SecretNotFoundError(f"secret name {name!r} is not allowed")
        try:
            return (self._directory / name).read_text(encoding="utf-8").strip()
        except OSError:
            raise SecretNotFoundError(
                f"secret {name!r} not found (expected file {self._directory / name})"
            ) from None


class ChainedSecrets:
    """Tries each resolver in turn and returns the first secret found."""

    def __init__(self, resolvers: Sequence[SecretResolver]) -> None:
        self._resolvers = tuple(resolvers)

    def get(self, name: str) -> str:
        reasons: list[str] = []
        for resolver in self._resolvers:
            try:
                return resolver.get(name)
            except SecretNotFoundError as exc:
                reasons.append(str(exc))
        raise SecretNotFoundError("; ".join(reasons) or f"secret {name!r} not found")
