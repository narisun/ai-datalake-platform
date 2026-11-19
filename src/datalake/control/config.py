"""Turns configuration rows into a typed, validated settings object.

Resolution order for each field of the settings model:

1. The control database (the process's own value, else the shared one).
2. If not strict: the environment variable `DL_<FIELD>`, with a warning. This is the
   bridge from `.env` to the database; strict mode switches it off.
3. The field's default, if it has one. Otherwise the key is reported as missing.

A value may contain references, resolved where the process runs:
`${env:NAME}` is an environment variable and `${secret:name}` is fetched from the
secret store. Secrets are never written to the snapshot.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping

from pydantic import BaseModel, ValidationError

from datalake.control.errors import ConfigError, SecretNotFoundError
from datalake.control.ports import SecretResolver

_REFERENCE = re.compile(r"\$\{(env|secret):([A-Za-z0-9_.-]+)\}")
_SENSITIVE_NAME = re.compile(r"password|secret|token|api_?key|credential", re.IGNORECASE)
_REDACTED = "***"

_log = logging.getLogger("datalake.control")

Snapshot = dict[str, dict[str, str]]


class ConfigResolver:
    def __init__(
        self,
        environment: Mapping[str, str],
        secrets: SecretResolver,
        strict: bool = False,
        env_prefix: str = "DL_",
    ) -> None:
        self._environment = environment
        self._secrets = secrets
        self._strict = strict
        self._env_prefix = env_prefix

    def resolve[T: BaseModel](
        self, model: type[T], database_values: Mapping[str, str]
    ) -> tuple[T, Snapshot]:
        """Build `model` and a snapshot that is safe to store. Raise ConfigError on any problem."""
        values: dict[str, str] = {}
        snapshot: Snapshot = {}
        problems: list[str] = []

        for name, field in model.model_fields.items():
            source = "database"
            raw = database_values.get(name)
            if raw is None and not self._strict:
                raw = self._environment.get(self._env_prefix + name.upper())
                if raw is not None:
                    source = "env"
                    _log.warning(
                        "configuration key %r is still served from the environment; "
                        "move it with `dl config set`",
                        name,
                    )
            if raw is None:
                if field.is_required():
                    problems.append(f"{name}: missing")
                continue

            try:
                value = self._expand(raw)
            except ConfigError as exc:
                problems.append(f"{name}: {exc}")
                continue
            values[name] = value
            snapshot[name] = {"value": self._display(name, raw, value), "source": source}

        if not problems:
            try:
                return model.model_validate(values), snapshot
            except ValidationError as exc:
                problems = [
                    f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
                    for error in exc.errors()
                ]
        raise ConfigError("invalid configuration: " + "; ".join(problems))

    def _expand(self, raw: str) -> str:
        def replace(match: re.Match[str]) -> str:
            kind, name = match.group(1), match.group(2)
            if kind == "env":
                if name not in self._environment:
                    raise ConfigError(f"environment variable {name} is not set")
                return self._environment[name]
            try:
                return self._secrets.get(name)
            except SecretNotFoundError as exc:
                raise ConfigError(str(exc)) from exc

        return _REFERENCE.sub(replace, raw)

    @staticmethod
    def _display(name: str, raw: str, value: str) -> str:
        """What may be stored: a secret reference stays a reference, never its value."""
        if "${secret:" in raw:
            return raw
        if _SENSITIVE_NAME.search(name):
            return _REDACTED
        return value
