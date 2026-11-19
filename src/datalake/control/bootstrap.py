"""Builds the control service. The only place that reads the process environment.

Bootstrap settings, which cannot live in the control database because they are needed
to reach it:

    DL_DATABASE_URL    the control database (required)
    DL_STRICT_CONFIG   "1" to refuse configuration from environment variables
    DL_GIT_COMMIT      recorded on each run (set when the image is built)
    DL_SECRETS_DIR     optional directory of secret files, one file per secret
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values

from datalake.control.config import ConfigResolver
from datalake.control.errors import ConfigError
from datalake.control.ports import ControlStore, SecretResolver
from datalake.control.postgres import PostgresControlStore
from datalake.control.runtime import (
    ChainedSecrets,
    EnvironmentSecrets,
    FileSecrets,
    SystemClock,
    Uuid7Generator,
)
from datalake.control.service import Control

DATABASE_URL = "DL_DATABASE_URL"


def load_environment(dotenv_path: Path | None = Path(".env")) -> dict[str, str]:
    """The real environment, with a `.env` file filling in anything not already set."""
    environment: dict[str, str] = {}
    if dotenv_path is not None and dotenv_path.is_file():
        environment.update(
            {key: value for key, value in dotenv_values(dotenv_path).items() if value is not None}
        )
    environment.update(os.environ)
    return environment


def database_url(environment: Mapping[str, str]) -> str:
    try:
        return environment[DATABASE_URL]
    except KeyError:
        raise ConfigError(
            f"{DATABASE_URL} is not set; it is the one setting that must come from the "
            "environment or .env"
        ) from None


def open_store(environment: Mapping[str, str]) -> PostgresControlStore:
    return PostgresControlStore.connect(database_url(environment))


def build_secrets(environment: Mapping[str, str]) -> SecretResolver:
    """Environment variables first, then files in DL_SECRETS_DIR if it is set."""
    resolvers: list[SecretResolver] = [EnvironmentSecrets(environment)]
    directory = environment.get("DL_SECRETS_DIR")
    if directory:
        resolvers.append(FileSecrets(Path(directory)))
    return ChainedSecrets(resolvers)


def build_control(environment: Mapping[str, str], store: ControlStore | None = None) -> Control:
    return Control(
        store=store if store is not None else open_store(environment),
        resolver=ConfigResolver(
            environment=environment,
            secrets=build_secrets(environment),
            strict=environment.get("DL_STRICT_CONFIG", "") == "1",
        ),
        clock=SystemClock(),
        ids=Uuid7Generator(),
        environment=environment,
        host=socket.gethostname(),
    )
