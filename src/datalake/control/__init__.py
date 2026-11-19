"""The control plane: registered processes, their configuration, and run records.

A process does its work inside `control.start(name)`:

    with control.start("ingest") as run:
        settings = run.config(IngestSettings)
        with run.step("load_orders") as step:
            ...
            step.rows_written = 1250

A process that is not registered, or is disabled, gets an exception and no configuration.
"""

from datalake.control.config import ConfigResolver
from datalake.control.errors import (
    ConfigError,
    ControlError,
    ProcessDisabledError,
    ProcessNotRegisteredError,
    SecretNotFoundError,
)
from datalake.control.model import ConfigEntry, Process, RunRecord, RunStart
from datalake.control.ports import ControlStore, SecretResolver
from datalake.control.service import Control, RunContext, StepContext

__all__ = [
    "ConfigEntry",
    "ConfigError",
    "ConfigResolver",
    "Control",
    "ControlError",
    "ControlStore",
    "Process",
    "ProcessDisabledError",
    "ProcessNotRegisteredError",
    "RunContext",
    "RunRecord",
    "RunStart",
    "SecretNotFoundError",
    "SecretResolver",
    "StepContext",
]
