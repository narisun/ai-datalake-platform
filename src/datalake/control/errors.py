from __future__ import annotations

from datalake.core.errors import DatalakeError


class ControlError(DatalakeError):
    """Base class for control-plane errors."""


class ProcessNotRegisteredError(ControlError):
    """The process is unknown to the control database, so it gets no configuration."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"process {name!r} is not registered; register it with `dl process register {name}`"
        )
        self.name = name


class ProcessDisabledError(ControlError):
    """The process is registered but switched off."""

    def __init__(self, name: str) -> None:
        super().__init__(f"process {name!r} is disabled; enable it with `dl process enable {name}`")
        self.name = name


class ConfigError(ControlError):
    """Configuration is missing, refers to something that does not exist, or is invalid."""


class SecretNotFoundError(ControlError):
    """A `${secret:name}` reference names a secret the resolver cannot find."""
