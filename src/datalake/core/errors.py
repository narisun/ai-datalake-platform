class DatalakeError(Exception):
    """Base class for every error the SDK raises on purpose."""


class PolicyError(DatalakeError):
    """The access policy is missing, malformed or inconsistent."""


class NotFoundError(DatalakeError):
    """A requested entity, object or table does not exist."""


class ProfileNotAvailableError(DatalakeError):
    """The requested deployment profile has no adapters wired yet."""
