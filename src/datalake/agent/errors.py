from __future__ import annotations

from datalake.core.errors import DatalakeError


class QueryFailedError(DatalakeError):
    """A query was refused or failed. The message is safe to show the model."""


class UnsafeSqlError(QueryFailedError):
    """The SQL is not a single read-only query."""


class ModelError(DatalakeError):
    """The model could not be reached, or returned nothing usable."""


class ModelRefusedError(ModelError):
    """The model declined the request."""


class NotAnswerableError(DatalakeError):
    """The question cannot be put to the agent as asked."""
