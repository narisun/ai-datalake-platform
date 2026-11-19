"""Refuses anything that is not a single read-only query.

Trino's access rules already stop a role from writing. This check comes first anyway:
SQL written by a language model is not trusted to be what it was asked for, and a
second, independent barrier costs little.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from datalake.agent.errors import UnsafeSqlError

_FORBIDDEN: tuple[type[Any], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,  # anything sqlglot does not model: CALL, GRANT, SET SESSION, ...
    exp.Use,
    exp.Set,
    exp.Grant,
)


def ensure_read_only(sql: str) -> str:
    """Return the query, normalised, or raise UnsafeSqlError saying why not."""
    try:
        statements = [s for s in sqlglot.parse(sql, dialect="trino") if s is not None]
    except SqlglotError as exc:
        raise UnsafeSqlError(f"the SQL could not be parsed: {str(exc).splitlines()[0]}") from exc
    if len(statements) != 1:
        raise UnsafeSqlError(f"expected one statement, found {len(statements)}")
    statement = statements[0]
    if not isinstance(statement, exp.Query):
        raise UnsafeSqlError(f"only SELECT queries are allowed, not {statement.key.upper()}")
    for node in statement.walk():
        if isinstance(node, _FORBIDDEN):
            raise UnsafeSqlError(f"the query contains {node.key.upper()}, which changes data")
    return statement.sql(dialect="trino")
