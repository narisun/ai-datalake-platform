"""QueryEngine on Trino: read-only SQL, run as the caller's role."""

# The trino client ships no type information.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false

from __future__ import annotations

import asyncio
from typing import Any

import trino

from datalake.agent.errors import QueryFailedError
from datalake.agent.model import QueryResult
from datalake.agent.sql_guard import ensure_read_only
from datalake.policy.trino_rules import trino_user_for


class TrinoQueryEngine:
    """Signs in as the platform's own account and acts as the given role.

    That account can do nothing as itself, and Trino lets it act only as roles that
    exist in the access policy, so a query can never see more than its role may.
    """

    def __init__(
        self,
        host: str,
        port: int,
        account: str,
        password: str,
        scheme: str = "https",
        ca_file: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._account = account
        self._password = password
        self._scheme = scheme
        self._ca_file = ca_file

    def _run(self, sql: str, user: str, limit: int) -> tuple[list[str], list[tuple[object, ...]]]:
        options: dict[str, Any] = {} if self._ca_file is None else {"verify": self._ca_file}
        connection = trino.dbapi.connect(
            host=self._host,
            port=self._port,
            http_scheme=self._scheme,
            user=user,
            auth=trino.auth.BasicAuthentication(self._account, self._password),
            **options,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            rows = [tuple(row) for row in cursor.fetchmany(limit)]
            cursor.cancel()
            return [column[0] for column in cursor.description or []], rows
        except trino.exceptions.TrinoQueryError as exc:
            raise QueryFailedError(str(exc.message)) from exc
        finally:
            connection.close()

    async def execute(self, sql: str, role: str, max_rows: int) -> QueryResult:
        query = ensure_read_only(sql)
        # Rows are read from the query as written, never from a wrapping SELECT, which
        # would lose its ORDER BY. One row more than wanted shows the result was cut short.
        columns, rows = await asyncio.to_thread(
            self._run, query, trino_user_for(role), max_rows + 1
        )
        return QueryResult(
            columns=tuple(columns), rows=tuple(rows[:max_rows]), truncated=len(rows) > max_rows
        )
