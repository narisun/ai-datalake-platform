from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from datalake.agent.model import AgentEvent, ChatMessage, QueryResult
from datalake.core.security import Principal


class QueryEngine(Protocol):
    """Read-only SQL (Trino in production), run as the given role."""

    async def execute(self, sql: str, role: str, max_rows: int) -> QueryResult: ...


class ChatModel(Protocol):
    async def complete(self, messages: Sequence[ChatMessage]) -> str: ...


class AgentRuntime(Protocol):
    """Answers a question as a stream of events (LangGraph in production)."""

    def ask(
        self, question: str, principal: Principal, thread_id: str
    ) -> AsyncIterator[AgentEvent]: ...
