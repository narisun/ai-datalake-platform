from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass

from datalake.agent.model import AgentEvent, ChatMessage, QueryResult
from datalake.core.security import Principal


@dataclass(frozen=True, slots=True)
class ExecutedQuery:
    sql: str
    role: str
    max_rows: int


class FakeQueryEngine:
    """Returns one canned result and records every query."""

    def __init__(self, result: QueryResult | None = None) -> None:
        self._result = result or QueryResult(columns=(), rows=())
        self.executed: list[ExecutedQuery] = []

    async def execute(self, sql: str, role: str, max_rows: int) -> QueryResult:
        self.executed.append(ExecutedQuery(sql, role, max_rows))
        rows = self._result.rows
        return QueryResult(
            columns=self._result.columns,
            rows=rows[:max_rows],
            truncated=len(rows) > max_rows,
        )


class ScriptedChatModel:
    """Replies with the given answers in order and records the prompts."""

    def __init__(self, replies: Sequence[str]) -> None:
        self._replies = list(replies)
        self.prompts: list[Sequence[ChatMessage]] = []

    async def complete(self, messages: Sequence[ChatMessage]) -> str:
        self.prompts.append(messages)
        if not self._replies:
            raise AssertionError("ScriptedChatModel ran out of replies")
        return self._replies.pop(0)


class ScriptedAgentRuntime:
    """Streams the same events for every question."""

    def __init__(self, events: Sequence[AgentEvent]) -> None:
        self._events = tuple(events)
        self.questions: list[tuple[str, str, str]] = []

    async def ask(
        self, question: str, principal: Principal, thread_id: str
    ) -> AsyncIterator[AgentEvent]:
        self.questions.append((question, principal.user_id, thread_id))
        for event in self._events:
            yield event
