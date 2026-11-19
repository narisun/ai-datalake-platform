"""The SQL guard, the question workflow (with a scripted model and a fake query engine),
the Claude adapter (over a fake HTTP transport) and the `agent` process."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import anthropic
import httpx2
import pytest

from datalake.agent.asking import AgentDependencies, ensure_role_may_ask, run_ask
from datalake.agent.claude import AnthropicChatModel
from datalake.agent.errors import (
    ModelError,
    ModelRefusedError,
    NotAnswerableError,
    QueryFailedError,
    UnsafeSqlError,
)
from datalake.agent.graph import LangGraphAgent
from datalake.agent.model import AgentEvent, ChatMessage, QueryResult
from datalake.agent.sql_guard import ensure_read_only
from datalake.agent.testing import FakeQueryEngine, ScriptedAgentRuntime, ScriptedChatModel
from datalake.control import ConfigResolver, Control
from datalake.control.testing import (
    InMemoryControlStore,
    SequentialUuids,
    StaticSecrets,
    TickingClock,
)
from datalake.core.security import Classification, Principal
from datalake.policy.policy_file import load_access_rules
from datalake.policy.service import RoleAccessPolicy
from datalake.search.retriever import HybridRetriever
from datalake.search.testing import (
    HashingEmbedder,
    InMemoryLexicalIndex,
    InMemoryVectorIndex,
    PassThroughReranker,
)
from tests.unit.helpers import chunk

POLICY = Path(__file__).parents[2] / "dbt" / "access.yml"
ANALYST = Principal("analyst_sales", frozenset({"analyst_sales"}))
REVENUE = QueryResult(columns=("revenue",), rows=((1250.5,),))
SQL = "select sum(amount) as revenue from lake.marts.fct_orders"


# --- SQL guard ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "select 1",
        "with t as (select 1 as a) select a from t",
        "select a from lake.marts.x union all select a from lake.marts.y",
        "SELECT customer_id, count(*) FROM lake.marts.fct_orders GROUP BY 1 ORDER BY 2 DESC",
    ],
)
def test_guard_allows_read_only_queries(sql: str) -> None:
    assert ensure_read_only(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "delete from lake.marts.fct_orders",
        "insert into lake.marts.t select 1",
        "drop table lake.marts.fct_orders",
        "create table lake.marts.t as select 1 as a",
        "update lake.marts.t set a = 1",
        "select 1; select 2",
        "select 1; drop table lake.marts.t",
        "call system.sync_partition_metadata('a', 'b', 'FULL')",
        "set session query_max_run_time = '1h'",
        "grant select on lake.marts.t to user x",
        "show tables",
        "",
        "select from where",
    ],
)
def test_guard_refuses_everything_else(sql: str) -> None:
    with pytest.raises(UnsafeSqlError):
        ensure_read_only(sql)


# --- the workflow ------------------------------------------------------------------------


async def _retriever() -> HybridRetriever:
    chunks = [
        chunk(
            "orders", "Table lake.marts.fct_orders: one row per order. Columns: amount, order_date"
        ),
        chunk("customers", "Table lake.marts.dim_customer: one row per customer. Columns: name"),
    ]
    lexical = InMemoryLexicalIndex()
    vector = InMemoryVectorIndex(HashingEmbedder())
    await lexical.upsert(chunks)
    await vector.upsert(chunks)
    return HybridRetriever(
        lexical=lexical,
        vector=vector,
        reranker=PassThroughReranker(),
        policy=RoleAccessPolicy(load_access_rules(POLICY)),
    )


async def _ask(agent: LangGraphAgent, question: str = "total order amount") -> list[AgentEvent]:
    return [event async for event in agent.ask(question, ANALYST, "thread-1")]


async def test_answers_a_question_from_the_query_result() -> None:
    model = ScriptedChatModel([f"```sql\n{SQL};\n```", "Revenue is 1,250.50.\nSource: fct_orders"])
    engine = FakeQueryEngine(REVENUE)
    events = await _ask(LangGraphAgent(await _retriever(), model, engine, max_rows=10))

    assert [event.kind for event in events] == ["status", "status", "sql", "status", "final"]
    assert events[2].text == SQL
    assert events[-1].text == "Revenue is 1,250.50.\nSource: fct_orders"
    # The query ran as the asker's role, with the row limit.
    assert [(q.sql, q.role, q.max_rows) for q in engine.executed] == [(SQL, "analyst_sales", 10)]
    # The model saw the catalog extract when drafting and the rows when answering.
    assert "lake.marts.fct_orders" in model.prompts[0][1].content
    assert "1250.5" in model.prompts[1][1].content


class _FailingEngine:
    """Fails the first `failures` queries, then returns the result."""

    def __init__(self, failures: int, error: QueryFailedError) -> None:
        self._failures = failures
        self._error = error
        self.queries: list[str] = []

    async def execute(self, sql: str, role: str, max_rows: int) -> QueryResult:
        self.queries.append(sql)
        if len(self.queries) <= self._failures:
            raise self._error
        return REVENUE


async def test_retries_once_with_the_error_when_the_query_fails() -> None:
    model = ScriptedChatModel(
        ["```sql\nselect total from lake.marts.fct_orders\n```", f"```sql\n{SQL}\n```", "1,250.50"]
    )
    engine = _FailingEngine(1, QueryFailedError("Column 'total' cannot be resolved"))
    events = await _ask(LangGraphAgent(await _retriever(), model, engine))

    assert engine.queries == ["select total from lake.marts.fct_orders", SQL]
    assert events[-1] == AgentEvent("final", "1,250.50")
    retry_prompt = model.prompts[1][1].content
    assert "Column 'total' cannot be resolved" in retry_prompt
    assert "select total from" in retry_prompt


async def test_gives_up_after_the_second_failure() -> None:
    model = ScriptedChatModel(["```sql\nselect 1\n```", "```sql\nselect 2\n```"])
    engine = _FailingEngine(5, UnsafeSqlError("only SELECT queries are allowed"))
    events = await _ask(LangGraphAgent(await _retriever(), model, engine))

    assert len(engine.queries) == 2
    assert events[-1].kind == "final"
    assert "could not run a query" in events[-1].text
    assert len(model.prompts) == 2  # no answer was written from a failed query


async def test_says_so_when_the_model_cannot_answer() -> None:
    model = ScriptedChatModel(["CANNOT_ANSWER: there is no table of suppliers."])
    engine = FakeQueryEngine(REVENUE)
    events = await _ask(LangGraphAgent(await _retriever(), model, engine))

    assert events[-1] == AgentEvent(
        "final", "I cannot answer that: there is no table of suppliers."
    )
    assert engine.executed == []


async def test_stops_before_the_model_when_nothing_is_found() -> None:
    model = ScriptedChatModel([])
    events = await _ask(LangGraphAgent(await _retriever(), model, FakeQueryEngine()), "zzzz qqqq")

    assert events[-1].kind == "final"
    assert "could not find any data" in events[-1].text
    assert model.prompts == []


async def test_a_question_needs_exactly_one_role() -> None:
    agent = LangGraphAgent(await _retriever(), ScriptedChatModel([]), FakeQueryEngine())
    with pytest.raises(NotAnswerableError):
        _ = [e async for e in agent.ask("q", Principal("u", frozenset({"a", "b"})), "t")]


# --- the Claude adapter ------------------------------------------------------------------


def _claude(reply: dict[str, Any] | int, seen: list[dict[str, Any]]) -> AnthropicChatModel:
    def handle(request: httpx2.Request) -> httpx2.Response:
        seen.append({"body": json.loads(request.content), "headers": dict(request.headers)})
        if isinstance(reply, int):
            error = {"type": "error", "error": {"type": "api_error", "message": "boom"}}
            return httpx2.Response(reply, json=error)
        message = {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-5-5",
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "content": [{"type": "text", "text": "hello"}],
            "usage": {"input_tokens": 12, "output_tokens": 3},
        }
        return httpx2.Response(200, json=message | reply)

    client = anthropic.AsyncAnthropic(
        api_key="test",
        max_retries=0,
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handle)),
    )
    return AnthropicChatModel(client, model="claude-opus-5-5", effort="low")


MESSAGES: Sequence[ChatMessage] = (ChatMessage("system", "be brief"), ChatMessage("user", "hi"))


async def test_claude_sends_the_request_and_counts_tokens() -> None:
    seen: list[dict[str, Any]] = []
    model = _claude({}, seen)

    assert await model.complete(MESSAGES) == "hello"
    body = seen[0]["body"]
    assert body["model"] == "claude-opus-5-5"
    assert body["system"][0]["text"] == "be brief"
    assert body["messages"] == [{"role": "user", "content": "hi"}]
    assert body["output_config"] == {"effort": "low"}
    assert body["fallbacks"] == "default"
    assert "server-side-fallback-2026-07-01" in seen[0]["headers"]["anthropic-beta"]
    assert (model.input_tokens, model.output_tokens) == (12, 3)


async def test_claude_refusal_is_its_own_error() -> None:
    model = _claude({"stop_reason": "refusal", "content": []}, [])
    with pytest.raises(ModelRefusedError):
        await model.complete(MESSAGES)


@pytest.mark.parametrize("reply", [{"stop_reason": "max_tokens"}, {"content": []}, 401, 429, 500])
async def test_claude_failures_become_model_errors(reply: dict[str, Any] | int) -> None:
    model = _claude(reply, [])
    with pytest.raises(ModelError):
        await model.complete(MESSAGES)


# --- the agent process -------------------------------------------------------------------

CONFIG = {
    "access_policy_file": str(POLICY),
    "openmetadata_url": "http://om",
    "openmetadata_token": "t",
    "opensearch_url": "http://os",
    "embed_url": "http://embed",
    "rerank_url": "http://rerank",
    "trino_host": "trino",
    "agent_trino_password": "p",
    "anthropic_api_key": "k",
}


def _control(config: dict[str, str]) -> tuple[Control, InMemoryControlStore]:
    store = InMemoryControlStore()
    store.register_process("agent", "questions")
    for key, value in config.items():
        store.set_config(key, value, None, "test")
    control = Control(
        store=store,
        resolver=ConfigResolver({}, StaticSecrets({}), strict=True),
        clock=TickingClock(),
        ids=SequentialUuids(),
        environment={},
    )
    return control, store


def test_only_roles_within_the_limit_may_ask() -> None:
    rules = load_access_rules(POLICY)
    assert ensure_role_may_ask(rules, "analyst_sales", Classification.INTERNAL) == ANALYST
    with pytest.raises(NotAnswerableError, match="above internal"):
        ensure_role_may_ask(rules, "support_sales", Classification.INTERNAL)
    with pytest.raises(NotAnswerableError, match="no role named"):
        ensure_role_may_ask(rules, "nobody", Classification.RESTRICTED)


async def test_a_question_is_recorded_as_a_run() -> None:
    control, store = _control(CONFIG)
    runtime = ScriptedAgentRuntime(
        [AgentEvent("status", "x"), AgentEvent("sql", SQL), AgentEvent("final", "42")]
    )
    seen: list[AgentEvent] = []

    answer = await run_ask(
        control,
        lambda _settings: AgentDependencies(load_access_rules(POLICY), runtime),
        "how many?",
        "analyst_sales",
        on_event=seen.append,
    )

    assert (answer.text, answer.sql) == ("42", SQL)
    assert len(seen) == 3
    assert [run.status for run in store.list_runs(None, 10)] == ["succeeded"]


async def test_a_refused_role_fails_the_run_without_asking() -> None:
    control, store = _control(CONFIG | {"agent_max_classification": "internal"})
    runtime = ScriptedAgentRuntime([AgentEvent("final", "42")])

    with pytest.raises(NotAnswerableError):
        await run_ask(
            control,
            lambda _settings: AgentDependencies(load_access_rules(POLICY), runtime),
            "q",
            "support_sales",
        )
    assert runtime.questions == []
    assert [run.status for run in store.list_runs(None, 10)] == ["failed"]
