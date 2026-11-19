"""Agent scenarios against the live dev stack, without a model. Run inside the airflow
container, with the platform's own Python:

    /opt/datalake/venv/bin/python /opt/datalake/dev/scenarios_agent.py

Everything is real except the language model: catalog search goes to OpenSearch and the
model services, and queries go to Trino as the asker's role. The model's replies are
scripted, so the checks show what the platform does with good SQL, with SQL the role may
not run, and with SQL that tries to change data. It needs the nightly pipeline to have run.
"""

from __future__ import annotations

import asyncio
import sys

import httpx

from datalake.agent.asking import AgentSettings, ensure_role_may_ask
from datalake.agent.graph import LangGraphAgent
from datalake.agent.model import AgentEvent
from datalake.agent.testing import ScriptedChatModel
from datalake.agent.trino_query import TrinoQueryEngine
from datalake.control.bootstrap import build_control, load_environment
from datalake.core.security import Classification
from datalake.policy.policy_file import load_access_rules
from datalake.policy.service import RoleAccessPolicy
from datalake.search.opensearch import OpenSearchChunkIndex, VectorSearch
from datalake.search.retriever import HybridRetriever
from datalake.search.tei import TeiEmbedder, TeiReranker

MARTS = "lake.analytics_marts"
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'}  {name}{f'  [{detail}]' if detail and not ok else ''}")
    if not ok:
        failures.append(name)


def sql(text: str) -> str:
    return f"```sql\n{text}\n```"


async def main() -> int:
    control = build_control(load_environment())
    with control.start("agent") as run:
        settings = run.config(AgentSettings)
        rules = load_access_rules(settings.access_policy_file)
        engine = TrinoQueryEngine(
            host=settings.trino_host,
            port=settings.trino_port,
            account=settings.agent_trino_user,
            password=settings.agent_trino_password,
            scheme=settings.trino_scheme,
            ca_file=str(settings.trino_ca_file) if settings.trino_ca_file else None,
        )
        async with httpx.AsyncClient(timeout=120) as http:
            index = OpenSearchChunkIndex(
                http,
                settings.opensearch_url,
                settings.search_index,
                TeiEmbedder(http, settings.embed_url),
            )
            retriever = HybridRetriever(
                lexical=index,
                vector=VectorSearch(index),
                reranker=TeiReranker(http, settings.rerank_url),
                policy=RoleAccessPolicy(rules),
            )

            async def ask(
                role: str, question: str, replies: list[str]
            ) -> tuple[list[AgentEvent], ScriptedChatModel]:
                principal = ensure_role_may_ask(rules, role, Classification.CONFIDENTIAL)
                model = ScriptedChatModel(replies)
                agent = LangGraphAgent(retriever, model, engine, max_rows=5)
                return [e async for e in agent.ask(question, principal, run.run_id)], model

            # 1. A good query runs as the role and its rows reach the answer step.
            events, model = await ask(
                "analyst_sales",
                "What is revenue by day?",
                [sql(f"select * from {MARTS}.agg_daily_revenue"), "ANSWER"],
            )
            check("a good query is answered", events[-1] == AgentEvent("final", "ANSWER"))
            check(
                "the catalog extract names the revenue table",
                "agg_daily_revenue" in model.prompts[0][1].content,
            )
            check(
                "the result is cut to the row limit and says so",
                "5 shown, result was cut short" in model.prompts[1][1].content,
                model.prompts[1][1].content[-300:],
            )

            # 2. The analyst may not see email. Trino refuses, the error goes back to the
            #    model once, and a second refusal ends the question.
            events, model = await ask(
                "analyst_sales",
                "List customer email addresses",
                [sql(f"select email from {MARTS}.dim_customer")] * 2,
            )
            check(
                "a hidden column is refused by Trino, twice, then given up",
                "could not run a query" in events[-1].text and len(model.prompts) == 2,
                events[-1].text,
            )
            # A description may say in words that email exists; the column itself is absent.
            check(
                "the analyst's catalog extract does not list the email column",
                "- email (" not in model.prompts[0][1].content,
            )

            # 3. Support may see email, masked.
            events, model = await ask(
                "support_sales",
                "List customer email addresses",
                [sql(f"select email from {MARTS}.dim_customer"), "ANSWER"],
            )
            rows = model.prompts[1][1].content if len(model.prompts) == 2 else ""
            check("support gets email addresses", events[-1].text == "ANSWER", events[-1].text)
            check("and they are masked", "***@" in rows and rows.count("@") == rows.count("***@"))

            # 4. SQL that changes data never reaches Trino.
            events, model = await ask(
                "analyst_sales",
                "Remove all orders",
                [f"```sql\ndelete from {MARTS}.fct_orders\n```"] * 2,
            )
            check(
                "a DELETE is refused before Trino",
                "only SELECT queries are allowed" in events[-1].text,
                events[-1].text,
            )

            # 5. A table outside the role's domains.
            events, model = await ask(
                "analyst_sales",
                "Show raw orders",
                [sql("select * from lake.raw.orders")] * 2,
            )
            check("the raw layer is refused", "could not run a query" in events[-1].text)

    print(f"\n{len(failures)} failure(s)" if failures else "\nall agent scenarios passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
