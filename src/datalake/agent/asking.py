"""The `agent` process: one question, asked as one role, recorded as one run."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from datalake.agent.errors import NotAnswerableError
from datalake.agent.model import AgentEvent
from datalake.agent.ports import AgentRuntime
from datalake.control.service import Control
from datalake.core.security import Classification, Principal
from datalake.policy.model import AccessRules
from datalake.search.indexing import SearchSettings

PROCESS = "agent"


class AgentSettings(SearchSettings):
    trino_host: str
    trino_port: int = 8443
    trino_scheme: str = "https"
    trino_ca_file: Path | None = None
    # The platform's own Trino account: it can only act as roles from the access policy.
    agent_trino_user: str = "svc_datalake"
    agent_trino_password: str
    anthropic_api_key: str
    agent_model: str = "claude-opus-5-5"
    agent_effort: str = "medium"
    agent_max_rows: int = 200
    # Query results are sent to the model. A role that may read data above this level
    # cannot use the agent at all, so such data can never reach a hosted model.
    agent_max_classification: str = "confidential"


@dataclass(frozen=True, slots=True)
class AgentDependencies:
    rules: AccessRules
    runtime: AgentRuntime


@dataclass(frozen=True, slots=True)
class Answer:
    text: str
    sql: str | None


def ensure_role_may_ask(rules: AccessRules, role: str, limit: Classification) -> Principal:
    """The principal for this role, or NotAnswerableError if it may not use the agent."""
    if role not in rules.roles:
        raise NotAnswerableError(f"no role named {role!r} in the access policy")
    principal = Principal(role, frozenset({role}))
    scopes = rules.search_filter(principal).scopes
    too_high = sorted(domain for domain, ceiling in scopes.items() if ceiling > limit)
    if too_high:
        raise NotAnswerableError(
            f"role {role!r} may read data above {limit.name.lower()} in {', '.join(too_high)}; "
            "the agent does not serve such roles (agent_max_classification)"
        )
    return principal


async def run_ask(
    control: Control,
    build: Callable[[AgentSettings], AgentDependencies],
    question: str,
    role: str,
    on_event: Callable[[AgentEvent], None] = lambda _event: None,
) -> Answer:
    with control.start(PROCESS) as run:
        settings = run.config(AgentSettings)
        dependencies = build(settings)
        limit = Classification.parse(settings.agent_max_classification)
        principal = ensure_role_may_ask(dependencies.rules, role, limit)

        text, sql = "", None
        with run.step("ask"):
            async for event in dependencies.runtime.ask(question, principal, run.run_id):
                on_event(event)
                if event.kind == "sql":
                    sql = event.text
                elif event.kind == "final":
                    text = event.text
        if not text:
            raise NotAnswerableError("the agent finished without an answer")
        return Answer(text, sql)
