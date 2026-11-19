"""The `dl` command line. Thin: it parses arguments and prints results."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from dotenv import dotenv_values

from datalake import __version__
from datalake.control.bootstrap import (
    DATABASE_URL,
    build_control,
    database_url,
    load_environment,
    open_store,
)
from datalake.control.command import run_external
from datalake.control.errors import ConfigError, ProcessNotRegisteredError
from datalake.control.ports import ControlStore
from datalake.control.postgres import apply_migrations
from datalake.control.runtime import SystemClock
from datalake.control.service import Control
from datalake.core.errors import DatalakeError
from datalake.ingest.service import IngestDependencies, IngestSettings, run_ingest
from datalake.policy.policy_file import load_access_rules
from datalake.policy.trino_rules import build_trino_rules
from datalake.search.indexing import IndexDependencies, SearchSettings, run_index
from datalake.transform.command import DocsEngine, TransformSettings, run_transform
from datalake.transform.dbt_manifest import read_manifest
from datalake.transform.tag_check import check_manifest_file

_ENV_PREFIX = "DL_"
# Bootstrap settings stay in the environment; they are never moved to the database.
_BOOTSTRAP_KEYS = {DATABASE_URL, "DL_STRICT_CONFIG", "DL_GIT_COMMIT"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dl", description="ai-datalake-platform command line")
    parser.add_argument("--version", action="version", version=f"dl {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    # dl db
    db = commands.add_parser("db", help="manage the control database").add_subparsers(
        dest="action", required=True
    )
    db.add_parser("upgrade", help="create or migrate the control schema")

    # dl process
    process = commands.add_parser("process", help="manage registered processes").add_subparsers(
        dest="action", required=True
    )
    register = process.add_parser("register", help="register a process so it may run")
    register.add_argument("name")
    register.add_argument("--description", default="")
    for action, text in (("enable", "allow a process to run"), ("disable", "block a process")):
        process.add_parser(action, help=text).add_argument("name")
    process.add_parser("list", help="show registered processes")

    # dl config
    config = commands.add_parser("config", help="manage configuration").add_subparsers(
        dest="action", required=True
    )
    set_ = config.add_parser("set", help="set a value: dl config set key=value")
    set_.add_argument("assignment", metavar="key=value")
    set_.add_argument("--process", help="set for one process only; default is shared")
    unset = config.add_parser("unset", help="remove a value")
    unset.add_argument("key")
    unset.add_argument("--process")
    config.add_parser("list", help="show all values").add_argument("--process")
    config.add_parser("export", help="print all values as text, to compare environments")
    import_ = config.add_parser("import", help="copy DL_* values from a .env file as shared values")
    import_.add_argument("path", type=Path)
    doctor = config.add_parser("doctor", help="list keys in a .env file not yet in the database")
    doctor.add_argument("path", type=Path, nargs="?", default=Path(".env"))

    # dl runs
    runs = commands.add_parser("runs", help="show recent runs")
    runs.add_argument("--process")
    runs.add_argument("--limit", type=int, default=20)

    # dl ingest / dl files
    ingest = commands.add_parser("ingest", help="load new and changed landing files")
    ingest.add_argument("--source", help="load one source only")
    files = commands.add_parser("files", help="show what ingestion did with each file")
    files.add_argument("--source")

    # dl index / dl search
    commands.add_parser("index", help="bring the agent's search index in step with the catalog")
    search = commands.add_parser("search", help="search the catalog as a role would see it")
    search.add_argument("--role", required=True, help="a role from access.yml")
    search.add_argument("--limit", type=int, default=5)
    search.add_argument("text")
    ask = commands.add_parser("ask", help="ask a question about the data, as a role")
    ask.add_argument("--role", required=True, help="a role from access.yml")
    ask.add_argument("question")

    # dl transform / dl exec
    commands.add_parser("transform", help="build the dbt project as the 'transform' process")
    exec_ = commands.add_parser(
        "exec", help="run a command as a registered process: dl exec NAME -- COMMAND"
    )
    exec_.add_argument("process")
    exec_.add_argument("cmd", nargs=argparse.REMAINDER, metavar="-- COMMAND")

    # dl check-tags
    check = commands.add_parser(
        "check-tags", help="check every dbt model has a domain, owner and classification"
    )
    check.add_argument("--manifest", type=Path, required=True, help="dbt manifest.json")

    # dl policy
    policy = commands.add_parser("policy", help="work with the access policy").add_subparsers(
        dest="action", required=True
    )
    policy.add_parser("validate", help="check an access.yml file").add_argument("path", type=Path)
    trino = policy.add_parser("trino-rules", help="generate Trino access rules from the policy")
    trino.add_argument("--policy", type=Path, required=True, help="access.yml")
    trino.add_argument("--manifest", type=Path, required=True, help="dbt manifest.json")
    trino.add_argument("--catalog", default="lake", help="Trino catalog holding the lake")
    trino.add_argument("--service-user", default="svc_datalake", help="the platform's Trino user")
    trino.add_argument("--pipeline-user", default="svc_pipeline", help="dbt and ingestion user")
    trino.add_argument("--catalog-user", help="the metadata catalog's read-only Trino user")
    trino.add_argument("--out", type=Path, help="write here instead of standard output")
    return parser


# --- policy ---------------------------------------------------------------------------


def _policy(args: argparse.Namespace) -> int:
    if args.action == "validate":
        rules = load_access_rules(args.path)
        print(f"{args.path}: valid, {len(rules.roles)} role(s)")
        for name in sorted(rules.roles):
            rule = rules.roles[name]
            domains = ", ".join(sorted(rule.domains))
            print(
                f"  {name}: domains [{domains}], up to {rule.max_classification.name.lower()}, "
                f"{len(rule.masks)} mask(s)"
            )
        return 0

    document = build_trino_rules(
        rules=load_access_rules(args.policy),
        datasets=read_manifest(args.manifest).datasets,
        catalog=args.catalog,
        service_user=args.service_user,
        pipeline_user=args.pipeline_user,
        catalog_user=args.catalog_user,
    )
    text = json.dumps(document, indent=2) + "\n"
    if args.out is None:
        sys.stdout.write(text)
    else:
        # Trino re-reads this file while it runs, so it must never see half of it:
        # write beside it, then swap in one step.
        staging = args.out.with_name(args.out.name + ".tmp")
        staging.write_text(text, encoding="utf-8")
        staging.replace(args.out)
        grants = sum(1 for rule in document["tables"] if "table" in rule)
        print(f"wrote {args.out}: {grants} table grant(s)")
    return 0


def _check_tags(manifest: Path) -> int:
    problems = check_manifest_file(manifest)
    models = sum(
        1
        for node in json.loads(manifest.read_text(encoding="utf-8")).get("nodes", {}).values()
        if node.get("resource_type") == "model"
    )
    if not problems:
        print(f"{manifest}: {models} model(s) checked, all tagged correctly")
        return 0
    for problem in problems:
        print(problem)
    print(f"{len(problems)} problem(s) in {models} model(s)", file=sys.stderr)
    return 1


# --- control --------------------------------------------------------------------------


def _process_id(store: ControlStore, name: str | None) -> int | None:
    if name is None:
        return None
    process = store.find_process(name)
    if process is None:
        raise ProcessNotRegisteredError(name)
    return process.process_id


def _process(args: argparse.Namespace, store: ControlStore) -> int:
    if args.action == "register":
        process = store.register_process(args.name, args.description)
        state = "enabled" if process.is_enabled else "disabled"
        print(f"registered {process.name} ({state})")
    elif args.action in ("enable", "disable"):
        store.set_process_enabled(args.name, args.action == "enable")
        print(f"{args.name}: {args.action}d")
    else:
        for process in store.list_processes():
            state = "enabled " if process.is_enabled else "DISABLED"
            print(f"{state}  {process.name}  {process.description}".rstrip())
    return 0


def _dotenv_keys(path: Path) -> dict[str, str]:
    """`DL_*` settings in a .env file, as database keys, without the bootstrap ones."""
    if not path.is_file():
        raise ConfigError(f"no such file: {path}")
    return {
        name[len(_ENV_PREFIX) :].lower(): value
        for name, value in dotenv_values(path).items()
        if value is not None and name.startswith(_ENV_PREFIX) and name not in _BOOTSTRAP_KEYS
    }


def _config(args: argparse.Namespace, store: ControlStore, actor: str) -> int:
    if args.action == "set":
        key, separator, value = args.assignment.partition("=")
        if not separator or not key:
            raise ConfigError("expected key=value")
        store.set_config(key, value, _process_id(store, args.process), actor)
        print(f"set {key}" + (f" for {args.process}" if args.process else " (shared)"))
    elif args.action == "unset":
        removed = store.unset_config(args.key, _process_id(store, args.process), actor)
        print(f"removed {args.key}" if removed else f"{args.key} was not set")
        return 0 if removed else 1
    elif args.action in ("list", "export"):
        wanted = getattr(args, "process", None)
        for entry in store.list_config():
            if wanted is not None and entry.process not in (None, wanted):
                continue
            print(f"{entry.process or '*'}\t{entry.key}={entry.value}")
    elif args.action == "import":
        values = _dotenv_keys(args.path)
        for key, value in sorted(values.items()):
            store.set_config(key, value, None, actor)
        print(f"imported {len(values)} value(s) from {args.path} as shared configuration")
    else:  # doctor
        in_database = {entry.key for entry in store.list_config()}
        remaining = sorted(set(_dotenv_keys(args.path)) - in_database)
        if not remaining:
            print(f"{args.path}: every DL_* setting is in the database")
            return 0
        print(f"{args.path}: {len(remaining)} setting(s) still only in the file:")
        for key in remaining:
            print(f"  {_ENV_PREFIX}{key.upper()}")
        return 1
    return 0


def _runs(args: argparse.Namespace, store: ControlStore) -> int:
    for run in store.list_runs(args.process, args.limit):
        started = run.started_at.isoformat(timespec="seconds")
        line = f"{started}  {run.status:<9}  {run.process:<14}  {run.run_id}"
        print(f"{line}  {run.error}" if run.error else line)
    return 0


def _ingest_dependencies(settings: IngestSettings) -> IngestDependencies:
    # Imported here so that `dl` works without these libraries for every other command.
    from datalake.ingest.sources_file import load_sources
    from datalake.ingest.trino_writer import TrinoRawTableWriter
    from datalake.storage.obstore_store import ObstoreObjectStore, create_landing_store

    return IngestDependencies(
        sources=load_sources(settings.sources_file),
        landing=ObstoreObjectStore(
            create_landing_store(
                account=settings.landing_account,
                container=settings.landing_container,
                endpoint=settings.landing_endpoint,
                account_key=settings.landing_account_key,
                managed_identity_client_id=settings.landing_client_id,
            )
        ),
        writer=TrinoRawTableWriter(
            host=settings.trino_host,
            port=settings.trino_port,
            user=settings.trino_user,
            catalog=settings.trino_catalog,
            scheme=settings.trino_scheme,
            password=settings.trino_password,
            ca_file=str(settings.trino_ca_file) if settings.trino_ca_file else None,
        ),
    )


def _files(args: argparse.Namespace, store: ControlStore) -> int:
    process = store.find_process("ingest")
    if process is None:
        raise ProcessNotRegisteredError("ingest")
    for file in store.list_ingested_files(process.process_id, args.source):
        rows = "" if file.rows_loaded is None else f"{file.rows_loaded} rows"
        line = f"{file.status:<10}  {file.source:<14}  {file.path}  {rows}".rstrip()
        print(f"{line}  {file.error}" if file.error else line)
    return 0


async def _index(control: Control) -> int:
    # Imported here so that `dl` works without these libraries for every other command.
    import httpx

    from datalake.catalog.openmetadata import OpenMetadataCatalog
    from datalake.search.opensearch import OpenSearchChunkIndex
    from datalake.search.tei import TeiEmbedder

    async with httpx.AsyncClient(timeout=120) as http:

        def build(settings: SearchSettings) -> IndexDependencies:
            return IndexDependencies(
                catalog=OpenMetadataCatalog(
                    http,
                    settings.openmetadata_url,
                    settings.openmetadata_token,
                    settings.catalog_service,
                ),
                store=OpenSearchChunkIndex(
                    http,
                    settings.opensearch_url,
                    settings.search_index,
                    TeiEmbedder(http, settings.embed_url),
                ),
                hidden_schemas=load_access_rules(settings.access_policy_file).hidden_schemas,
            )

        summary = await run_index(control, build)
    print(
        f"index succeeded: {summary.total} chunk(s); {summary.written} written, "
        f"{summary.deleted} removed, {summary.unchanged} unchanged"
    )
    return 0


async def _search(control: Control, role: str, text: str, limit: int) -> int:
    import httpx

    from datalake.core.security import Principal
    from datalake.policy.service import RoleAccessPolicy
    from datalake.search.opensearch import OpenSearchChunkIndex, VectorSearch
    from datalake.search.retriever import HybridRetriever
    from datalake.search.tei import TeiEmbedder, TeiReranker

    # A search is recorded as a run of the `agent` process: who asked, as which role.
    with control.start("agent") as run:
        settings = run.config(SearchSettings)
        rules = load_access_rules(settings.access_policy_file)
        if role not in rules.roles:
            raise ConfigError(f"no role named {role!r} in {settings.access_policy_file}")
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
                final_n=limit,
            )
            with run.step("search") as step:
                passages = await retriever.retrieve(text, Principal(role, frozenset({role})))
                step.rows_read = len(passages)
    if not passages:
        print("nothing found that this role may see")
    for rank, passage in enumerate(passages, start=1):
        first_line = passage.chunk.text.splitlines()[0]
        print(f"{rank}. {passage.score:6.3f}  {first_line}")
    return 0


async def _ask(control: Control, role: str, question: str) -> int:
    import anthropic
    import httpx

    from datalake.agent.asking import AgentDependencies, AgentSettings, run_ask
    from datalake.agent.claude import AnthropicChatModel
    from datalake.agent.graph import LangGraphAgent
    from datalake.agent.model import AgentEvent
    from datalake.agent.trino_query import TrinoQueryEngine
    from datalake.policy.service import RoleAccessPolicy
    from datalake.search.opensearch import OpenSearchChunkIndex, VectorSearch
    from datalake.search.retriever import HybridRetriever
    from datalake.search.tei import TeiEmbedder, TeiReranker

    def show(event: AgentEvent) -> None:
        if event.kind == "status":
            print(f"... {event.text}", file=sys.stderr)
        elif event.kind == "sql":
            print(f"\n{event.text}\n", file=sys.stderr)

    async with httpx.AsyncClient(timeout=120) as http:

        def build(settings: AgentSettings) -> AgentDependencies:
            if not settings.anthropic_api_key.strip():
                raise ConfigError("anthropic_api_key is empty; set ANTHROPIC_API_KEY for the stack")
            rules = load_access_rules(settings.access_policy_file)
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
            model = AnthropicChatModel(
                anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key),
                model=settings.agent_model,
                effort=settings.agent_effort,
            )
            engine = TrinoQueryEngine(
                host=settings.trino_host,
                port=settings.trino_port,
                account=settings.agent_trino_user,
                password=settings.agent_trino_password,
                scheme=settings.trino_scheme,
                ca_file=str(settings.trino_ca_file) if settings.trino_ca_file else None,
            )
            runtime = LangGraphAgent(retriever, model, engine, max_rows=settings.agent_max_rows)
            return AgentDependencies(rules=rules, runtime=runtime)

        answer = await run_ask(control, build, question, role, on_event=show)
    print(answer.text)
    return 0


def _dbt_engine(settings: TransformSettings, run_id: str) -> DocsEngine:
    # Imported here so that `dl` works without dbt installed for every other command.
    from datalake.transform.dbt_core_runner import DbtCoreRunner

    return DbtCoreRunner(
        project_dir=settings.dbt_project_dir,
        profiles_dir=settings.dbt_profiles_dir or settings.dbt_project_dir,
        target_dir=settings.dbt_target_dir,
        log_dir=settings.dbt_log_dir,
        environment=settings.dbt_environment(),
        variables={"run_id": run_id},
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    store: ControlStore | None = None,
    environment: Mapping[str, str] | None = None,
) -> int:
    """`store` and `environment` are for tests; by default both come from the real world."""
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "policy":
            return _policy(args)
        if args.command == "check-tags":
            return _check_tags(args.manifest)

        env = environment if environment is not None else load_environment()
        if args.command == "db":
            applied = apply_migrations(database_url(env))
            print(f"applied {len(applied)} migration(s)" if applied else "already up to date")
            return 0

        control_store = store if store is not None else open_store(env)
        if args.command == "ingest":
            summary = asyncio.run(
                run_ingest(
                    build_control(env, control_store),
                    _ingest_dependencies,
                    SystemClock(),
                    only=args.source,
                )
            )
            print(
                f"ingest succeeded: {summary.count('loaded')} file(s) loaded, "
                f"{summary.rows_loaded} row(s); {summary.count('unchanged')} unchanged, "
                f"{summary.count('superseded')} superseded"
            )
            return 0
        if args.command == "index":
            return asyncio.run(_index(build_control(env, control_store)))
        if args.command == "search":
            return asyncio.run(
                _search(build_control(env, control_store), args.role, args.text, args.limit)
            )
        if args.command == "ask":
            return asyncio.run(_ask(build_control(env, control_store), args.role, args.question))
        if args.command == "files":
            return _files(args, control_store)
        if args.command == "transform":
            result = asyncio.run(run_transform(build_control(env, control_store), _dbt_engine))
            print(f"transform succeeded: {len(result.models_built)} model(s) built")
            for warning in result.warnings:
                print(f"  warning: {warning}")
            return 0
        if args.command == "exec":
            command = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
            run_external(build_control(env, control_store), args.process, command, env)
            return 0
        if args.command == "process":
            return _process(args, control_store)
        if args.command == "config":
            return _config(args, control_store, actor=env.get("DL_ACTOR") or getpass.getuser())
        return _runs(args, control_store)
    except (DatalakeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
