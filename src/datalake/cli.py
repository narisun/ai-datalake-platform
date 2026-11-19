"""The `dl` command line. Thin: it parses arguments and prints results."""

from __future__ import annotations

import argparse
import getpass
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from dotenv import dotenv_values

from datalake import __version__
from datalake.control.bootstrap import DATABASE_URL, database_url, load_environment, open_store
from datalake.control.errors import ConfigError, ProcessNotRegisteredError
from datalake.control.ports import ControlStore
from datalake.control.postgres import apply_migrations
from datalake.core.errors import DatalakeError
from datalake.policy.policy_file import load_access_rules
from datalake.policy.trino_rules import build_trino_rules
from datalake.transform.dbt_manifest import read_manifest

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
    )
    text = json.dumps(document, indent=2) + "\n"
    if args.out is None:
        sys.stdout.write(text)
    else:
        args.out.write_text(text, encoding="utf-8")
        print(f"wrote {args.out}: {len(document['tables']) - 2} table grant(s)")
    return 0


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

        env = environment if environment is not None else load_environment()
        if args.command == "db":
            applied = apply_migrations(database_url(env))
            print(f"applied {len(applied)} migration(s)" if applied else "already up to date")
            return 0

        control_store = store if store is not None else open_store(env)
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
