"""The `ingest` process: load new and changed landing files into raw lake tables."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

from pydantic import BaseModel

from datalake.control.ports import Clock
from datalake.control.service import Control, RunContext
from datalake.core.errors import DatalakeError
from datalake.ingest.model import FileOutcome, IngestSummary, SourceDefinition
from datalake.ingest.parsers import FileRejectedError, parse_file
from datalake.ingest.ports import RawTableWriter
from datalake.storage.ports import ObjectStore

PROCESS = "ingest"


class IngestSettings(BaseModel):
    sources_file: Path
    landing_container: str
    landing_account: str = "devstoreaccount1"
    landing_endpoint: str | None = None  # set for Azurite; unset for real ADLS Gen2
    landing_account_key: str | None = None  # Azurite only; ADLS uses a managed identity
    landing_client_id: str | None = None  # the managed identity, on Azure
    trino_host: str
    trino_port: int = 8080
    trino_scheme: str = "http"
    trino_user: str = "svc_pipeline"
    trino_password: str | None = None  # set where Trino requires sign-in
    trino_ca_file: Path | None = None  # the certificate to trust, if not a public one
    trino_catalog: str = "lake"


@dataclass(frozen=True, slots=True)
class IngestDependencies:
    sources: Sequence[SourceDefinition]
    landing: ObjectStore
    writer: RawTableWriter


class IngestRejectedError(DatalakeError):
    """At least one file was rejected. Everything loadable was still loaded."""


class UnknownSourceError(DatalakeError):
    pass


async def run_ingest(
    control: Control,
    build: Callable[[IngestSettings], IngestDependencies],
    clock: Clock,
    only: str | None = None,
) -> IngestSummary:
    """Load every source's new or changed files. Raises after loading if any was rejected.

    One bad file never stops the others. But the run ends as failed, so the pipeline
    does not build on a load that is known to be incomplete.
    """
    outcomes: list[FileOutcome] = []
    with control.start(PROCESS) as run:
        dependencies = build(run.config(IngestSettings))
        sources = [s for s in dependencies.sources if only is None or s.name == only]
        if only is not None and not sources:
            raise UnknownSourceError(f"no source named {only!r} in the sources file")

        for source in sources:
            with run.step(source.name) as step:
                found = await _load_source(run, source, dependencies, clock)
                step.rows_read = sum(1 for o in found if o.outcome != "unchanged")
                step.rows_written = sum(o.rows for o in found if o.outcome == "loaded")
                outcomes.extend(found)

        rejected = [o for o in outcomes if o.outcome == "rejected"]
        if rejected:
            details = "; ".join(f"{o.path}: {o.error}" for o in rejected)
            raise IngestRejectedError(f"{len(rejected)} file(s) rejected: {details}")
    return IngestSummary(tuple(outcomes))


async def _load_source(
    run: RunContext, source: SourceDefinition, deps: IngestDependencies, clock: Clock
) -> list[FileOutcome]:
    paths = sorted(
        path
        for path in await deps.landing.list(source.landing_prefix)
        if fnmatch(path.rsplit("/", 1)[-1], source.pattern)
    )

    outcomes: list[FileOutcome] = []
    pending: list[tuple[str, bytes, str]] = []
    for path in paths:
        data = await deps.landing.get(path)
        content_hash = hashlib.sha256(data).hexdigest()
        earlier = run.ingested_file(source.name, path)
        if earlier is not None and earlier.content_hash == content_hash:
            # Same bytes as last time: already loaded, or already known to be bad.
            outcomes.append(FileOutcome(source.name, path, "unchanged"))
        else:
            pending.append((path, data, content_hash))

    if source.mode == "replace" and pending:
        # Each file is a full snapshot, so only the newest file in the folder (by name)
        # may become the table. A changed older file must never replace a newer one,
        # whether or not the newer one is being loaded in this run.
        newest = paths[-1]
        for path, _, content_hash in pending:
            if path != newest:
                run.record_file(source.name, path, content_hash, "superseded")
                outcomes.append(FileOutcome(source.name, path, "superseded"))
        pending = [item for item in pending if item[0] == newest]

    for path, data, content_hash in pending:
        try:
            parsed = parse_file(source, data)
        except FileRejectedError as exc:
            run.record_file(source.name, path, content_hash, "rejected", error=str(exc))
            outcomes.append(FileOutcome(source.name, path, "rejected", error=str(exc)))
            continue

        await deps.writer.ensure_table(source.schema, source.table_name, parsed.columns)
        if source.mode == "replace":
            await deps.writer.truncate(source.schema, source.table_name)
        else:
            # Makes a retry, or a corrected re-upload of the same file, safe.
            await deps.writer.delete_file(source.schema, source.table_name, path)
        await deps.writer.insert(
            source.schema,
            source.table_name,
            parsed.columns,
            parsed.rows,
            source_file=path,
            run_id=run.run_id,
            loaded_at=clock.now(),
        )
        # Recorded only after the rows are in, so a crash before this line is retried.
        run.record_file(source.name, path, content_hash, "loaded", rows_loaded=len(parsed.rows))
        outcomes.append(FileOutcome(source.name, path, "loaded", rows=len(parsed.rows)))
    return outcomes
