# ai-datalake-platform

A metadata-driven data lake platform: Iceberg on ADLS Gen2,
dbt on Trino, OpenMetadata, hybrid search, a LangGraph agent with a Chainlit UI, and an
MCP server.

The design is in [docs/architecture/PLATFORM_PROPOSAL.md](docs/architecture/PLATFORM_PROPOSAL.md).

## Status

Built so far: the control module (registered processes, configuration, run records),
the access policy and Trino rule generator, the dbt runner and manifest reader, the
storage adapter, file ingestion, the search index built from OpenMetadata, hybrid search,
and a question-answering agent (`dl ask`) that writes read-only SQL and runs it as the
asker's role. The whole platform runs in containers from `deploy/compose/dev`.
Not built yet: the MCP server and the Chainlit UI.

## Control

Every process runs inside `control.start(name)`. A process that is not registered, or
is disabled, gets an exception and no configuration.

```
dl db upgrade                           # create or migrate the control schema
dl process register ingest
dl config set trino_host=trino          # shared by all processes
dl config set batch_size=5000 --process ingest
dl config import .env                   # move DL_* values from a .env file
dl config doctor                        # list values still only in .env
dl runs
dl ingest                               # load new and changed landing files
dl files                                # what ingestion did with each file
dl transform                            # build the dbt project
dl check-tags --manifest target/manifest.json   # every model tagged correctly?
```

Only `DL_DATABASE_URL` must come from the environment or `.env`. While values are
being moved, a key missing from the database falls back to `DL_<KEY>` with a warning;
`DL_STRICT_CONFIG=1` turns that off.

## Layout

```
src/datalake/
  core/        shared types: errors, security, catalog entities (standard library only)
  control/     registered processes, configuration, run records
  policy/      the access policy, its file format, Trino rule generation
  storage/     object storage (ADLS Gen2, Azurite) and the table catalog
  ingest/      loading landing files (CSV, Excel, Parquet) into the raw layer
  transform/   running dbt and reading its artifacts
  catalog/     the metadata catalog (OpenMetadata)
  search/      hybrid search: BM25, vectors, fusion, rerank
  agent/       chat model, read-only SQL, the agent runtime
  cli.py       the `dl` command
```

Inside a module: `model.py` holds its types, `ports.py` the interfaces it needs,
`testing.py` an in-memory fake for each interface, and other files the real
implementations. Every class receives its collaborators through `__init__`, typed
against a port. `import-linter` enforces that modules depend only on the ones listed
above them here, and that models, ports and pure logic import no vendor libraries.

## Develop

```
uv sync
uv run pytest
uv run ruff check .
uv run pyright
uv run lint-imports
```

## Access policy

Access rules live in one file (see [dbt/access.yml](dbt/access.yml)). Validate it with:

```
uv run dl policy validate dbt/access.yml
```
