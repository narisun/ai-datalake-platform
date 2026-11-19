# Dev stack

The whole platform on one machine, in containers: a laptop or a single VM.

**Not hardened.** There is no TLS and no sign-in provider, and OpenMetadata and Airflow
use their default admin logins. Do not expose these ports beyond the host.

## What runs

| Service | Role | Port |
|---|---|---|
| `postgres` | One server, five databases: control, OpenMetadata, Airflow, Polaris, and a sample source (`shop`) | 5432 |
| `polaris` | Iceberg catalog | 8181 |
| `trino` | SQL engine | 8090 |
| `opensearch` | OpenMetadata's search | 9200 |
| `openmetadata` | The unified layer: tables, lineage, tests, pipelines, glossary | 8585 |
| `airflow` | Scheduler and orchestrator, with the platform installed in its own environment | 8080 |
| `azurite` | Azure storage emulator for the platform's own objects | 10000 |

One-shot helpers: `lake-init` (opens up the lake volume), `polaris-bootstrap`,
`openmetadata-migrate`.

## Start it

From the repository root:

```
python deploy/compose/dev/init_env.py
docker compose -f deploy/compose/dev/docker-compose.yml up -d --build
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow python /opt/datalake/dev/bootstrap.py
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow python /opt/datalake/dev/run_nightly.py
```

1. `init_env.py` writes `.env` with a generated secret for every password. It never
   overwrites an existing `.env`.
2. `up` builds two small images (Airflow with the platform; Polaris with a named user)
   and starts everything. The first start takes a few minutes.
3. `bootstrap.py` creates the control schema, registers the processes, sets their
   configuration, creates the Polaris catalog, and prepares OpenMetadata. It is safe
   to run again.
4. `run_nightly.py` triggers the pipeline and prints each task's result.

## The pipeline

`dags/nightly.py`, scheduled for 02:00:

```
ingest → transform → access_rules
                   → catalog_trino → catalog_dbt → catalog_airflow
```

Every task is a `dl` command, so each is a registered process with a run record:

```
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow dl runs
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow dl config list
```

To see what OpenMetadata holds for a table, the tests, or the pipelines:

```
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow \
    python /opt/datalake/dev/check.py lake_trino.lake.analytics_marts.fct_orders tests pipelines
```

## What is a stand-in

- **Ingestion.** `scripts/ingest_standin.py` copies the sample tables in full through
  Trino. It has no incremental loads, watermarks or schema evolution. The real ingest
  module (dlt) replaces it.
- **Access rules.** The pipeline generates Trino's rules file but Trino does not load
  it yet, so no access control is enforced.
- **Secrets.** They come from `.env`. Key Vault references are not wired in.

## Things that will surprise you

- **The lake is a Docker volume**, not object storage. Trino creates each table's
  folder and Polaris writes metadata into it, so both run as the same user id (1000).
  That is the only reason `polaris/Dockerfile` exists. It goes away with ADLS Gen2.
- **Polaris has a fixed token-signing secret** (`POLARIS_TOKEN_SECRET`). Without it,
  every Polaris restart invalidates the token Trino holds and queries fail until
  Trino restarts too.
- **Polaris is told to allow local files and table purges.** Both are flagged by
  Polaris as unsafe for production and are for this phase only.
- **The last pipeline task shows as Pending in OpenMetadata.** It is the task that
  reports to OpenMetadata, so it cannot report its own success; the next run corrects it.
- **Airflow 3 hides its database from tasks.** The `catalog_airflow` task is given the
  connection back, because OpenMetadata's Airflow ingestion reads from that database.
- **`polaris-bootstrap` logs the Polaris client secret**, because the tool takes it on
  the command line. Treat those container logs as sensitive.

## Stop it

```
docker compose -f deploy/compose/dev/docker-compose.yml down        # keep data
docker compose -f deploy/compose/dev/docker-compose.yml down -v     # delete data
```

After `down -v`, also delete `.env` if you want new secrets; otherwise keep it.

## Running the contract tests against this stack

```
DL_TEST_AZURITE_CONTAINER=contract-tests
DL_TEST_DATABASE_URL=postgresql://postgres:<POSTGRES_PASSWORD from .env>@localhost:5432/dl_control_test
```

Create the `dl_control_test` database and the `contract-tests` container first.
