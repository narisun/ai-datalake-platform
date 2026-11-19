# Dev stack

The whole platform on one machine, in containers: a laptop or a single VM.

**Not hardened.** Trino requires sign-in, but OpenMetadata and Airflow still use their
default admin logins over plain HTTP, and no sign-in provider is connected. Do not
expose these ports beyond the host.

## What runs

| Service | Role | Port |
|---|---|---|
| `postgres` | One server, four databases: control, OpenMetadata, Airflow, Polaris | 5432 |
| `polaris` | Iceberg catalog | 8181 |
| `trino` | SQL engine. Requires sign-in over HTTPS | 8443 |
| `opensearch` | OpenMetadata's search, and the agent's search index | 9200 |
| `openmetadata` | The unified layer: tables, lineage, tests, pipelines, glossary | 8585 |
| `airflow` | Scheduler and orchestrator, with the platform installed in its own environment | 8080 |
| `azurite` | Azure storage emulator. Holds the `landing` container that files are dropped into | 10000 |
| `tei-embed`, `tei-rerank` | Model services for search: one turns text into vectors, one re-orders results | internal |

One-shot helpers: `lake-init` (opens up the lake volume), `rules-init` (a starting
access rules file, so Trino can start), `trino-auth-init` (Trino's certificate and
password file), `polaris-bootstrap`, `openmetadata-migrate`.

## Start it

From the repository root:

```
python deploy/compose/dev/init_env.py     # also run this again after pulling changes
docker compose -f deploy/compose/dev/docker-compose.yml up -d --build
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow python /opt/datalake/dev/bootstrap.py
docker compose -f deploy/compose/dev/docker-compose.yml exec airflow python /opt/datalake/dev/run_nightly.py
```

1. `init_env.py` writes `.env` with a generated secret for every password. It never
   overwrites an existing `.env`.
2. `up` builds two small images (Airflow with the platform; Polaris with a named user)
   and starts everything. The first start takes a few minutes.
3. `bootstrap.py` creates the control schema, registers the processes, sets their
   configuration, creates the Polaris catalog, prepares OpenMetadata, and uploads
   sample files to the landing container. It is safe to run again.
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

## Ingesting files

Files dropped into the `landing` container are loaded by `dl ingest`, the first task
of the pipeline. Which files, and where they go, is declared in
[`ingest/sources.yml`](../../../ingest/sources.yml). The sample set is two CSV order
files, an Excel customer list and a Parquet product list.

```
A="docker compose -f deploy/compose/dev/docker-compose.yml exec airflow"
L="/opt/datalake/venv/bin/python /opt/datalake/dev/landing.py"

$A $L ls                         # what is in the landing container
$A $L put /tmp/x.csv shop/orders/orders_2026-11-06.csv
$A dl ingest                     # load new and changed files
$A dl files                      # what happened to each file
$A bash /opt/datalake/dev/scenarios.sh             # ingestion scenarios, end to end
$A bash /opt/datalake/dev/scenarios_transform.sh   # transformation scenarios
```

How it behaves:

- Every value is loaded as text, with `_source_file`, `_row_number`, `_loaded_at` and
  `_run_id` added. The dbt staging models do the typing and de-duplication.
- A file is loaded once. Re-uploading it under the same name with different content
  replaces that file's rows.
- A new column in a file is added to the table. A missing optional column loads as null.
- A file that cannot be loaded is rejected with a reason and recorded. The other files
  still load, but the run ends as failed, so the pipeline does not build on an
  incomplete load. The same bad file is not reported again until it changes.

## Transforming

`dl transform` builds the dbt project in [`dbt/`](../../../dbt) in three layers:

| Layer | What it holds |
|---|---|
| staging | Raw text made typed and de-duplicated. A row that cannot be typed is kept and given a `reject_reason` |
| canonical | One agreed version of each business entity (`customer`, `sales_order`), valid rows only, with enforced column contracts |
| marts | Analytical tables built from the canonical layer |

How it behaves:

- One bad value never fails a model. The row appears in `rejected_rows` with the reason
  and the file it came from, and the build warns with the count.
- A fault that makes the data untrustworthy, such as a duplicated customer id, fails a
  test and stops the build. The canonical and mart tables keep their last good content.
- `dl transform` names every failed model or test in its error, and `dl runs` keeps it.
- A source that has not received data within its freshness window still builds, but
  the run ends as failed so that someone is told.
- A model change that would alter a contracted column's type is refused by dbt.
- `dl check-tags --manifest <manifest.json>` checks that every model has a domain, an
  owner and exactly one classification tag.

A warning-level test shows as *Failed* in OpenMetadata, which has no warning state.

## Access control

Trino enforces who may see what. The rules come from [`dbt/access.yml`](../../../dbt/access.yml)
and the tags on each dbt model. The pipeline's `access_rules` task regenerates Trino's
rules file after every build, and Trino re-reads it within ten seconds, with no restart.

| Trino user | What it may do |
|---|---|
| `role_<name>` (one per role in `access.yml`) | Read the tables in its domains up to its classification ceiling. Columns above the ceiling are hidden; masked columns show the masked value. No writes. |
| `svc_pipeline` | Everything in the lake: this is dbt and ingestion |
| `svc_catalog` | Read everything in the lake, change nothing: this is OpenMetadata |
| anyone else | Nothing |

Raw tables, and any table without tags, are visible to no role. Nor is anything in a
schema listed under `hidden_schemas` in `access.yml`, whatever its tags say. Staging is
listed there: it is a working layer for the pipeline, its columns carry no masks, and
the agent's search index leaves it out. People and the agent read canonical and marts.
To look at rejected rows, sign in as `svc_pipeline` or use OpenMetadata.

```
$A python /opt/datalake/dev/scenarios_access.py   # 42 checks of sign-in and access
```

### Sign-in

Trino requires a user name and password over HTTPS (port 8443).

- **Three service accounts have passwords**, generated into `.env`: `svc_pipeline`,
  `svc_catalog` and `svc_datalake`.
- **Roles have no passwords.** Nobody can sign in as `role_analyst_sales`. The
  platform signs in as `svc_datalake` and then acts as a role; Trino's rules allow
  that for this one account, and only for roles that exist in `access.yml`.
- **`svc_datalake` can do nothing as itself.** It reads no table, and cannot act as
  the pipeline or the catalog reader.

The `trino-auth-init` service makes a self-signed certificate on first start and
rewrites the password file from `.env` on every start. To query by hand:

```
$A python /opt/datalake/dev/trino_dev.py "select count(*) from lake.raw.orders"
$A python /opt/datalake/dev/trino_dev.py --role analyst_sales "select * from lake.analytics_marts.agg_daily_revenue limit 5"
```

What this does not cover: people are not signed in individually. Which role a person
gets will come from Entra ID when the gateway is built; until then a role is chosen by
whoever runs the query. The certificate is self-signed, so clients must be given
`ca.crt` to trust it. And the service passwords live in `.env`, not Key Vault.

## Search and the agent

The pipeline's last task, `index`, copies what OpenMetadata knows (tables, columns,
descriptions, glossary terms) into a search index. Each entry carries its domain and
classification, and a search returns only what the asking role may see. Columns above
a table's own classification are indexed separately, so a role that may see the table
but not those columns never finds them.

```
$A dl index                                              # normally run by the pipeline
$A dl search --role analyst_sales "client revenue"       # what that role would find
$A dl ask --role analyst_sales "What was revenue by day last week?"
```

`dl ask` is a fixed sequence, not a free-roaming agent: search the catalog, have the
model write one SQL query from what was found, run it, have the model word the answer.
If the query fails, the model sees the error and gets one more attempt.

- **The query runs as the role.** The platform signs in to Trino as `svc_datalake` and
  acts as `role_<name>`, so hidden columns, masks and denied tables apply to the agent
  exactly as they do to a person.
- **Only one read-only SELECT is accepted.** Anything else is refused before it reaches
  Trino, which would also refuse it.
- **Results go to the model.** A role that may read above `agent_max_classification`
  (default `confidential`) cannot use the agent at all. Lower it with `dl config set`.
- **Each question is a run** of the `agent` process, visible in `dl runs`.

`dl ask` needs a Claude API key: put `ANTHROPIC_API_KEY=...` in `.env` and run
`docker compose up -d airflow`. The model is `claude-opus-5-5` unless `agent_model`
is set. Calls ask the API to fall back to another model if the first declines a request.

To test everything except the model (real search, real Trino, scripted model replies):

```
$A /opt/datalake/venv/bin/python /opt/datalake/dev/scenarios_agent.py   # 9 checks
```

The model services use small models (`bge-small-en-v1.5`, `bge-reranker-base`) so the
stack fits a laptop. The first start downloads them (about 1.5 GB).

## What is a stand-in

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
