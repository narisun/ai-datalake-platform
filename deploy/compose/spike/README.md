# Spike stack

Proves that OpenMetadata, Airflow, Trino and dbt work together as the unified layer.
Results are recorded in [PLATFORM_PROPOSAL.md](../../../docs/architecture/PLATFORM_PROPOSAL.md), section 10.

**Not for production:** default passwords, no TLS, no sign-in provider.

## What is here

| File | Purpose |
|---|---|
| `docker-compose.openmetadata.yml` | OpenMetadata 2.0.8's own quickstart file, copied unchanged |
| `docker-compose.spike.yml` | Adds Trino and mounts the platform's DAGs and dbt project into the Airflow container |
| `trino/catalog/lake.properties` | A local Iceberg catalog named `lake` |
| `dags/nightly.py` | The platform's pipeline: ingest (stand-in), dbt build, catalog refresh |
| `scripts/om_setup.py` | Fetches the bot token; creates the domain, glossary term and team dbt links to |
| `scripts/om_ingest.py` | Runs OpenMetadata's ingestion for the source database, Trino, dbt and Airflow |
| `scripts/om_check.py` | Prints what OpenMetadata knows about a table, the tests, or the pipelines |

## Run it

```
docker compose -p dlspike -f docker-compose.openmetadata.yml -f docker-compose.spike.yml up -d
```

Then, in order: create the sample source and raw tables, install dbt into
`/opt/datalake/work/venv` inside the `openmetadata_ingestion` container, run
`om_setup.py`, run `om_ingest.py shop trino`, trigger the `nightly` DAG, and run
`om_ingest.py airflow`. The exact commands used are in the session that produced the
spike; they will become a single script when the dev profile is built.

- OpenMetadata: http://localhost:8585
- Airflow: http://localhost:8080
- Trino: http://localhost:8090

## Stop it

```
docker compose -p dlspike -f docker-compose.openmetadata.yml -f docker-compose.spike.yml down
```

Add `-v` to delete its data as well.
