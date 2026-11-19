"""The platform's nightly pipeline.

The DAG holds only ordering, schedule and retries. Every task is
one `dl` command, so each runs as a registered process: it gets its configuration from
the control database and leaves a run record there, with this DAG run's id on it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

WORK = "/opt/datalake/work"


def catalog(source: str) -> str:
    """Run one of OpenMetadata's ingestion workflows as the catalog-sync process."""
    return f"dl exec catalog-sync -- metadata ingest -c {WORK}/ingest_{source}.yaml"


with DAG(
    dag_id="nightly",
    description="Ingest sources, build dbt models, refresh access rules and the catalog.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["platform"],
) as dag:
    ingest = BashOperator(task_id="ingest", bash_command="dl ingest")
    transform = BashOperator(task_id="transform", bash_command="dl transform")
    access_rules = BashOperator(
        task_id="access_rules",
        bash_command=(
            "dl exec access-rules -- dl policy trino-rules "
            "--policy /opt/datalake/dbt/access.yml "
            f"--manifest {WORK}/target/manifest.json --catalog-user svc_catalog "
            f"--out {WORK}/trino-rules.json"
        ),
    )
    catalog_trino = BashOperator(task_id="catalog_trino", bash_command=catalog("trino"))
    catalog_dbt = BashOperator(task_id="catalog_dbt", bash_command=catalog("dbt"))
    # The agent searches what the catalog holds, so the index follows the catalog.
    index = BashOperator(task_id="index", bash_command="dl index")
    # Last, so that OpenMetadata sees the status of the tasks above.
    # Airflow 3 hides its database from tasks by blanking this variable. OpenMetadata's
    # Airflow ingestion reads pipelines and run history from that database, so this one
    # read-only task is given the connection back.
    catalog_airflow = BashOperator(
        task_id="catalog_airflow",
        bash_command=(
            "export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="
            '"postgresql+psycopg2://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$AIRFLOW_DB" && '
            + catalog("airflow")
        ),
    )

    ingest >> transform >> [access_rules, catalog_trino]
    catalog_trino >> catalog_dbt >> [catalog_airflow, index]
