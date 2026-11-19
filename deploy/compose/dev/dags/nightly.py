"""The platform's nightly pipeline.

The DAG holds only ordering, schedule, retries and lineage declarations. Every task is
one `dl` command, so each runs as a registered process: it gets its configuration from
the control database and leaves a run record there, with this DAG run's id on it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

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
    ingest = BashOperator(
        task_id="ingest",
        bash_command="dl exec ingest -- python /opt/datalake/dev/ingest_standin.py",
        # An inlet and an outlet that share a key become lineage through this pipeline.
        inlets=[
            OMEntity(entity=Table, fqn="shop_db.shop.public.orders", key="orders"),
            OMEntity(entity=Table, fqn="shop_db.shop.public.customers", key="customers"),
        ],
        outlets=[
            OMEntity(entity=Table, fqn="lake_trino.lake.raw.orders", key="orders"),
            OMEntity(entity=Table, fqn="lake_trino.lake.raw.customers", key="customers"),
        ],
    )
    transform = BashOperator(task_id="transform", bash_command="dl transform")
    access_rules = BashOperator(
        task_id="access_rules",
        bash_command=(
            "dl exec access-rules -- dl policy trino-rules "
            "--policy /opt/datalake/dbt/access.yml "
            f"--manifest {WORK}/target/manifest.json --out {WORK}/trino-rules.json"
        ),
    )
    catalog_trino = BashOperator(task_id="catalog_trino", bash_command=catalog("trino"))
    catalog_dbt = BashOperator(task_id="catalog_dbt", bash_command=catalog("dbt"))
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
    catalog_trino >> catalog_dbt >> catalog_airflow
