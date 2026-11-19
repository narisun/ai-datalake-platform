"""Spike: the platform's nightly pipeline, running beside OpenMetadata's own DAGs.

The DAG holds only ordering, schedule, retries and lineage declarations. Each task is
one shell command. `inlets` and `outlets` name OpenMetadata tables; an inlet and an
outlet that share a key (for example "orders") become source -> raw lineage through
this pipeline.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

DBT = (
    "/opt/datalake/work/venv/bin/dbt {command} "
    "--project-dir /opt/datalake/dbt --profiles-dir /opt/datalake/dbt "
    "--target-path /opt/datalake/work/target --log-path /opt/datalake/work/logs --no-use-colors"
)
CATALOG = "cd /opt/datalake/work && python om_ingest.py {names}"

with DAG(
    dag_id="nightly",
    description="Ingest sources, build dbt models, refresh the catalog.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["platform"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        # Stand-in for `dl ingest`: the spike's raw tables are loaded by hand.
        bash_command="echo 'ingest: shop.orders, shop.customers -> lake.raw'",
        inlets=[
            OMEntity(entity=Table, fqn="shop_db.shop.public.orders", key="orders"),
            OMEntity(entity=Table, fqn="shop_db.shop.public.customers", key="customers"),
        ],
        outlets=[
            OMEntity(entity=Table, fqn="lake_trino.lake.raw.orders", key="orders"),
            OMEntity(entity=Table, fqn="lake_trino.lake.raw.customers", key="customers"),
        ],
    )
    transform = BashOperator(
        task_id="transform",
        bash_command=DBT.format(command="docs generate") + " && " + DBT.format(command="build"),
        env={"TRINO_HOST": "trino", "TRINO_PORT": "8080"},
        append_env=True,
    )
    catalog = BashOperator(
        task_id="catalog",
        bash_command=CATALOG.format(names="trino dbt"),
    )

    ingest >> transform >> catalog
