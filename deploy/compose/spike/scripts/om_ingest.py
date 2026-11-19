"""Spike: run OpenMetadata's ingestion workflows from the command line.

Run inside the ingestion container:  python om_ingest.py shop trino dbt airflow
Each name writes a workflow file and runs `metadata ingest -c <file>`.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import yaml

WORK = pathlib.Path("/opt/datalake/work")
TARGET = WORK / "target"


def _workflow(source: dict[str, object]) -> dict[str, object]:
    return {
        "source": source,
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {
                "hostPort": "http://openmetadata-server:8585/api",
                "authProvider": "openmetadata",
                "securityConfig": {"jwtToken": (WORK / "om_token").read_text().strip()},
            },
        },
    }


WORKFLOWS: dict[str, dict[str, object]] = {
    # The source system, so lineage can start outside the lake.
    "shop": {
        "type": "postgres",
        "serviceName": "shop_db",
        "serviceConnection": {
            "config": {
                "type": "Postgres",
                "username": "postgres",
                "authType": {"password": "password"},
                "hostPort": "postgresql:5432",
                "database": "shop",
            }
        },
        "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
    },
    # Every table in the lake, in all layers.
    "trino": {
        "type": "trino",
        "serviceName": "lake_trino",
        "serviceConnection": {
            "config": {
                "type": "Trino",
                "hostPort": "trino:8080",
                "username": "svc_catalog",
                "catalog": "lake",
            }
        },
        "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
    },
    # dbt artifacts: descriptions, tags, owners, domain, glossary, lineage, tests.
    "dbt": {
        "type": "dbt",
        "serviceName": "lake_trino",
        "sourceConfig": {
            "config": {
                "type": "DBT",
                "dbtConfigSource": {
                    "dbtConfigType": "local",
                    "dbtCatalogFilePath": str(TARGET / "catalog.json"),
                    "dbtManifestFilePath": str(TARGET / "manifest.json"),
                    "dbtRunResultsFilePath": str(TARGET / "run_results.json"),
                },
                "dbtUpdateDescriptions": True,
                "dbtUpdateOwners": True,
                "includeTags": True,
            }
        },
    },
    # Airflow itself: pipelines, tasks, run status, and inlets/outlets lineage.
    "airflow": {
        "type": "airflow",
        "serviceName": "platform_airflow",
        "serviceConnection": {
            "config": {
                "type": "Airflow",
                "hostPort": "http://localhost:8080",
                "numberOfStatus": 10,
                "connection": {"type": "Backend"},
            }
        },
        "sourceConfig": {"config": {"type": "PipelineMetadata", "includeLineage": True}},
    },
}


def main(names: list[str]) -> int:
    for name in names:
        path = WORK / f"ingest_{name}.yaml"
        path.write_text(yaml.safe_dump(_workflow(WORKFLOWS[name])))
        print(f"=== {name}", flush=True)
        result = subprocess.run(["metadata", "ingest", "-c", str(path)], check=False)
        if result.returncode != 0:
            print(f"=== {name} FAILED ({result.returncode})")
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
