"""Set the dev stack up after its first start. Safe to run again.

Run inside the airflow container, with Airflow's own Python:

    docker compose -f deploy/compose/dev/docker-compose.yml exec airflow \\
        python /opt/datalake/dev/bootstrap.py

It does what cannot be expressed in the Compose file:
  1. creates the control schema, registers the processes and sets their configuration
  2. creates the `lake` catalog in Polaris
  3. creates in OpenMetadata what dbt can only link to: tags, domain, glossary, team
  4. writes OpenMetadata's ingestion workflow files and ingests the sample source
"""

from __future__ import annotations

import base64
import os
import subprocess
import time
from pathlib import Path

import requests
import yaml

WORK = Path("/opt/datalake/work")
POLARIS = "http://polaris:8181"
OPENMETADATA = "http://openmetadata:8585/api"
LAKE_LOCATION = "file:///data/lake"

PROCESSES = {
    "ingest": "Load sources into the raw layer",
    "transform": "Build and test the dbt project",
    "access-rules": "Generate Trino access rules from the policy",
    "catalog-sync": "Refresh OpenMetadata from Trino, dbt and Airflow",
}

CONFIG = {
    "trino_host": "trino",
    "trino_port": "8080",
    "trino_catalog": "lake",
    "dbt_project_dir": "/opt/datalake/dbt",
    "dbt_target_dir": str(WORK / "target"),
    "dbt_log_dir": str(WORK / "logs"),
}

CLASSIFICATIONS = {
    "public": "May be shared outside the company.",
    "internal": "For anyone inside the company.",
    "confidential": "Limited to people who need it; includes personal data.",
    "restricted": "Most sensitive. Also the default for anything not yet tagged.",
}


def dl(*args: str) -> None:
    subprocess.run(["dl", *args], check=True)


def control() -> None:
    print("== control database")
    dl("db", "upgrade")
    for name, description in PROCESSES.items():
        dl("process", "register", name, "--description", description)
    for key, value in CONFIG.items():
        dl("config", "set", f"{key}={value}")


def polaris() -> None:
    print("== polaris catalog")
    token = requests.post(
        f"{POLARIS}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": os.environ["POLARIS_CLIENT_ID"],
            "client_secret": os.environ["POLARIS_CLIENT_SECRET"],
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=30,
    )
    token.raise_for_status()
    headers = {"Authorization": f"Bearer {token.json()['access_token']}"}
    management = f"{POLARIS}/api/management/v1"

    if requests.get(f"{management}/catalogs/lake", headers=headers, timeout=30).status_code == 200:
        print("catalog lake already exists")
    else:
        created = requests.post(
            f"{management}/catalogs",
            headers=headers,
            json={
                "catalog": {
                    "name": "lake",
                    "type": "INTERNAL",
                    "properties": {"default-base-location": LAKE_LOCATION},
                    "storageConfigInfo": {
                        "storageType": "FILE",
                        "allowedLocations": [LAKE_LOCATION],
                    },
                }
            },
            timeout=30,
        )
        print("create catalog lake:", created.status_code, created.text[:200])
        created.raise_for_status()

    granted = requests.put(
        f"{management}/catalogs/lake/catalog-roles/catalog_admin/grants",
        headers=headers,
        json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        timeout=30,
    )
    print("grant CATALOG_MANAGE_CONTENT:", granted.status_code)
    granted.raise_for_status()


def openmetadata() -> None:
    print("== openmetadata")
    api = f"{OPENMETADATA}/v1"
    login = requests.post(
        f"{api}/users/login",
        json={
            "email": "admin@open-metadata.org",
            "password": base64.b64encode(b"admin").decode(),
        },
        timeout=30,
    )
    login.raise_for_status()
    admin = {"Authorization": f"Bearer {login.json()['accessToken']}"}

    bot = requests.get(f"{api}/users/name/ingestion-bot", headers=admin, timeout=30).json()
    mechanism = requests.get(f"{api}/users/auth-mechanism/{bot['id']}", headers=admin, timeout=30)
    token = mechanism.json()["config"]["JWTToken"]

    def put(path: str, body: dict[str, object]) -> None:
        response = requests.put(f"{api}/{path}", headers=admin, json=body, timeout=30)
        detail = response.text[:160] if response.status_code >= 400 else ""
        print(f"  {path} {body.get('name')}: {response.status_code} {detail}")
        response.raise_for_status()

    # dbt ingestion links to these but does not create them (or creates tags too late
    # to attach them to columns on the first run).
    put("classifications", {"name": "dbtTags", "description": "Data classification, set in dbt."})
    for name, description in CLASSIFICATIONS.items():
        put("tags", {"name": name, "classification": "dbtTags", "description": description})
    put(
        "domains",
        {
            "name": "sales",
            "displayName": "Sales",
            "domainType": "Aggregate",
            "description": "Orders, customers and revenue.",
        },
    )
    put(
        "glossaries",
        {"name": "Business", "displayName": "Business", "description": "Business terms."},
    )
    put(
        "glossaryTerms",
        {
            "name": "Customer",
            "glossary": "Business",
            "description": "A person or organisation that has placed at least one order.",
            "synonyms": ["client", "account holder"],
        },
    )
    put("teams", {"name": "sales-data", "displayName": "Sales Data", "teamType": "Group"})

    write_workflows(token)
    print("== ingest the sample source into openmetadata")
    subprocess.run(["metadata", "ingest", "-c", str(WORK / "ingest_shop.yaml")], check=True)


def write_workflows(token: str) -> None:
    """OpenMetadata's ingestion workflow files, used by the nightly pipeline."""
    target = WORK / "target"
    sources: dict[str, dict[str, object]] = {
        "shop": {
            "type": "postgres",
            "serviceName": "shop_db",
            "serviceConnection": {
                "config": {
                    "type": "Postgres",
                    "username": "shop",
                    "authType": {"password": os.environ["SHOP_DB_PASSWORD"]},
                    "hostPort": "postgres:5432",
                    "database": "shop",
                }
            },
            "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
        },
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
        "dbt": {
            "type": "dbt",
            "serviceName": "lake_trino",
            "sourceConfig": {
                "config": {
                    "type": "DBT",
                    "dbtConfigSource": {
                        "dbtConfigType": "local",
                        "dbtCatalogFilePath": str(target / "catalog.json"),
                        "dbtManifestFilePath": str(target / "manifest.json"),
                        "dbtRunResultsFilePath": str(target / "run_results.json"),
                    },
                    # dbt is the home of these descriptions, so it overwrites.
                    "dbtUpdateDescriptions": True,
                    "dbtUpdateOwners": True,
                    "includeTags": True,
                }
            },
        },
        "airflow": {
            "type": "airflow",
            "serviceName": "platform_airflow",
            "serviceConnection": {
                "config": {
                    "type": "Airflow",
                    "hostPort": "http://localhost:8080",
                    "numberOfStatus": 10,
                    # Airflow 3 hides its database from tasks, so the connection is
                    # given explicitly instead of read from the task's environment.
                    "connection": {
                        "type": "Postgres",
                        "username": os.environ["DB_USER"],
                        "authType": {"password": os.environ["DB_PASSWORD"]},
                        "hostPort": f"{os.environ['DB_HOST']}:{os.environ['DB_PORT']}",
                        "database": os.environ["AIRFLOW_DB"],
                    },
                }
            },
            "sourceConfig": {"config": {"type": "PipelineMetadata", "includeLineage": True}},
        },
    }
    for name, source in sources.items():
        workflow = {
            "source": source,
            "sink": {"type": "metadata-rest", "config": {}},
            "workflowConfig": {
                "loggerLevel": "WARN",
                "openMetadataServerConfig": {
                    "hostPort": OPENMETADATA,
                    "authProvider": "openmetadata",
                    "securityConfig": {"jwtToken": token},
                },
            },
        }
        path = WORK / f"ingest_{name}.yaml"
        path.write_text(yaml.safe_dump(workflow), encoding="utf-8")
        path.chmod(0o600)
    print(f"  wrote {len(sources)} workflow files to {WORK}")


def wait_for(url: str, name: str) -> None:
    for _ in range(60):
        try:
            if requests.get(url, timeout=5).status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise SystemExit(f"{name} did not become ready at {url}")


if __name__ == "__main__":
    wait_for(f"{OPENMETADATA}/v1/system/version", "OpenMetadata")
    control()
    polaris()
    openmetadata()
    print("== done. Trigger the `nightly` pipeline in Airflow (http://localhost:8080).")
