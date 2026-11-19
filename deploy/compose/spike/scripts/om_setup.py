"""Spike: prepare OpenMetadata. Run inside the ingestion container.

Fetches the ingestion bot's token and creates what dbt can only link to, not create:
the `sales` domain, the `Business.Customer` glossary term and the `sales-data` team.
"""

from __future__ import annotations

import base64
import pathlib

import requests

API = "http://openmetadata-server:8585/api/v1"
WORK = pathlib.Path("/opt/datalake/work")


def main() -> None:
    login = requests.post(
        f"{API}/users/login",
        json={
            "email": "admin@open-metadata.org",
            "password": base64.b64encode(b"admin").decode(),
        },
        timeout=30,
    )
    login.raise_for_status()
    admin = {"Authorization": f"Bearer {login.json()['accessToken']}"}

    bot = requests.get(f"{API}/users/name/ingestion-bot", headers=admin, timeout=30).json()
    mechanism = requests.get(
        f"{API}/users/auth-mechanism/{bot['id']}", headers=admin, timeout=30
    ).json()
    token = mechanism["config"]["JWTToken"]
    (WORK / "om_token").write_text(token)

    def put(path: str, body: dict[str, object]) -> None:
        response = requests.put(f"{API}/{path}", headers=admin, json=body, timeout=30)
        print(
            path,
            body.get("name"),
            response.status_code,
            response.text[:160] if response.status_code >= 400 else "",
        )

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
        {
            "name": "Business",
            "displayName": "Business",
            "description": "Company-wide business terms.",
        },
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
    print("token saved")


if __name__ == "__main__":
    main()
