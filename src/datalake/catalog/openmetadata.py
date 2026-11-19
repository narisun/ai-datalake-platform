"""CatalogReader on OpenMetadata's REST API.

Plain HTTP on purpose: OpenMetadata's own Python package pins many dependencies and
would tie the platform's environment to its release cycle.
"""

# Responses are untyped JSON, so unknown-type checks are relaxed for this module.
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from datalake.catalog.model import CatalogColumn, CatalogTable, GlossaryTerm
from datalake.core.errors import DatalakeError
from datalake.core.security import Classification

_PAGE = 100


class CatalogError(DatalakeError):
    """The metadata catalog could not be read."""


class OpenMetadataCatalog:
    def __init__(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        token: str,
        service: str,
        classification_prefix: str = "dbtTags.",
    ) -> None:
        """`service` is the catalog's name for the lake (its Trino database service)."""
        self._http = http
        self._api = base_url.rstrip("/") + "/v1"
        self._headers = {"Authorization": f"Bearer {token}"}
        self._service = service
        self._prefix = classification_prefix

    async def tables(self) -> Sequence[CatalogTable]:
        entries = await self._pages(
            "tables", {"fields": "columns,tags,domains,owners", "include": "non-deleted"}
        )
        own = f"{self._service}."
        return [
            self._table(entry)
            for entry in entries
            if str(entry.get("fullyQualifiedName", "")).startswith(own)
        ]

    async def glossary_terms(self) -> Sequence[GlossaryTerm]:
        entries = await self._pages("glossaryTerms", {"include": "non-deleted"})
        return [
            GlossaryTerm(
                fqn=str(entry.get("fullyQualifiedName", "")),
                name=str(entry.get("displayName") or entry.get("name", "")),
                description=str(entry.get("description") or ""),
                synonyms=tuple(str(s) for s in entry.get("synonyms") or ()),
            )
            for entry in entries
        ]

    async def _pages(self, path: str, params: Mapping[str, str]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            query = {**params, "limit": str(_PAGE)}
            if after:
                query["after"] = after
            try:
                response = await self._http.get(
                    f"{self._api}/{path}", params=query, headers=self._headers
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise CatalogError(f"cannot read {path} from the metadata catalog: {exc}") from exc
            body: dict[str, Any] = response.json()
            results.extend(body.get("data") or [])
            after = (body.get("paging") or {}).get("after")
            if not after:
                return results

    def _classification(self, tags: Sequence[Mapping[str, Any]]) -> Classification | None:
        """The most sensitive classification tag present, or None if there is none."""
        found: list[Classification] = []
        for tag in tags:
            fqn = str(tag.get("tagFQN", ""))
            if fqn.startswith(self._prefix):
                try:
                    found.append(Classification.parse(fqn[len(self._prefix) :]))
                except ValueError:
                    continue
        return max(found) if found else None

    def _table(self, entry: Mapping[str, Any]) -> CatalogTable:
        fqn = str(entry["fullyQualifiedName"])
        tags: Sequence[Mapping[str, Any]] = entry.get("tags") or []
        classification = self._classification(tags) or Classification.RESTRICTED
        domains: Sequence[Mapping[str, Any]] = entry.get("domains") or []
        owners: Sequence[Mapping[str, Any]] = entry.get("owners") or []
        columns = tuple(
            CatalogColumn(
                name=str(column.get("name", "")),
                data_type=str(column.get("dataTypeDisplay") or column.get("dataType") or ""),
                description=str(column.get("description") or ""),
                classification=max(
                    classification, self._classification(column.get("tags") or []) or classification
                ),
            )
            for column in entry.get("columns") or []
        )
        return CatalogTable(
            fqn=fqn,
            sql_name=fqn[len(self._service) + 1 :],
            name=str(entry.get("name", "")),
            schema=str((entry.get("databaseSchema") or {}).get("name", "")),
            description=str(entry.get("description") or ""),
            domain=str(domains[0].get("name", "")) if domains else "",
            classification=classification,
            owner=str(owners[0].get("name", "")) if owners else "",
            columns=columns,
            glossary_terms=tuple(
                str(tag.get("tagFQN", "")) for tag in tags if tag.get("source") == "Glossary"
            ),
        )
