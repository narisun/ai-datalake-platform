from __future__ import annotations

from dataclasses import dataclass

from datalake.core.security import Classification


@dataclass(frozen=True, slots=True)
class CatalogColumn:
    name: str
    data_type: str = ""
    description: str = ""
    # At least as sensitive as its table.
    classification: Classification = Classification.RESTRICTED


@dataclass(frozen=True, slots=True)
class CatalogTable:
    """A table as the metadata catalog describes it."""

    fqn: str  # the catalog's own name for it, for example lake_trino.lake.marts.fct_orders
    sql_name: str  # how to refer to it in SQL, for example lake.marts.fct_orders
    name: str
    schema: str
    description: str = ""
    # Empty domain and restricted classification are the defaults for untagged data,
    # which keeps it out of every role's reach.
    domain: str = ""
    classification: Classification = Classification.RESTRICTED
    owner: str = ""
    columns: tuple[CatalogColumn, ...] = ()
    glossary_terms: tuple[str, ...] = ()  # fully qualified, for example Business.Customer


@dataclass(frozen=True, slots=True)
class GlossaryTerm:
    fqn: str
    name: str
    description: str = ""
    synonyms: tuple[str, ...] = ()
