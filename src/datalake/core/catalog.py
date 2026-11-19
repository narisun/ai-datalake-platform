from __future__ import annotations

from dataclasses import dataclass

from datalake.core.security import Classification


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    data_type: str
    description: str = ""
    classification: Classification = Classification.INTERNAL


@dataclass(frozen=True, slots=True)
class Dataset:
    """A table or dbt model in the lake."""

    urn: str
    name: str
    domain: str
    description: str = ""
    owner: str = ""
    classification: Classification = Classification.INTERNAL
    columns: tuple[Column, ...] = ()
    schema: str = ""


@dataclass(frozen=True, slots=True)
class TermBinding:
    """Links a glossary term to a model, or to one column of a model."""

    model: str
    column: str | None = None


@dataclass(frozen=True, slots=True)
class Term:
    """A business term in the glossary (the lightweight ontology)."""

    name: str
    description: str = ""
    synonyms: tuple[str, ...] = ()
    related: tuple[str, ...] = ()
    parent: str | None = None
    bound_to: tuple[TermBinding, ...] = ()


@dataclass(frozen=True, slots=True)
class LineageEdge:
    upstream_urn: str
    downstream_urn: str
