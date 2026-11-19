"""Turns catalog entries into the text chunks the agent searches.

What each chunk says, and who may find it, is decided here:

- A table's chunk carries its description and only the columns at the table's own
  classification. More sensitive columns go in a separate chunk at their own level,
  so a role that cannot see `email` is not told the column exists.
- A business term is indexed once per domain it is used in, so its synonyms lead to
  that domain's tables without revealing tables in other domains.
- A table with no domain is not indexed: no role could ever be shown it.
- A table in a hidden schema (see access.yml) is not indexed either.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Sequence

from datalake.catalog.model import CatalogColumn, CatalogTable, GlossaryTerm
from datalake.core.security import Classification
from datalake.search.model import Chunk


def _chunk(
    chunk_id: str, entity: str, text: str, domain: str, classification: Classification
) -> Chunk:
    digest = hashlib.sha256(f"{domain}|{int(classification)}|{text}".encode()).hexdigest()
    return Chunk(chunk_id, entity, text, domain, classification, digest)


def _column_line(column: CatalogColumn) -> str:
    kind = f" ({column.data_type})" if column.data_type else ""
    description = f": {column.description}" if column.description else ""
    return f"- {column.name}{kind}{description}"


def build_chunks(
    tables: Sequence[CatalogTable],
    terms: Sequence[GlossaryTerm],
    hidden_schemas: frozenset[str] = frozenset(),
) -> list[Chunk]:
    by_fqn = {term.fqn: term for term in terms}
    chunks: list[Chunk] = []
    term_tables: dict[tuple[str, str], list[str]] = defaultdict(list)

    for table in sorted(tables, key=lambda t: t.fqn):
        if not table.domain or table.schema in hidden_schemas:
            continue

        lines = [f"Table {table.sql_name}"]
        if table.description:
            lines.append(table.description)
        for term_fqn in table.glossary_terms:
            term_tables[(term_fqn, table.domain)].append(table.sql_name)
            term = by_fqn.get(term_fqn)
            if term is not None:
                also = f" (also called: {', '.join(term.synonyms)})" if term.synonyms else ""
                lines.append(f"Business term: {term.name}{also}. {term.description}".rstrip())
        own = [c for c in table.columns if c.classification <= table.classification]
        if own:
            lines.append("Columns:")
            lines.extend(_column_line(column) for column in own)
        chunks.append(
            _chunk(
                f"table:{table.fqn}",
                table.fqn,
                "\n".join(lines),
                table.domain,
                table.classification,
            )
        )

        sensitive: dict[Classification, list[CatalogColumn]] = defaultdict(list)
        for column in table.columns:
            if column.classification > table.classification:
                sensitive[column.classification].append(column)
        for level, columns in sorted(sensitive.items()):
            text = "\n".join(
                [
                    f"Table {table.sql_name} also has these {level.name.lower()} columns:",
                    *(_column_line(column) for column in columns),
                ]
            )
            chunks.append(
                _chunk(
                    f"table:{table.fqn}#{level.name.lower()}", table.fqn, text, table.domain, level
                )
            )

    for (term_fqn, domain), names in sorted(term_tables.items()):
        term = by_fqn.get(term_fqn)
        if term is None:
            continue
        also = f" (also called: {', '.join(term.synonyms)})" if term.synonyms else ""
        text = "\n".join(
            [
                f"Business term: {term.name}{also}",
                term.description,
                f"Found in: {', '.join(sorted(names))}",
            ]
        )
        chunks.append(
            _chunk(f"term:{term.fqn}@{domain}", term.fqn, text, domain, Classification.PUBLIC)
        )
    return chunks
