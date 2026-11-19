from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from datalake.core.security import Classification, Principal


@dataclass(frozen=True, slots=True)
class ColumnMask:
    """Replace a column's value with a SQL expression for one role."""

    model: str
    column: str
    expression: str


@dataclass(frozen=True, slots=True)
class RoleRule:
    """What one role may see: which domains, and up to which classification."""

    name: str
    domains: frozenset[str]
    max_classification: Classification
    masks: tuple[ColumnMask, ...] = ()


@dataclass(frozen=True, slots=True)
class SearchFilter:
    """The data a caller may see, as domain -> highest classification allowed.

    A domain that is absent is not visible at all, so an empty filter denies everything.
    """

    scopes: Mapping[str, Classification]

    def allows(self, domain: str, classification: Classification) -> bool:
        ceiling = self.scopes.get(domain)
        return ceiling is not None and classification <= ceiling

    @property
    def denies_everything(self) -> bool:
        return not self.scopes


@dataclass(frozen=True, slots=True)
class AccessRules:
    """The whole access policy: every role and what it grants."""

    roles: Mapping[str, RoleRule]
    # Schemas no role may read and search never shows, whatever their tables' tags say.
    # For working layers (staging) that only the pipeline should touch.
    hidden_schemas: frozenset[str] = frozenset()

    def search_filter(self, principal: Principal) -> SearchFilter:
        """Combine the caller's roles. Roles unknown to the policy grant nothing."""
        scopes: dict[str, Classification] = {}
        for role in principal.roles:
            rule = self.roles.get(role)
            if rule is None:
                continue
            for domain in rule.domains:
                current = scopes.get(domain)
                if current is None or rule.max_classification > current:
                    scopes[domain] = rule.max_classification
        return SearchFilter(MappingProxyType(scopes))
