from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from datalake.core.security import Classification, Principal
from datalake.policy.model import SearchFilter


class StaticAccessPolicy:
    """Gives every caller the same scope. Pass nothing to deny everything."""

    def __init__(self, scopes: Mapping[str, Classification] | None = None) -> None:
        self._filter = SearchFilter(MappingProxyType(dict(scopes or {})))

    def search_filter(self, principal: Principal) -> SearchFilter:
        return self._filter
