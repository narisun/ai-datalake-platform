from __future__ import annotations

from typing import Protocol

from datalake.core.security import Principal
from datalake.policy.model import SearchFilter


class AccessPolicy(Protocol):
    """Decides what a caller may see. Pure: no I/O."""

    def search_filter(self, principal: Principal) -> SearchFilter: ...
