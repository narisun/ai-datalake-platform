from __future__ import annotations

from datalake.core.security import Principal
from datalake.policy.model import AccessRules, SearchFilter


class RoleAccessPolicy:
    """The AccessPolicy used in every profile: evaluates the rules from access.yml."""

    def __init__(self, rules: AccessRules) -> None:
        self._rules = rules

    def search_filter(self, principal: Principal) -> SearchFilter:
        return self._rules.search_filter(principal)
