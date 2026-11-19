"""Loads access.yml into the domain's AccessRules."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from datalake.core.errors import PolicyError
from datalake.core.security import Classification
from datalake.policy.model import AccessRules, ColumnMask, RoleRule


class _MaskModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column: str
    expression: str = Field(min_length=1)

    @field_validator("column")
    @classmethod
    def _model_dot_column(cls, value: str) -> str:
        parts = value.split(".")
        if len(parts) != 2 or not all(parts):
            raise ValueError("must be written as <model>.<column>")
        return value


class _RoleModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domains: list[str] = Field(min_length=1)
    max_classification: str
    masks: list[_MaskModel] = []

    @field_validator("max_classification")
    @classmethod
    def _known_classification(cls, value: str) -> str:
        Classification.parse(value)
        return value


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    roles: dict[str, _RoleModel] = Field(min_length=1)
    hidden_schemas: list[str] = []


def _to_mask(mask: _MaskModel) -> ColumnMask:
    model, column = mask.column.split(".")
    return ColumnMask(model=model, column=column, expression=mask.expression)


def parse_access_rules(text: str) -> AccessRules:
    """Parse and validate the YAML text of an access policy."""
    try:
        raw: Any = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PolicyError(f"access policy is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise PolicyError("access policy must be a mapping with a top-level 'roles' key")

    try:
        policy = _PolicyModel.model_validate(raw)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors()
        )
        raise PolicyError(f"access policy is invalid: {problems}") from exc

    roles: dict[str, RoleRule] = {}
    for name, role in policy.roles.items():
        masks = tuple(_to_mask(mask) for mask in role.masks)
        roles[name] = RoleRule(
            name=name,
            domains=frozenset(role.domains),
            max_classification=Classification.parse(role.max_classification),
            masks=masks,
        )
    return AccessRules(roles, frozenset(policy.hidden_schemas))


def load_access_rules(path: Path) -> AccessRules:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PolicyError(f"cannot read access policy {path}: {exc}") from exc
    return parse_access_rules(text)
