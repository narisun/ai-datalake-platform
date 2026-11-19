from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class Classification(IntEnum):
    """Sensitivity of data, ordered from least to most sensitive."""

    PUBLIC = 0
    INTERNAL = 1
    CONFIDENTIAL = 2
    RESTRICTED = 3

    @classmethod
    def parse(cls, name: str) -> Classification:
        try:
            return cls[name.strip().upper()]
        except KeyError as exc:
            allowed = ", ".join(member.name.lower() for member in cls)
            raise ValueError(
                f"unknown classification {name!r}; expected one of: {allowed}"
            ) from exc


@dataclass(frozen=True, slots=True)
class Principal:
    """The signed-in caller: who they are and the roles their token carries."""

    user_id: str
    roles: frozenset[str] = frozenset()
