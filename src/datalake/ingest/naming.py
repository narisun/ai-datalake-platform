"""Turns whatever a file calls its columns into safe, predictable lake column names."""

from __future__ import annotations

import re
from collections.abc import Sequence

_NOT_ALLOWED = re.compile(r"[^a-z0-9]+")
SAFE_NAME = re.compile(r"[a-z][a-z0-9_]*")


def normalize_column(name: object, position: int) -> str:
    """Lower-case snake_case. `position` (1-based) names a column with no usable header."""
    text = _NOT_ALLOWED.sub("_", str(name if name is not None else "").strip().lower()).strip("_")
    if not text:
        return f"column_{position}"
    if text[0].isdigit():
        return f"c_{text}"
    return text


def normalize_columns(names: Sequence[object]) -> tuple[str, ...]:
    """Normalise every header and make repeats unique: amount, amount_2, amount_3."""
    result: list[str] = []
    seen: set[str] = set()
    for position, raw in enumerate(names, start=1):
        base = normalize_column(raw, position)
        name, suffix = base, 2
        while name in seen:
            name = f"{base}_{suffix}"
            suffix += 1
        seen.add(name)
        result.append(name)
    return tuple(result)
