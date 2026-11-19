"""The core module may import only the standard library and itself."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import datalake

DOMAIN = Path(datalake.__file__).parent / "core"


def _imported_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_core_imports_only_stdlib_and_itself() -> None:
    files = sorted(DOMAIN.glob("*.py"))
    assert files, "core package not found"

    offenders = {
        f"{path.name}: {module}"
        for path in files
        for module in _imported_modules(path)
        if module.split(".")[0] not in sys.stdlib_module_names
        and not module.startswith("datalake.core")
    }
    assert not offenders
