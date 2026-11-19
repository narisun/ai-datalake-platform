from __future__ import annotations

from collections.abc import Sequence

from datalake.transform.model import RunResult


class FakeTransformEngine:
    """Pretends to build the given models and records each selection."""

    def __init__(
        self,
        models: Sequence[str] = (),
        success: bool = True,
        artifacts_dir: str = "target",
    ) -> None:
        self._models = tuple(models)
        self._success = success
        self._artifacts_dir = artifacts_dir
        self.selections: list[str | None] = []

    async def build(self, select: str | None = None) -> RunResult:
        self.selections.append(select)
        built = self._models if select is None else tuple(m for m in self._models if m == select)
        return RunResult(self._success, built, self._artifacts_dir)
