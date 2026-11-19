"""Run any command as a registered process: `dl exec <process> -- <command>`.

This is how tools the platform does not own (for example OpenMetadata's ingestion)
become registered, recorded runs like everything else.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping, Sequence

from datalake.control.errors import ControlError
from datalake.control.service import Control

Runner = Callable[[Sequence[str], Mapping[str, str]], int]


class CommandFailedError(ControlError):
    def __init__(self, command: Sequence[str], exit_code: int) -> None:
        super().__init__(f"command exited with code {exit_code}: {' '.join(command)}")
        self.exit_code = exit_code


def run_subprocess(command: Sequence[str], environment: Mapping[str, str]) -> int:
    return subprocess.run(list(command), env=dict(environment), check=False).returncode


def run_external(
    control: Control,
    process: str,
    command: Sequence[str],
    environment: Mapping[str, str],
    runner: Runner = run_subprocess,
) -> None:
    """Run `command` inside a run of `process`. The child sees DL_RUN_ID.

    Raises CommandFailedError, marking the run failed, if the command exits non-zero.
    """
    if not command:
        raise ControlError("no command given; usage: dl exec <process> -- <command>")
    with control.start(process) as run, run.step(command[0]):
        exit_code = runner(command, {**environment, "DL_RUN_ID": run.run_id})
        if exit_code != 0:
            raise CommandFailedError(command, exit_code)
