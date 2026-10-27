"""Running one RuleCall: the default runner, retries, and command timing."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess

from necroflow import logger as _logger
from necroflow.containers import docker_argv
from necroflow.rule_call import RuleCall

Runner = Callable[[RuleCall, Path], None]


def run_call(call: RuleCall, log_path: Path) -> None:
    """Default runner: execute a call's materializer or realized command.

    A command whose `{name}:` prefix selects a Docker input runs through
    `docker run`; any other command runs in a host shell. Output and errors go
    to `log_path`.
    """
    call.workdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as log:
        materializer = call.rule.materializer
        if materializer is not None:
            materializer(call, log)
            return
        command = call.resolve()
        if command is None:
            raise RuntimeError(
                f"rule {call.rule.__name__!r} has neither a command nor a materializer"
            )
        container = call.container
        if container is not None:
            argv = docker_argv(call, container, command)
            subprocess.run(argv, check=True, stdout=log, stderr=log)
            return
        subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=log,
            stderr=log,
            executable=call.shellpath,
        )


@dataclass(frozen=True)
class Outcome:
    """How one submitted RuleCall's command attempts ended."""

    started_at: str
    finished_at: str
    error: Exception | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def exit_code(self) -> int | None:
        """Exit status of a failed process; None when no process failed."""
        if isinstance(self.error, subprocess.CalledProcessError):
            return self.error.returncode
        return None

    @property
    def interrupted(self) -> bool:
        """Whether a signal ended the process (negative exit status)."""
        return self.exit_code is not None and self.exit_code < 0


def attempt(call: RuleCall, runner: Runner) -> Outcome:
    """Run `call` up to `rule.repeat` times, stopping at the first success.

    Only process failures are retried. Declared outputs are cleared before
    every attempt, so a replay never sees a prior output. Timestamps are taken
    here, in the worker thread: the coordinator may be hashing another call's
    outputs when this one finishes, and that must not count as its runtime.
    """
    started_at = _utc_now()
    maximum = call.rule.repeat
    error: Exception | None = None
    for number in range(1, maximum + 1):
        try:
            call.validate_dependencies()
            call.clear_outputs()
            runner(call, call.log_path())
            return Outcome(started_at, _utc_now())
        except subprocess.CalledProcessError as exc:
            error = exc
            if number < maximum:
                _logger.job_retry(call, number, maximum, exc.returncode)
        except Exception as exc:
            return Outcome(started_at, _utc_now(), exc)
    return Outcome(started_at, _utc_now(), error)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
