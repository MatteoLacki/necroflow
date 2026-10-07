"""Per-RuleCall execution report events and their TOML records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import tomlkit

from necroflow.rule_call import RuleCall


@dataclass
class RuleCallExecution:
    """One RuleCall cache hit or execution attempt."""

    call_key: str
    rule: str
    state: str
    cached: bool
    workdir: str
    output_node_keys: tuple[str, ...]
    output_paths: tuple[str, ...]
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    error: str | None = None
    output_size_bytes: int | None = None

    @classmethod
    def from_call(
        cls,
        call: RuleCall,
        *,
        state: str,
        cached: bool,
        started_at: str | None = None,
        finished_at: str | None = None,
        exit_code: int | None = None,
        error: str | None = None,
        output_size_bytes: int | None = None,
    ) -> RuleCallExecution:
        """Build one report event from a canonical RuleCall."""
        return cls(
            call_key=call.relative_path.as_posix(),
            rule=call.rule.__name__,
            state=state,
            cached=cached,
            workdir=str(call.workdir),
            output_node_keys=tuple(
                output.relative_path.as_posix() for output in call.outputs
            ),
            output_paths=tuple(str(output.path) for output in call.outputs),
            started_at=started_at,
            finished_at=finished_at,
            exit_code=exit_code,
            error=error,
            output_size_bytes=output_size_bytes,
        )

    def interval(self) -> tuple[datetime, datetime] | None:
        """Return command start and finish times when both exist."""
        if self.started_at is None or self.finished_at is None:
            return None
        return (
            datetime.fromisoformat(self.started_at),
            datetime.fromisoformat(self.finished_at),
        )

    def duration_seconds(self) -> float | None:
        """Return elapsed wall-clock seconds when both timestamps exist."""
        interval = self.interval()
        if interval is None:
            return None
        started, finished = interval
        return (finished - started).total_seconds()

    def to_toml_dict(self) -> dict[str, Any]:
        """Return the whole event: identity, outputs, then status fields."""
        return {
            "key": self.call_key,
            "rule": self.rule,
            "output_node_keys": list(self.output_node_keys),
            "output_paths": list(self.output_paths),
            **self.status_fields(),
        }

    def status_fields(self) -> dict[str, Any]:
        """Return state, cache status, workdir, and the measurements that are set."""
        return {
            "state": self.state,
            "cached": self.cached,
            "workdir": self.workdir,
            **self._measurements(error=self.error),
        }

    def write_run_toml(self) -> None:
        """Write call timings, exit code, and size to `.rip/run.toml`."""
        rip = Path(self.workdir) / ".rip"
        rip.mkdir(parents=True, exist_ok=True)
        data = {"run": self._measurements(error=None)}
        (rip / "run.toml").write_text(tomlkit.dumps(data), encoding="utf-8")

    def _measurements(self, *, error: str | None) -> dict[str, Any]:
        """Return the timing, exit, error, and size fields that are set."""
        size = self.output_size_bytes
        fields = {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds(),
            "exit_code": self.exit_code,
            "error": error,
            "output_size_bytes": size,
            "output_size_human": None if size is None else human_size(size),
        }
        return {key: value for key, value in fields.items() if value is not None}


def human_size(size: int) -> str:
    """Format a byte count with binary units, e.g. `1.5 MiB`."""
    if size < 1024:
        return f"{size} B"
    value = float(size)
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        value /= 1024
        if value < 1024:
            return f"{value:.1f} {unit}"
    return f"{value / 1024:.1f} PiB"


def count_events(report: dict[str, RuleCallExecution]) -> tuple[int, int, int]:
    """Return (executed successfully, cached, failed or interrupted) counts."""
    completed = cached = failed = 0
    for event in report.values():
        if event.cached:
            cached += 1
        elif event.state == "up_to_date":
            completed += 1
        else:
            failed += 1
    return completed, cached, failed
