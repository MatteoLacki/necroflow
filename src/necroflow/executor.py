"""Local atomic RuleCall execution."""

from __future__ import annotations

import concurrent.futures
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import inspect
import os
from pathlib import Path
import subprocess
from typing import Any

import tomlkit

from necroflow import logger as _logger
from necroflow.tgf import write_ancestor_tgf
from necroflow.dag import DAG
from necroflow.fs import _acquire_lock
from necroflow.planning import ExecutionPlan, classify_available, plan_execution
from necroflow.rule_call import RuleCall, RuleCallState
from necroflow.schedulers import Scheduler, fifo_scheduler


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

    def duration_seconds(self) -> float | None:
        """Return elapsed wall-clock seconds when both timestamps exist."""
        if self.started_at is None or self.finished_at is None:
            return None
        started = datetime.fromisoformat(self.started_at)
        finished = datetime.fromisoformat(self.finished_at)
        return (finished - started).total_seconds()

    def to_toml_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "key": self.call_key,
            "rule": self.rule,
            "state": self.state,
            "cached": self.cached,
            "workdir": self.workdir,
            "output_node_keys": list(self.output_node_keys),
            "output_paths": list(self.output_paths),
        }
        optional = {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds(),
            "exit_code": self.exit_code,
            "error": self.error,
            "output_size_bytes": self.output_size_bytes,
            "output_size_human": (
                _human_size(self.output_size_bytes)
                if self.output_size_bytes is not None
                else None
            ),
        }
        data.update(
            {key: value for key, value in optional.items() if value is not None}
        )
        return data


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _human_size(size: int) -> str:
    value = float(size)
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def _write_run_stats(call: RuleCall, event: RuleCallExecution) -> None:
    run = {
        "started_at": event.started_at,
        "finished_at": event.finished_at,
        "duration_seconds": event.duration_seconds(),
        "exit_code": event.exit_code,
        "output_size_bytes": event.output_size_bytes,
        "output_size_human": (
            _human_size(event.output_size_bytes)
            if event.output_size_bytes is not None
            else None
        ),
    }
    data = {"run": {key: value for key, value in run.items() if value is not None}}
    rip = call.workdir / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "run.toml").write_text(tomlkit.dumps(data), encoding="utf-8")


def _record_cached(report: dict[str, RuleCallExecution], calls: list[RuleCall]) -> int:
    added = 0
    for call in calls:
        key = call.relative_path.as_posix()
        if call.state != RuleCallState.UP_TO_DATE or key in report:
            continue
        report[key] = RuleCallExecution.from_call(
            call,
            state="up_to_date",
            cached=True,
            output_size_bytes=call.output_size_bytes(),
        )
        added += 1
    return added


@dataclass
class _AutocleanPlan:
    enabled: bool
    children: dict[Path, list[RuleCall]] = field(default_factory=dict)
    final_keys: set[Path] = field(default_factory=set)


def _build_autoclean_plan(enabled: bool, active: list[RuleCall]) -> _AutocleanPlan:
    if not enabled:
        return _AutocleanPlan(enabled=False)
    active_keys = {call.relative_path for call in active}
    children = {call.relative_path: [] for call in active}
    for call in active:
        for parent in call.parent_calls:
            if parent.relative_path in active_keys:
                children[parent.relative_path].append(call)
    requested_keys = {
        node.rule_call.relative_path
        for call in active[:1]
        for node in call.dag.required_nodes
    }
    final_keys = requested_keys | {
        key for key, values in children.items() if not values
    }
    return _AutocleanPlan(True, children, final_keys)


def _cleanup_parents(call: RuleCall, plan: _AutocleanPlan) -> int:
    if not plan.enabled:
        return 0
    cleaned = 0
    for parent in call.parent_calls:
        key = parent.relative_path
        if parent.mutable or key in plan.final_keys:
            continue
        if (
            all(
                child.state == RuleCallState.UP_TO_DATE
                for child in plan.children.get(key, ())
            )
            and parent.remove_workdir()
        ):
            _logger.cleaned(parent)
            cleaned += 1
    return cleaned


def _clean_orphans(plan: ExecutionPlan, *, autoclean: bool, dry_run: bool) -> int:
    if not autoclean or dry_run:
        return 0
    cleaned = [call for call in plan.orphans if call.remove_workdir()]
    for call in cleaned:
        _logger.cleaned(call)
    return len(cleaned)


def _validate_scheduler(scheduler: Scheduler) -> None:
    try:
        signature = inspect.signature(scheduler)
    except (TypeError, ValueError):
        return
    try:
        signature.bind([], [], {})
    except TypeError:
        name = getattr(scheduler, "__name__", type(scheduler).__name__)
        raise TypeError(
            f"scheduler {name!r} does not match protocol: "
            "scheduler(ready, remaining, available_resources) -> list[RuleCall]"
        ) from None


def _validated_schedule(
    scheduler: Scheduler,
    ready: list[RuleCall],
    remaining: list[RuleCall],
    available_resources: dict[str, int],
) -> list[RuleCall]:
    selected = scheduler(ready, remaining, available_resources)
    if not isinstance(selected, list):
        raise TypeError(
            f"scheduler must return list[RuleCall], got {type(selected).__name__}"
        )
    ready_by_key = {call.relative_path: call for call in ready}
    seen: set[Path] = set()
    result: list[RuleCall] = []
    for call in selected:
        key = getattr(call, "relative_path", None)
        if key not in ready_by_key:
            raise ValueError(f"scheduler returned RuleCall that is not ready: {key}")
        if key in seen:
            raise ValueError(f"scheduler returned duplicate RuleCall: {key}")
        seen.add(key)
        result.append(ready_by_key[key])
    return result


def _run_with_retries(call: RuleCall, runner) -> None:
    maximum = call.rule.repeat
    for attempt in range(1, maximum + 1):
        try:
            runner(call, call.log_path())
            return
        except subprocess.CalledProcessError as exc:
            if attempt == maximum:
                raise
            _logger.job_retry(call, attempt, maximum, exc.returncode)


def _promote_ready(active: list[RuleCall]) -> None:
    for call in active:
        if call.state in {RuleCallState.MISSING, RuleCallState.STALE} and all(
            parent.state == RuleCallState.UP_TO_DATE for parent in call.parent_calls
        ):
            call.state = RuleCallState.READY


def _complete_call(
    call: RuleCall,
    plan: ExecutionPlan,
    report: dict[str, RuleCallExecution],
    *,
    started_at: str,
    finished_at: str,
) -> RuleCallExecution:
    missing = [output.path for output in call.outputs if not output.path.exists()]
    if missing:
        raise RuntimeError(
            "command succeeded but output missing: " + ", ".join(map(str, missing))
        )
    call.write_dependencies(plan.hash_cache)
    if call.outputs:
        write_ancestor_tgf(call.outputs[0])
    call.mark_done("up_to_date")
    call.state = RuleCallState.UP_TO_DATE
    event = RuleCallExecution.from_call(
        call,
        state="up_to_date",
        cached=False,
        started_at=started_at,
        finished_at=finished_at,
        exit_code=0,
        output_size_bytes=call.output_size_bytes(),
    )
    report[event.call_key] = event
    _write_run_stats(call, event)
    return event


def run(
    dag: DAG,
    resource_caps: dict[str, int] | None = None,
    scheduler: Scheduler | None = None,
    keep_going: bool = False,
    autoclean: bool = False,
    dry_run: bool = False,
    rule_call_runner=None,
    forced_stale_call_keys: set[Path] | None = None,
    on_complete: Callable[[dict[str, RuleCallExecution]], None] | None = None,
) -> dict[str, RuleCallExecution]:
    """Execute required RuleCalls atomically; return report keyed by call path.

    • forced_stale_call_keys is a set of canonical RuleCall.relative_path values forced to rerun despite valid cache.
    """
    if not isinstance(dag, DAG):
        raise TypeError(f"run requires a DAG, got {type(dag).__name__}")
    scheduler = fifo_scheduler if scheduler is None else scheduler
    _validate_scheduler(scheduler)
    runner = RuleCall.run if rule_call_runner is None else rule_call_runner
    _logger.setup()
    caps = {"threads": os.cpu_count() or 1}
    if resource_caps:
        caps.update(resource_caps)

    with _acquire_lock(dag.nodes_dir):
        plan = plan_execution(dag, forced_stale_call_keys=forced_stale_call_keys)
        n_cleaned = _clean_orphans(plan, autoclean=autoclean, dry_run=dry_run)
        report: dict[str, RuleCallExecution] = {}
        n_skipped = _record_cached(report, plan.active)

        if dry_run:
            would_run = sum(
                call.state in {RuleCallState.MISSING, RuleCallState.STALE}
                for call in plan.active
            )
            for call in plan.active:
                if call.state in {RuleCallState.MISSING, RuleCallState.STALE}:
                    _logger.dry_run_node(call)
            _logger.dry_run_summary(would_run, n_skipped)
            return report

        running: dict = {}
        running_resources: dict[str, int] = {}
        errors: list[Exception] = []
        executed_call_keys: set[Path] = set()
        n_run = 0
        n_failed = 0
        autoclean_plan = _build_autoclean_plan(autoclean, plan.active)
        unfinished = {
            RuleCallState.MISSING,
            RuleCallState.STALE,
            RuleCallState.READY,
            RuleCallState.RUNNING,
        }

        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(plan.active) or 1
            ) as pool:
                while any(
                    call.state is None or call.state in unfinished
                    for call in plan.active
                ):
                    classify_available(plan, executed_call_keys=executed_call_keys)
                    n_skipped += _record_cached(report, plan.active)
                    _promote_ready(plan.active)
                    ready = [
                        call
                        for call in plan.active
                        if call.state == RuleCallState.READY
                    ]
                    remaining = [
                        call
                        for call in plan.active
                        if call.state is None or call.state in unfinished
                    ]
                    available = {
                        name: cap - running_resources.get(name, 0)
                        for name, cap in caps.items()
                    }
                    for call in _validated_schedule(
                        scheduler, ready, remaining, available
                    ):
                        can_run = not running or all(
                            running_resources.get(name, 0) + amount <= caps[name]
                            for name, amount in call.resources.items()
                            if name in caps
                        )
                        if not can_run:
                            continue
                        call.mark_running()
                        call.state = RuleCallState.RUNNING
                        _logger.job_start(call)
                        start_wall = _utc_now()
                        future = pool.submit(_run_with_retries, call, runner)
                        running[future] = (call, start_wall)
                        for name, amount in call.resources.items():
                            running_resources[name] = (
                                running_resources.get(name, 0) + amount
                            )

                    if not running:
                        break

                    done, _ = concurrent.futures.wait(
                        running, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for future in done:
                        call, start_wall = running.pop(future)
                        finished_wall = _utc_now()
                        try:
                            future.result()
                            event = _complete_call(
                                call,
                                plan,
                                report,
                                started_at=start_wall,
                                finished_at=finished_wall,
                            )
                            executed_call_keys.add(call.relative_path)
                            n_cleaned += _cleanup_parents(call, autoclean_plan)
                            _logger.job_done(call, event.duration_seconds())
                            n_run += 1
                        except Exception as exc:
                            exit_code = (
                                exc.returncode
                                if isinstance(exc, subprocess.CalledProcessError)
                                else None
                            )
                            interrupted = exit_code is not None and exit_code < 0
                            call.state = (
                                RuleCallState.INTERRUPTED
                                if interrupted
                                else RuleCallState.FAILED
                            )
                            state = "interrupted" if interrupted else "failed"
                            call.mark_done(state)
                            event = RuleCallExecution.from_call(
                                call,
                                state=state,
                                cached=False,
                                started_at=start_wall,
                                finished_at=finished_wall,
                                exit_code=exit_code,
                                error=str(exc),
                            )
                            report[event.call_key] = event
                            if exit_code is not None:
                                _logger.job_failed(
                                    call,
                                    event.duration_seconds(),
                                    exit_code,
                                    call.log_path(),
                                )
                            else:
                                _logger.job_error(
                                    call,
                                    event.duration_seconds(),
                                    exc,
                                    call.log_path(),
                                )
                            _logger.job_output(call.log_path())
                            n_failed += 1
                            if not keep_going:
                                raise
                            errors.append(exc)
                        finally:
                            for name, amount in call.resources.items():
                                running_resources[name] -= amount
        finally:
            _logger.summary(n_run, n_skipped, n_failed, n_cleaned)

        if on_complete is not None:
            on_complete(report)

    if errors:
        error = ExceptionGroup("necroflow: some RuleCalls failed", errors)
        setattr(error, "execution_report", report)
        raise error
    return report
