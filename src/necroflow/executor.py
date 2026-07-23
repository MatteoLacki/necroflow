from __future__ import annotations

import concurrent.futures
import fcntl
import inspect
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from contextlib import contextmanager
from typing import TYPE_CHECKING

import tomlkit

from necroflow._compat import ExceptionGroup
from necroflow.dag import (
    NodeState,
    classify_nodes,
    resolve_command,
    write_dependencies,
)
from necroflow.pipeline import DAG, write_ancestor_graph
from necroflow.schedulers import (
    Scheduler,
    connected_component_scheduler,
    fifo_scheduler,
)
from necroflow import logger as _logger

if TYPE_CHECKING:
    from necroflow.dag import Node


@dataclass
class ExecutionEvent:
    node_key: str
    rule: str
    output_name: str | None
    pipeline_label: str | None
    path: str | None
    state: str
    cached: bool
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float | None = None
    exit_code: int | None = None
    error: str | None = None
    output_size_bytes: int | None = None
    output_size_human: str | None = None

    def to_toml_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "key": self.node_key,
            "rule": self.rule,
            "state": self.state,
            "cached": self.cached,
        }
        optional = {
            "output_name": self.output_name,
            "label": self.pipeline_label,
            "path": self.path,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "error": self.error,
            "output_size_bytes": self.output_size_bytes,
            "output_size_human": self.output_size_human,
        }
        data.update({k: v for k, v in optional.items() if v is not None})
        return data


@dataclass
class ExecutionReport:
    events: dict[str, ExecutionEvent] = field(default_factory=dict)

    def add(self, event: ExecutionEvent) -> None:
        self.events[event.node_key] = event

    def get(self, node_or_key) -> ExecutionEvent | None:
        key = (
            node_or_key
            if isinstance(node_or_key, str)
            else node_or_key.relative_path.as_posix()
        )
        return self.events.get(key)


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


def _rule_output_size_bytes(output_dir: Path) -> int:
    if not output_dir.exists():
        return 0
    total = 0
    for path in output_dir.rglob("*"):
        if ".rip" in path.parts or not path.is_file():
            continue
        total += path.stat().st_size
    return total


def _event_for_node(
    node,
    *,
    state: str,
    cached: bool,
    started_at: str | None = None,
    finished_at: str | None = None,
    duration_seconds: float | None = None,
    exit_code: int | None = None,
    error: str | None = None,
    output_size_bytes: int | None = None,
) -> ExecutionEvent:
    return ExecutionEvent(
        node_key=node.relative_path.as_posix(),
        rule=node.rule.__name__ if node.rule else "unknown",
        output_name=node.output_name,
        pipeline_label=node.rule_call.dag.label_for(node),
        path=str(node.path) if node.path is not None else None,
        state=state,
        cached=cached,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        exit_code=exit_code,
        error=error,
        output_size_bytes=output_size_bytes,
        output_size_human=(
            _human_size(output_size_bytes) if output_size_bytes is not None else None
        ),
    )


def _write_run_stats(node, event: ExecutionEvent) -> None:
    if node.path is None:
        return
    run = {
        "started_at": event.started_at,
        "finished_at": event.finished_at,
        "duration_seconds": event.duration_seconds,
        "exit_code": event.exit_code,
        "output_size_bytes": event.output_size_bytes,
        "output_size_human": event.output_size_human,
    }
    data = {"run": {k: v for k, v in run.items() if v is not None}}
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "run.toml").write_text(tomlkit.dumps(data), encoding="utf-8")


def _record_cached_events(report: ExecutionReport, active: list) -> None:
    measured_dirs: dict[Path, int] = {}
    for node in active:
        if node.state != NodeState.UP_TO_DATE or node.path is None:
            continue
        output_dir = node.path.parent
        size = measured_dirs.setdefault(output_dir, _rule_output_size_bytes(output_dir))
        report.add(
            _event_for_node(
                node,
                state="up_to_date",
                cached=True,
                output_size_bytes=size,
            )
        )


def _record_success_events(
    report: ExecutionReport,
    node,
    *,
    active_keys: set,
    needs_run: set,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
) -> None:
    size = _rule_output_size_bytes(node.path.parent)
    for conode in node.output_nodes.values():
        if conode.relative_path not in active_keys:
            continue
        if conode is not node and conode.state not in needs_run:
            continue
        event = _event_for_node(
            conode,
            state="up_to_date",
            cached=False,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            exit_code=0,
            output_size_bytes=size,
        )
        report.add(event)
    _write_run_stats(node, event)


def _record_failure_event(
    report: ExecutionReport,
    node,
    *,
    state: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    exit_code: int | None,
    error: str,
) -> None:
    report.add(
        _event_for_node(
            node,
            state=state,
            cached=False,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            exit_code=exit_code,
            error=error,
        )
    )


@contextmanager
def _acquire_lock(outdir: Path):
    """Context manager holding an exclusive fcntl lock on outdir/.rip/necroflow.lock.

    Raises RuntimeError immediately if another necroflow instance holds the lock.

    Only one necroflow instance per outdir is supported. Running two instances
    against overlapping outdirs (e.g. outdir and outdir/sub) is also unsupported
    and may corrupt outputs — there is no OS primitive to detect this case.
    """
    lock_path = outdir / ".rip" / "necroflow.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise RuntimeError(
            f"Another necroflow instance is already running against {outdir}.\n"
            "Only one instance per outdir is supported. If no instance is running, "
            f"delete the stale lock manually: {lock_path}"
        )
    try:
        yield
    finally:
        fh.close()


def _remove_rule_output_dir(node) -> bool:
    """Remove the whole rule-call output directory for node, if it exists."""
    if node.path is None:
        return False
    output_dir = node.path.parent
    if not output_dir.exists():
        return False
    shutil.rmtree(output_dir)
    _logger.cleaned(node)
    return True


def _remove_output_path(node) -> bool:
    """Remove only node.path, leaving siblings and side files in the rule-call dir."""
    if node.path is None or not node.path.exists():
        return False
    if node.path.is_dir():
        shutil.rmtree(node.path)
    else:
        node.path.unlink()
    _logger.cleaned(node)
    return True


def _can_remove_parent_dir(
    parent, children: dict, final_keys: set, active_keys: set
) -> bool:
    """Return True when every active co-output in a rule-call is cleanable."""
    siblings = [
        n for n in parent.output_nodes.values() if n.relative_path in active_keys
    ]
    if not siblings:
        return False
    if any(s.relative_path in final_keys for s in siblings):
        return False
    return all(
        all(c.state == NodeState.UP_TO_DATE for c in children[s.relative_path])
        for s in siblings
    )


def _cleanup_parents(node, children: dict, final_keys: set, active_keys: set) -> int:
    """Delete each finished intermediate parent's whole rule-call output directory."""
    n_cleaned = 0
    seen_dirs: set[Path] = set()
    for parent in node.parents:
        if (
            parent.relative_path not in active_keys
            or parent.relative_path in final_keys
            or parent.path is None
        ):
            continue
        output_dir = parent.path.parent
        if output_dir in seen_dirs:
            continue
        if _can_remove_parent_dir(parent, children, final_keys, active_keys):
            seen_dirs.add(output_dir)
            if _remove_rule_output_dir(parent):
                n_cleaned += 1
    return n_cleaned


def _propagate_stale(active: list, active_keys: set) -> None:
    """Propagate STALE from active parents to active UP_TO_DATE descendants."""
    changed = True
    while changed:
        changed = False
        for node in active:
            if node.state != NodeState.UP_TO_DATE:
                continue
            if any(
                p.relative_path in active_keys and p.state == NodeState.STALE
                for p in node.parents
            ):
                node.state = NodeState.STALE
                changed = True


def _prepare_active(
    dag,
    autoclean: bool,
    dry_run: bool,
    forced_stale_keys: set[Path] | None = None,
):
    """Classify eagerly addressed nodes, clean orphans, reclassify compromised.

    Returns (active, active_keys, n_cleaned):
      active      — nodes in the required subgraph (state is not None and not ORPHAN)
      active_keys — set of their relative paths
      n_cleaned   — number of orphan outputs deleted (only non-zero when autoclean=True)
    """
    nodes = list(dag.nodes)
    classify_nodes(nodes, dag.required_nodes)

    active = [n for n in nodes if n.state is not None and n.state != NodeState.ORPHAN]
    active_keys = {n.relative_path for n in active}

    if forced_stale_keys:
        for n in active:
            if n.relative_path in forced_stale_keys and n.state == NodeState.UP_TO_DATE:
                n.state = NodeState.STALE
        _propagate_stale(active, active_keys)

    n_cleaned = 0
    if autoclean and not dry_run:
        active_dirs = {n.path.parent for n in active if n.path is not None}
        cleaned_dirs: set[Path] = set()
        for n in nodes:
            if n.state != NodeState.ORPHAN or n.path is None:
                continue
            output_dir = n.path.parent
            if output_dir in cleaned_dirs:
                continue
            if output_dir not in active_dirs:
                if _remove_rule_output_dir(n):
                    cleaned_dirs.add(output_dir)
                    n_cleaned += 1
            elif _remove_output_path(n):
                n_cleaned += 1

    compromised = False
    for n in active:
        if n.state == NodeState.UP_TO_DATE and n.is_compromised:
            n.state = NodeState.STALE
            compromised = True
    if compromised:
        _propagate_stale(active, active_keys)

    return active, active_keys, n_cleaned


def _promote_states(active: list) -> None:
    """Advance node states one step: MISSING/STALE → READY or FAILED."""
    blocked = {NodeState.FAILED, NodeState.INTERRUPTED}
    for n in active:
        if n.state in (NodeState.MISSING, NodeState.STALE):
            if any(p.state in blocked for p in n.parents):
                n.state = NodeState.FAILED
            elif all(p.state == NodeState.UP_TO_DATE for p in n.parents):
                n.state = NodeState.READY


def _on_job_done(
    node,
    active_keys: set,
    needs_run: set,
    autoclean: bool,
    children: dict,
    final_keys: set,
    report: ExecutionReport,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
) -> int:
    """Handle a successful job completion. Returns number of intermediate outputs cleaned."""
    for conode in node.output_nodes.values():
        if conode.relative_path in active_keys and not conode.path.exists():
            raise RuntimeError(f"command succeeded but output missing: {conode.path}")
    _record_success_events(
        report,
        node,
        active_keys=active_keys,
        needs_run=needs_run,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
    )
    write_dependencies(node)
    write_ancestor_graph(node)
    n_cleaned = 0
    for conode in node.output_nodes.values():
        if (
            conode is not node
            and conode.relative_path in active_keys
            and conode.state in needs_run
        ):
            conode.mark_done("up_to_date")
            conode.state = NodeState.UP_TO_DATE
            if autoclean:
                n_cleaned += _cleanup_parents(conode, children, final_keys, active_keys)
    node.mark_done("up_to_date")
    node.state = NodeState.UP_TO_DATE
    if autoclean:
        n_cleaned += _cleanup_parents(node, children, final_keys, active_keys)
    return n_cleaned


def _validate_scheduler(scheduler: Scheduler) -> None:
    """Reject callables that cannot be called with the 3-argument protocol.

    The scheduler protocol is scheduler(ready, remaining, available_resources).
    A legacy 2-argument scheduler would otherwise fail deep inside the run loop
    after the lock is taken and nodes are classified; failing here names the
    expected protocol up front.
    """
    try:
        signature = inspect.signature(scheduler)
    except (TypeError, ValueError):
        return
    try:
        signature.bind([], [], {})
    except TypeError:
        name = getattr(scheduler, "__name__", type(scheduler).__name__)
        raise TypeError(
            f"scheduler {name!r} does not match the scheduler protocol: "
            "scheduler(ready, remaining, available_resources) -> list[Node]"
        ) from None


def execute(
    dag: DAG,
    resource_caps: dict[str, int] | None = None,
    scheduler: Scheduler = connected_component_scheduler,
    keep_going: bool = False,
    autoclean: bool = False,
    dry_run: bool = False,
    node_runner=None,
    forced_stale_keys: set[Path] | None = None,
) -> ExecutionReport:
    """Run the DAG's required nodes, respecting declared resource caps.

    Classifies each node as Missing/Stale/UpToDate/Orphan before execution.
    Skips UpToDate and Orphan nodes. Writes dependencies.toml after each
    successful job.

    resource_caps: {resource: int} upper bounds (e.g. {"threads": 8, "ram": 4*2**30}).
    Defaults to {"threads": os.cpu_count()}. Resources not in caps are unconstrained.
    A job whose requirements exceed a cap still runs solo when nothing else is running.

    keep_going=False (default): raise on the first failure.
    keep_going=True: continue running independent nodes; raise ExceptionGroup
    at the end listing all failures.

    node_runner: optional callable(node, log_path) replacing _run_node. Use this to
    intercept subprocess execution (e.g. to feed output to a TUI).
    """
    if not isinstance(dag, DAG):
        raise TypeError(f"execute requires a DAG, got {type(dag).__name__}")
    _validate_scheduler(scheduler)
    _run = node_runner if node_runner is not None else _run_node
    _logger.setup()
    caps: dict[str, int] = {"threads": os.cpu_count() or 1}
    if resource_caps:
        caps.update(resource_caps)
    outdir = dag.nodes_dir
    with _acquire_lock(outdir):
        active, active_keys, n_cleaned = _prepare_active(
            dag, autoclean, dry_run, forced_stale_keys
        )
        report = ExecutionReport()
        _record_cached_events(report, active)

        if dry_run:
            n_would_run = sum(
                1 for n in active if n.state in (NodeState.MISSING, NodeState.STALE)
            )
            n_up_to_date = sum(1 for n in active if n.state == NodeState.UP_TO_DATE)
            for n in active:
                if n.state in (NodeState.MISSING, NodeState.STALE):
                    _logger.dry_run_node(n)
            _logger.dry_run_summary(n_would_run, n_up_to_date)
            return report

        running: dict = {}  # future -> (node, start_time, start_wall, job_resources)
        running_resources: dict[str, int] = {}
        errors: list = []  # exceptions collected in keep_going mode
        n_run = n_failed = 0
        n_skipped = sum(1 for n in active if n.state == NodeState.UP_TO_DATE)

        needs_run = {
            NodeState.MISSING,
            NodeState.STALE,
            NodeState.READY,
            NodeState.RUNNING,
        }

        if autoclean:
            children: dict[Path, list] = {n.relative_path: [] for n in active}
            for n in active:
                for p in n.parents:
                    if p.relative_path in active_keys:
                        children[p.relative_path].append(n)
            final_keys = {k for k, kids in children.items() if not kids}
        else:
            children, final_keys = {}, set()

        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(active) or 1
            ) as pool:
                while any(n.state in needs_run for n in active):
                    _promote_states(active)

                    ready = [n for n in active if n.state == NodeState.READY]
                    remaining = [n for n in active if n.state in needs_run]
                    available_resources = {
                        resource: cap - running_resources.get(resource, 0)
                        for resource, cap in caps.items()
                    }
                    for node in scheduler(ready, remaining, available_resources):
                        # skip co-outputs whose sibling is already running
                        coouts = [
                            c
                            for c in node.output_nodes.values()
                            if c.relative_path in active_keys and c is not node
                        ]
                        if any(c.state == NodeState.RUNNING for c in coouts):
                            continue
                        job_res = node.rule.resources
                        # run if all capped resources have room, or nothing else is running (solo fallback)
                        can_run = (not running) or all(
                            running_resources.get(r, 0) + v <= caps[r]
                            for r, v in job_res.items()
                            if r in caps
                        )
                        if can_run:
                            log_path = node.path.parent / ".rip" / "job.log"
                            node.mark_running()
                            node.state = NodeState.RUNNING
                            _logger.job_start(node)
                            start = time.monotonic()
                            start_wall = _utc_now()
                            future = pool.submit(_run, node, log_path)
                            running[future] = (node, start, start_wall, job_res)
                            for r, v in job_res.items():
                                running_resources[r] = running_resources.get(r, 0) + v

                    if not running:
                        break

                    done_fs, _ = concurrent.futures.wait(
                        running, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for f in done_fs:
                        node, start, start_wall, job_res = running.pop(f)
                        elapsed = time.monotonic() - start
                        finished_wall = _utc_now()
                        try:
                            f.result()
                            n_cleaned += _on_job_done(
                                node,
                                active_keys,
                                needs_run,
                                autoclean,
                                children,
                                final_keys,
                                report,
                                start_wall,
                                finished_wall,
                                elapsed,
                            )
                            _logger.job_done(node, elapsed)
                            n_run += 1
                        except Exception as exc:
                            log_path = node.path.parent / ".rip" / "job.log"
                            if isinstance(exc, subprocess.CalledProcessError):
                                rc = exc.returncode
                                if rc < 0:
                                    node.state = NodeState.INTERRUPTED
                                    node.mark_done("interrupted")
                                    state = "interrupted"
                                else:
                                    node.state = NodeState.FAILED
                                    node.mark_done("failed")
                                    state = "failed"
                                _record_failure_event(
                                    report,
                                    node,
                                    state=state,
                                    started_at=start_wall,
                                    finished_at=finished_wall,
                                    duration_seconds=elapsed,
                                    exit_code=rc,
                                    error=str(exc),
                                )
                                _logger.job_failed(node, elapsed, rc, log_path)
                            else:
                                node.state = NodeState.FAILED
                                node.mark_done("failed")
                                _record_failure_event(
                                    report,
                                    node,
                                    state="failed",
                                    started_at=start_wall,
                                    finished_at=finished_wall,
                                    duration_seconds=elapsed,
                                    exit_code=None,
                                    error=str(exc),
                                )
                                _logger.job_error(node, elapsed, exc, log_path)
                            _logger.job_output(log_path)
                            n_failed += 1
                            if not keep_going:
                                raise
                            errors.append(exc)
                        for r, v in job_res.items():
                            running_resources[r] -= v
        finally:
            _logger.summary(n_run, n_skipped, n_failed, n_cleaned)

    if errors:
        exc = ExceptionGroup(
            "necroflow: some nodes failed",
            errors,
        )
        setattr(exc, "execution_report", report)
        raise exc
    return report


def _run_node(node, log_path) -> None:
    node.path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as log:
        materializer = getattr(node.rule, "materializer", None)
        if materializer is not None:
            materializer(node, log)
            return
        cmd = resolve_command(node)
        if cmd is None:
            raise RuntimeError(
                f"rule {node.rule.__name__!r} has neither a command nor a materializer"
            )
        shellpath = node.rule_call.shellpath
        if shellpath is not None:
            subprocess.run(
                cmd,
                shell=True,
                executable=shellpath,
                check=True,
                stdout=log,
                stderr=log,
            )
        else:
            subprocess.run(cmd, shell=True, check=True, stdout=log, stderr=log)
