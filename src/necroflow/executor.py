from __future__ import annotations

import concurrent.futures
import fcntl
import os
import shutil
import subprocess
import time
from pathlib import Path
from contextlib import contextmanager
from typing import TYPE_CHECKING

from necroflow.dag import (
    NodeState,
    classify_nodes,
    resolve_command,
    write_dependencies,
)
from necroflow.schedulers import Scheduler, connected_component_scheduler, fifo_scheduler
from necroflow import logger as _logger

if TYPE_CHECKING:
    from necroflow.dag import Node
    from necroflow.pipeline import _GraphBase


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


def _cleanup_parents(node, children: dict, final_keys: set, active_keys: set) -> int:
    """Delete output of each parent whose all children are now UP_TO_DATE (intermediates only)."""
    n_cleaned = 0
    for parent in node.parents:
        if parent.key not in active_keys or parent.key in final_keys:
            continue
        if all(c.state == NodeState.UP_TO_DATE for c in children[parent.key]):
            if parent.path is not None and parent.path.exists():
                if parent.path.is_dir():
                    shutil.rmtree(parent.path)
                else:
                    parent.path.unlink()
                _logger.cleaned(parent)
                n_cleaned += 1
    return n_cleaned


def _prepare_active(pipeline, outdir: Path, autoclean: bool, dry_run: bool):
    """Resolve paths, classify nodes, clean orphans, reclassify compromised.

    Returns (active, active_keys, n_cleaned):
      active      — nodes in the required subgraph (state is not None and not ORPHAN)
      active_keys — set of their keys
      n_cleaned   — number of orphan outputs deleted (only non-zero when autoclean=True)
    """
    pipeline.resolve_paths(outdir)
    nodes = list(pipeline.nodes)

    # After DAG deduplication, unique nodes may hold parent references to
    # superseded objects that are never classified.  Remap every parent pointer
    # to the canonical node (same .key) so classify_nodes and the state
    # machine operate on a consistent graph.
    canonical = {n.key: n for n in nodes}
    for n in nodes:
        n.parents[:] = [canonical.get(p.key, p) for p in n.parents]

    req = getattr(pipeline, "required_nodes", None)
    classify_nodes(nodes, req if req is not None else nodes)

    active = [n for n in nodes if n.state is not None and n.state != NodeState.ORPHAN]
    active_keys = {n.key for n in active}

    n_cleaned = 0
    if autoclean and not dry_run:
        for n in nodes:
            if n.state == NodeState.ORPHAN and n.path is not None and n.path.exists():
                if n.path.is_dir():
                    shutil.rmtree(n.path)
                else:
                    n.path.unlink()
                _logger.cleaned(n)
                n_cleaned += 1

    for n in active:
        if n.state == NodeState.UP_TO_DATE and n.is_compromised:
            n.state = NodeState.STALE

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


def _on_job_done(node, active_keys: set, needs_run: set, autoclean: bool,
                 children: dict, final_keys: set) -> int:
    """Handle a successful job completion. Returns number of intermediate outputs cleaned."""
    for conode in node.output_nodes.values():
        if conode.key in active_keys and not conode.path.exists():
            raise RuntimeError(f"command succeeded but output missing: {conode.path}")
    write_dependencies(node)
    n_cleaned = 0
    for conode in node.output_nodes.values():
        if conode is not node and conode.key in active_keys and conode.state in needs_run:
            conode.mark_done("up_to_date")
            conode.state = NodeState.UP_TO_DATE
            if autoclean:
                n_cleaned += _cleanup_parents(conode, children, final_keys, active_keys)
    node.mark_done("up_to_date")
    node.state = NodeState.UP_TO_DATE
    if autoclean:
        n_cleaned += _cleanup_parents(node, children, final_keys, active_keys)
    return n_cleaned


def execute(
    pipeline: _GraphBase,
    outdir,
    resource_caps: dict[str, int] | None = None,
    scheduler: Scheduler = connected_component_scheduler,
    keep_going: bool = False,
    autoclean: bool = False,
    dry_run: bool = False,
) -> None:
    """Run required nodes in the pipeline, respecting declared resource caps.

    Classifies each node as Missing/Stale/UpToDate/Orphan before execution.
    Skips UpToDate and Orphan nodes. Writes dependencies.toml after each
    successful job.

    resource_caps: {resource: int} upper bounds (e.g. {"threads": 8, "ram": 4*2**30}).
    Defaults to {"threads": os.cpu_count()}. Resources not in caps are unconstrained.
    A job whose requirements exceed a cap still runs solo when nothing else is running.

    keep_going=False (default): raise on the first failure.
    keep_going=True: continue running independent nodes; raise ExceptionGroup
    at the end listing all failures.
    """
    _logger.setup()
    caps: dict[str, int] = {"threads": os.cpu_count() or 1}
    if resource_caps:
        caps.update(resource_caps)
    outdir = Path(outdir)
    with _acquire_lock(outdir):
        active, active_keys, n_cleaned = _prepare_active(pipeline, outdir, autoclean, dry_run)

        if dry_run:
            n_would_run = sum(1 for n in active if n.state in (NodeState.MISSING, NodeState.STALE))
            n_up_to_date = sum(1 for n in active if n.state == NodeState.UP_TO_DATE)
            for n in active:
                if n.state in (NodeState.MISSING, NodeState.STALE):
                    _logger.dry_run_node(n)
            _logger.dry_run_summary(n_would_run, n_up_to_date)
            return

        running: dict = {}  # future -> (node, start_time, job_resources)
        running_resources: dict[str, int] = {}
        errors: list = []  # exceptions collected in keep_going mode
        n_run = n_failed = 0
        n_skipped = sum(1 for n in active if n.state == NodeState.UP_TO_DATE)

        needs_run = {NodeState.MISSING, NodeState.STALE, NodeState.READY, NodeState.RUNNING}

        if autoclean:
            children: dict[str, list] = {n.key: [] for n in active}
            for n in active:
                for p in n.parents:
                    if p.key in active_keys:
                        children[p.key].append(n)
            final_keys = {k for k, kids in children.items() if not kids}
        else:
            children, final_keys = {}, set()

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(active) or 1) as pool:
                while any(n.state in needs_run for n in active):
                    _promote_states(active)

                    ready = [n for n in active if n.state == NodeState.READY]
                    remaining = [n for n in active if n.state in needs_run]
                    for node in scheduler(ready, remaining):
                        # skip co-outputs whose sibling is already running or done
                        coouts = [c for c in node.output_nodes.values() if c.key in active_keys and c is not node]
                        if any(c.state in (NodeState.RUNNING, NodeState.UP_TO_DATE) for c in coouts):
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
                            future = pool.submit(_run_node, node, log_path)
                            running[future] = (node, start, job_res)
                            for r, v in job_res.items():
                                running_resources[r] = running_resources.get(r, 0) + v

                    if not running:
                        break

                    done_fs, _ = concurrent.futures.wait(
                        running, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for f in done_fs:
                        node, start, job_res = running.pop(f)
                        elapsed = time.monotonic() - start
                        try:
                            f.result()
                            n_cleaned += _on_job_done(node, active_keys, needs_run, autoclean, children, final_keys)
                            _logger.job_done(node, elapsed)
                            n_run += 1
                        except Exception as exc:
                            log_path = node.path.parent / ".rip" / "job.log"
                            if isinstance(exc, subprocess.CalledProcessError):
                                rc = exc.returncode
                                if rc < 0:
                                    node.state = NodeState.INTERRUPTED
                                    node.mark_done("interrupted")
                                else:
                                    node.state = NodeState.FAILED
                                    node.mark_done("failed")
                                _logger.job_failed(node, elapsed, rc, log_path)
                            else:
                                node.state = NodeState.FAILED
                                node.mark_done("failed")
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
        raise ExceptionGroup(
            "necroflow: some nodes failed",
            errors,
        )



def _run_node(node, log_path) -> None:
    cmd = resolve_command(node)
    node.path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as log:
        if isinstance(cmd, list):
            subprocess.run(cmd, check=True, stdout=log, stderr=log)
        else:
            subprocess.run(cmd, shell=True, check=True, stdout=log, stderr=log)
