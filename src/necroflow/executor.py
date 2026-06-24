from __future__ import annotations

import concurrent.futures
import os
import shutil
import subprocess
import time
from typing import TYPE_CHECKING, Callable

from necroflow.dag import (
    NodeState,
    _node_key,
    classify_nodes,
    parse_resource,
    resolve_command,
    write_dependencies,
)
from necroflow.state_db import StateDB
from necroflow import logger as _logger

if TYPE_CHECKING:
    from necroflow.dag import Node
    from necroflow.pipeline import _GraphBase

# Scheduler protocol:
#   scheduler(ready, remaining) -> list[Node]
# ready     -- nodes whose parents are all done, not yet running
# remaining -- all not-yet-done, not-yet-running nodes (superset of ready)
# Returns ready nodes in priority order; executor submits from the front.
Scheduler = Callable[["list[Node]", "list[Node]"], "list[Node]"]


def fifo_scheduler(ready: list, remaining: list) -> list:
    """Submit ready nodes in topological (registration) order."""
    return ready


def connected_component_scheduler(ready: list, remaining: list) -> list:
    """Prioritise nodes from the smallest connected component of remaining work."""
    remaining_ids = {id(n) for n in remaining}

    # build undirected adjacency within remaining (parent↔child edges)
    adj: dict[int, list] = {id(n): [] for n in remaining}
    for n in remaining:
        for p in n.parents:
            if id(p) in remaining_ids:
                adj[id(n)].append(p)
                adj[id(p)].append(n)

    visited: set[int] = set()
    node_to_size: dict[int, int] = {}
    for n in remaining:
        if id(n) in visited:
            continue
        frontier = [n]
        members: list[int] = []
        while frontier:
            cur = frontier.pop()
            if id(cur) in visited:
                continue
            visited.add(id(cur))
            members.append(id(cur))
            for nb in adj[id(cur)]:
                if id(nb) not in visited:
                    frontier.append(nb)
        size = len(members)
        for nid in members:
            node_to_size[nid] = size

    return sorted(ready, key=lambda n: node_to_size.get(id(n), 0))


def _cleanup_parents(node, children: dict, final_ids: set, active_id_set: set) -> int:
    """Delete output of each parent whose all children are now UP_TO_DATE (intermediates only)."""
    n_cleaned = 0
    for parent in node.parents:
        if id(parent) not in active_id_set or id(parent) in final_ids:
            continue
        if all(c.state == NodeState.UP_TO_DATE for c in children[id(parent)]):
            if parent.path is not None and parent.path.exists():
                if parent.path.is_dir():
                    shutil.rmtree(parent.path)
                else:
                    parent.path.unlink()
                _logger.cleaned(parent)
                n_cleaned += 1
    return n_cleaned


def execute(
    pipeline: _GraphBase,
    outdir,
    resource_caps: dict[str, int] | None = None,
    scheduler: Scheduler | None = None,
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
    if scheduler is None:
        scheduler = connected_component_scheduler
    caps: dict[str, int] = {"threads": os.cpu_count() or 1}
    if resource_caps:
        caps.update(resource_caps)
    pipeline.resolve_paths(outdir)
    nodes = list(pipeline.nodes)

    # After DAG deduplication, unique nodes may hold parent references to
    # superseded objects that are never classified.  Remap every parent pointer
    # to the canonical node (same _node_key) so classify_nodes and the state
    # machine operate on a consistent graph.
    canonical = {_node_key(n): n for n in nodes}
    for n in nodes:
        n.parents[:] = [canonical.get(_node_key(p), p) for p in n.parents]

    req = getattr(pipeline, "required_nodes", None)
    if req is None:
        req = nodes
    classify_nodes(nodes, req)

    # only operate on nodes in the required subgraph
    active = [n for n in nodes if n.state is not None and n.state != NodeState.ORPHAN]

    active_id_set = {id(n) for n in active}
    children: dict[int, list] = {id(n): [] for n in active}
    for n in active:
        for p in n.parents:
            if id(p) in active_id_set:
                children[id(p)].append(n)
    final_ids = {nid for nid, kids in children.items() if not kids}

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

    db = StateDB(outdir)
    compromised = db.compromised_keys()

    # reclassify UP_TO_DATE nodes that were compromised in a previous run
    for n in active:
        if n.state == NodeState.UP_TO_DATE and _node_key(n) in compromised:
            n.state = NodeState.STALE

    if dry_run:
        db.close()
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
    n_run = n_skipped = n_failed = 0

    n_skipped = sum(1 for n in active if n.state == NodeState.UP_TO_DATE)

    _blocked = {NodeState.FAILED, NodeState.INTERRUPTED}
    _needs_run = {NodeState.MISSING, NodeState.STALE, NodeState.READY, NodeState.RUNNING}

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(active) or 1) as pool:
            while any(n.state in _needs_run for n in active):
                # promote Missing/Stale: blocked parent → FAILED; all parents UP_TO_DATE → READY
                for n in active:
                    if n.state in (NodeState.MISSING, NodeState.STALE):
                        if any(p.state in _blocked for p in n.parents):
                            n.state = NodeState.FAILED
                        elif all(p.state == NodeState.UP_TO_DATE for p in n.parents):
                            n.state = NodeState.READY

                active_ids = {id(n) for n in active}
                ready = [n for n in active if n.state == NodeState.READY]
                remaining = [n for n in active if n.state in _needs_run]
                for node in scheduler(ready, remaining):
                    # skip co-outputs whose sibling is already running or done
                    coouts = [c for c in node.output_nodes.values() if id(c) in active_ids and c is not node]
                    if any(c.state in (NodeState.RUNNING, NodeState.UP_TO_DATE) for c in coouts):
                        continue
                    job_res = _node_resources(node)
                    # run if all capped resources have room, or nothing else is running (solo fallback)
                    can_run = (not running) or all(
                        running_resources.get(r, 0) + v <= caps[r]
                        for r, v in job_res.items()
                        if r in caps
                    )
                    if can_run:
                        log_path = node.path.parent / ".rip" / "job.log"
                        db.mark_running(_node_key(node))
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
                        # validate all active co-outputs were produced
                        for conode in node.output_nodes.values():
                            if id(conode) in active_ids and not conode.path.exists():
                                raise RuntimeError(f"command succeeded but output missing: {conode.path}")
                        write_dependencies(node)
                        # mark co-outputs that were skipped in the scheduler
                        for conode in node.output_nodes.values():
                            if conode is not node and id(conode) in active_ids and conode.state in _needs_run:
                                db.mark_done(_node_key(conode), "up_to_date")
                                conode.state = NodeState.UP_TO_DATE
                                if autoclean:
                                    n_cleaned += _cleanup_parents(conode, children, final_ids, active_id_set)
                        db.mark_done(_node_key(node), "up_to_date")
                        node.state = NodeState.UP_TO_DATE
                        _logger.job_done(node, elapsed)
                        n_run += 1
                        if autoclean:
                            n_cleaned += _cleanup_parents(node, children, final_ids, active_id_set)
                    except Exception as exc:
                        log_path = node.path.parent / ".rip" / "job.log"
                        if isinstance(exc, subprocess.CalledProcessError):
                            rc = exc.returncode
                            if rc < 0:
                                node.state = NodeState.INTERRUPTED
                                db.mark_done(_node_key(node), "interrupted")
                            else:
                                node.state = NodeState.FAILED
                                db.mark_done(_node_key(node), "failed")
                            _logger.job_failed(node, elapsed, rc, log_path)
                        else:
                            node.state = NodeState.FAILED
                            db.mark_done(_node_key(node), "failed")
                            _logger.job_error(node, elapsed, exc, log_path)
                        _logger.job_output(log_path)
                        n_failed += 1
                        if not keep_going:
                            raise
                        errors.append(exc)
                    for r, v in job_res.items():
                        running_resources[r] -= v
    finally:
        db.close()
        _logger.summary(n_run, n_skipped, n_failed, n_cleaned)

    if errors:
        raise ExceptionGroup(
            "necroflow: some nodes failed",
            errors,
        )


def _node_resources(node) -> dict[str, int]:
    """Parse all constraint values for a node. threads defaults to 1 if not declared."""
    raw = node.rule.constraints if node.rule and node.rule.constraints else {}
    result = {k: parse_resource(v) for k, v in raw.items()}
    result.setdefault("threads", 1)
    return result


def _run_node(node, log_path) -> None:
    cmd = resolve_command(node)
    node.path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as log:
        if isinstance(cmd, list):
            subprocess.run(cmd, check=True, stdout=log, stderr=log)
        else:
            subprocess.run(cmd, shell=True, check=True, stdout=log, stderr=log)
