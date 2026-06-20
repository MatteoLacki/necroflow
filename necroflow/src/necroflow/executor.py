from __future__ import annotations

import concurrent.futures
import os
import subprocess
from typing import TYPE_CHECKING, Callable

from necroflow.dag import (
    NodeState,
    _node_key,
    classify_nodes,
    resolve_command,
    write_dependencies,
)
from necroflow.state_db import StateDB

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
    import networkx as nx

    remaining_ids = {id(n) for n in remaining}
    G = nx.Graph()
    for n in remaining:
        G.add_node(id(n))
        for p in n.parents:
            if id(p) in remaining_ids:
                G.add_edge(id(n), id(p))

    node_to_component_size: dict[int, int] = {}
    for component in nx.connected_components(G):
        size = len(component)
        for nid in component:
            node_to_component_size[nid] = size

    return sorted(ready, key=lambda n: node_to_component_size.get(id(n), 0))


def execute(
    pipeline: _GraphBase,
    outdir,
    total_threads: int | None = None,
    scheduler: Scheduler | None = None,
    keep_going: bool = False,
) -> None:
    """Run required nodes in the pipeline, respecting the thread budget.

    Classifies each node as Missing/Stale/UpToDate/Orphan before execution.
    Skips UpToDate and Orphan nodes. Writes dependencies.toml after each
    successful job.

    keep_going=False (default): raise on the first failure.
    keep_going=True: continue running independent nodes; raise ExceptionGroup
    at the end listing all failures.
    """
    if scheduler is None:
        scheduler = connected_component_scheduler
    total_threads = total_threads or os.cpu_count() or 1
    pipeline.resolve_paths(outdir)
    nodes = list(pipeline.nodes)

    req = getattr(pipeline, "required_nodes", None)
    if req is None:
        req = nodes
    classify_nodes(nodes, req)

    # only operate on nodes in the required subgraph
    active = [n for n in nodes if n.state is not None and n.state != NodeState.ORPHAN]

    db = StateDB(outdir)
    compromised = db.compromised_keys()

    # reclassify UP_TO_DATE nodes that were compromised in a previous run
    for n in active:
        if n.state == NodeState.UP_TO_DATE and _node_key(n) in compromised:
            n.state = NodeState.STALE

    running: dict = {}  # future -> (node, threads_used)
    used_threads = 0
    errors: list = []  # exceptions collected in keep_going mode

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

                ready = [n for n in active if n.state == NodeState.READY]
                remaining = [n for n in active if n.state in _needs_run]
                for node in scheduler(ready, remaining):
                    t = _node_threads(node)
                    if used_threads + t <= total_threads or used_threads == 0:
                        db.mark_running(_node_key(node))
                        node.state = NodeState.RUNNING
                        future = pool.submit(_run_node, node)
                        running[future] = (node, t)
                        used_threads += t

                if not running:
                    break

                done_fs, _ = concurrent.futures.wait(
                    running, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for f in done_fs:
                    node, t = running.pop(f)
                    try:
                        f.result()
                        write_dependencies(node)
                        db.mark_done(_node_key(node), "up_to_date")
                        node.state = NodeState.UP_TO_DATE
                    except Exception as exc:
                        if (
                            isinstance(exc, subprocess.CalledProcessError)
                            and exc.returncode < 0
                        ):
                            node.state = NodeState.INTERRUPTED
                            db.mark_done(_node_key(node), "interrupted")
                        else:
                            node.state = NodeState.FAILED
                            db.mark_done(_node_key(node), "failed")
                        if not keep_going:
                            raise
                        errors.append(exc)
                    used_threads -= t
    finally:
        db.close()

    if errors:
        raise ExceptionGroup(
            "necroflow: some nodes failed",
            errors,
        )


def _node_threads(node) -> int:
    if node.rule and node.rule.constraints:
        return node.rule.constraints.get("threads", 1)
    return 1


def _run_node(node) -> None:
    cmd = resolve_command(node)
    node.path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(cmd, list):
        subprocess.run(cmd, check=True)
    else:
        subprocess.run(cmd, shell=True, check=True)
