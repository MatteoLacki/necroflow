from __future__ import annotations

import concurrent.futures
import os
import subprocess
from typing import TYPE_CHECKING, Callable

from necroflow.dag import check_cache, resolve_command, write_dependencies

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
) -> None:
    """Run all nodes in the pipeline, respecting the thread budget.

    Skips nodes whose outputs already exist (cache hits). Writes
    dependencies.toml after each successful job. Raises
    subprocess.CalledProcessError on the first failure.
    """
    if scheduler is None:
        scheduler = connected_component_scheduler
    total_threads = total_threads or os.cpu_count() or 1
    pipeline.resolve_paths(outdir)
    nodes = list(pipeline.nodes)

    done_ids = {id(n) for n in nodes if check_cache(n)}
    running: dict = {}  # future -> (node, threads_used)
    used_threads = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes) or 1) as pool:
        while len(done_ids) < len(nodes):
            running_ids = {id(n) for n, _ in running.values()}
            remaining = [
                n for n in nodes
                if id(n) not in done_ids and id(n) not in running_ids
            ]
            ready = [
                n for n in remaining
                if all(id(p) in done_ids for p in n.parents)
            ]
            for node in scheduler(ready, remaining):
                t = _node_threads(node)
                if used_threads + t <= total_threads or used_threads == 0:
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
                f.result()
                write_dependencies(node)
                done_ids.add(id(node))
                used_threads -= t


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
