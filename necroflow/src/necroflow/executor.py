from __future__ import annotations

import concurrent.futures
import os
import subprocess
from typing import TYPE_CHECKING

from necroflow.dag import check_cache, resolve_command, write_dependencies

if TYPE_CHECKING:
    from necroflow.pipeline import _GraphBase


def execute(pipeline: _GraphBase, outdir, total_threads: int | None = None) -> None:
    """Run all nodes in the pipeline, respecting the thread budget.

    Skips nodes whose outputs already exist (cache hits). Writes
    dependencies.toml after each successful job. Raises
    subprocess.CalledProcessError on the first failure.
    """
    total_threads = total_threads or os.cpu_count() or 1
    pipeline.resolve_paths(outdir)
    nodes = list(pipeline.nodes)

    done_ids = {id(n) for n in nodes if check_cache(n)}
    running: dict = {}  # future -> (node, threads_used)
    used_threads = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes) or 1) as pool:
        while len(done_ids) < len(nodes):
            running_ids = {id(n) for n, _ in running.values()}
            ready = [
                n for n in nodes
                if id(n) not in done_ids
                and id(n) not in running_ids
                and all(id(p) in done_ids for p in n.parents)
            ]
            for node in ready:
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
