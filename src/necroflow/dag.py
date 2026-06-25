from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import tomlkit

from necroflow.nodes import Node, NodeState, NodeType, NodeTypeMeta, _is_nodetype, _topo_sort
from necroflow.rules import Inputs, Outputs, Constraints, Rule, Rules, parse_resource



def _content_hash(path: Path) -> str:
    """SHA-256 of a file's bytes, or of all non-.rip files in a directory."""
    h = hashlib.sha256()
    if path.is_file():
        h.update(path.read_bytes())
    else:
        for f in sorted(path.rglob("*")):
            if f.is_file() and ".rip" not in f.parts:
                h.update(str(f.relative_to(path)).encode())
                h.update(f.read_bytes())
    return h.hexdigest()


def _accumulated_config(node: Node) -> dict:
    config = {}
    for parent in node.parents:
        config.update(_accumulated_config(parent))
    config.update(node.config)
    return config


def write_dependencies(node: Node) -> None:
    """Write dependencies.toml and per-output content hashes into node.path.parent.

    Call after the job succeeds. Co-outputs share a directory, so calling this for
    any one of them writes metadata for all siblings via node.output_nodes.
    """
    data = {
        "rule": node.rule.__name__ if node.rule else "unknown",
        "hash": node.path.parent.name,
        "config": _accumulated_config(node),
    }
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "dependencies.toml").write_text(tomlkit.dumps(data))
    for onode in node.output_nodes.values():
        if onode.path is not None and onode.path.exists():
            (rip / (onode.path.name + ".hash")).write_text(_content_hash(onode.path))


def _output_mtime(path: Path) -> float:
    """Mtime of a node output. For directories, returns the max mtime of all files inside."""
    if path.is_dir():
        mtimes = [f.stat().st_mtime for f in path.rglob("*") if f.is_file()]
        return max(mtimes) if mtimes else path.stat().st_mtime
    return path.stat().st_mtime



def classify_nodes(nodes: list[Node], required_nodes: list[Node]) -> None:
    """Set node.state for each node. Requires resolve_paths() to have been called first.

    Nodes in the required subgraph (required_nodes + all ancestors) get Missing/Stale/UpToDate.
    Nodes outside the subgraph with existing output get Orphan.
    Nodes outside the subgraph with no output get state=None (excluded from execution).
    """
    # BFS to collect all nodes in the required subgraph
    required: dict[str, Node] = {}
    frontier = list(required_nodes)
    while frontier:
        n = frontier.pop()
        if n.key in required:
            continue
        required[n.key] = n
        frontier.extend(p for p in n.parents if p.key not in required)

    # ORPHAN pass: output exists from a prior run but isn't needed now; skipped
    # by the executor unless autoclean=True, in which case it gets deleted
    for node in nodes:
        if node.key not in required:
            node.state = NodeState.ORPHAN if (node.path is not None and node.path.exists()) else None

    # Classify in topological order so STALE propagates naturally in one pass
    for node in _topo_sort(list(required.values())):
        if node.path is None or not node.path.exists():
            node.state = NodeState.MISSING
            continue

        node_mtime = _output_mtime(node.path)
        stale = any(
            p.state in (NodeState.MISSING, NodeState.STALE)
            for p in node.parents
            if p.state is not None
        )
        if not stale:
            for p in node.parents:
                if p.path is None or not p.path.exists():
                    continue
                if _output_mtime(p.path) <= node_mtime:
                    continue  # fast path: parent not newer
                hash_file = p.path.parent / ".rip" / (p.path.name + ".hash")
                if hash_file.exists() and _content_hash(p.path) == hash_file.read_text().strip():
                    continue  # parent re-ran but content unchanged
                stale = True
                break
        node.state = NodeState.STALE if stale else NodeState.UP_TO_DATE


def resolve_command(node: Node) -> str | list[str] | None:
    """Format node.command with input/output paths and config values.

    Requires resolve_paths() to have been called first.
    Substitutions: {input_name} -> parent.path, {output_name} -> node.path, {config_key} -> value.
    """
    if node.command is None:
        return None
    pos_input_names = [n for n, t in node.rule.inputs.specs.items() if _is_nodetype(t)]
    subs: dict[str, Any] = {}
    for iname, parent in zip(pos_input_names, node.parents):
        subs[iname] = parent.path
    subs.update(node.config)
    for oname, onode in node.output_nodes.items():
        subs[oname] = onode.path
    if isinstance(node.command, list):
        return [c.format(**subs) for c in node.command]
    return node.command.format(**subs)


def resolve_paths(nodes: list[Node], outdir: Path | str) -> None:
    """Set node.path for each node: outdir / rule_name / hash8 / filename.

    Filename is node_type.name if set, else output_name (no extension).
    Co-outputs of the same rule call share the same hash directory.
    """
    outdir = Path(outdir)
    for node in nodes:
        node.path = outdir / node.key



