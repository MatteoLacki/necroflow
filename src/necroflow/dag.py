from __future__ import annotations

import hashlib
import os
import shlex
from pathlib import Path
from typing import Any

import tomlkit

from necroflow.nodes import (
    Node,
    NodeState,
    NodeType,
    NodeTypeMeta,
    _topo_sort,
)
from necroflow.fingerprints import command_ast, python_identity
from necroflow.rules import parse_resource, _is_node_input_contract


def _filesystem_limits(path: Path) -> tuple[int | None, int | None]:
    """Return (NAME_MAX, PATH_MAX) for the nearest existing parent of path."""
    for candidate in (path, *path.parents):
        if not candidate.exists():
            continue
        try:
            name_max = os.pathconf(candidate, "PC_NAME_MAX")
        except (OSError, ValueError):
            name_max = None
        try:
            path_max = os.pathconf(candidate, "PC_PATH_MAX")
        except (OSError, ValueError):
            path_max = None
        return name_max, path_max
    return None, None


def _check_path_limits(path: Path) -> None:
    name_max, path_max = _filesystem_limits(path)
    if name_max is not None:
        for part in path.parts:
            if part in (path.anchor, os.sep, ""):
                continue
            length = len(os.fsencode(part))
            if length > name_max:
                raise ValueError(
                    f"path component too long ({length} > NAME_MAX {name_max}): {part!r}"
                )
    if path_max is not None:
        length = len(os.fsencode(os.fspath(path)))
        if length > path_max:
            raise ValueError(f"path too long ({length} > PATH_MAX {path_max}): {path}")


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


def _invalidator(node: Node):
    if node.node_type is None:
        return None
    return getattr(node.node_type, "invalidator", None)


def _invalidation_file(node: Node) -> Path:
    return node.path.parent / ".rip" / (node.path.name + ".invalidation")


def _invalidation_token(node: Node) -> str | None:
    invalidator = _invalidator(node)
    if invalidator is None:
        return None
    token = invalidator(node)
    if not isinstance(token, str):
        raise TypeError(
            f"invalidator for {node.node_type.__name__} must return str, "
            f"got {type(token).__name__}"
        )
    return token


def _has_changed_invalidation(node: Node) -> bool:
    token = _invalidation_token(node)
    if token is None:
        return False
    token_path = _invalidation_file(node)
    return not token_path.exists() or token_path.read_text() != token


def write_dependencies(node: Node) -> None:
    """Write dependencies.toml, content hashes, and invalidation tokens.

    Call after the job succeeds. Co-outputs share a directory, so calling this for
    any one of them writes metadata for all siblings via node.output_nodes.
    """
    data = {
        "rule": node.rule.__name__ if node.rule else "unknown",
        "hash": node.path.parent.name,
        "config": _accumulated_config(node),
    }
    if node.rule_call is not None:
        data["fingerprint"] = {
            "format": "v2",
            "provider": node.rule_call.fingerprint_provider,
            "digest": node.fingerprint,
        }
    if node.rule_call.shellpath is not None:
        data["execution"] = {"shellpath": node.rule_call.shellpath}
    if node.command is not None:
        command_data = {
            "kind": "python" if callable(node.command) else "shell",
            "realized": resolve_command(node),
        }
        if callable(node.command):
            _tree, source_path = command_ast(node.command)
            command_data["source"] = os.path.relpath(source_path, node.path.parents[2])
            command_data["python"] = python_identity()
        else:
            command_data["template"] = node.command
        data["command"] = command_data
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "dependencies.toml").write_text(tomlkit.dumps(data))
    for onode in node.output_nodes.values():
        if onode.path is not None and onode.path.exists():
            (rip / (onode.path.name + ".hash")).write_text(_content_hash(onode.path))
            token = _invalidation_token(onode)
            if token is not None:
                _invalidation_file(onode).write_text(token)


def _output_mtime(path: Path) -> float:
    """Mtime of a node output. For directories, returns the max mtime of all files inside."""
    if path.is_dir():
        mtimes = [f.stat().st_mtime for f in path.rglob("*") if f.is_file()]
        return max(mtimes) if mtimes else path.stat().st_mtime
    return path.stat().st_mtime


def classify_nodes(nodes: list[Node], required_nodes: list[Node]) -> None:
    """Set each eagerly addressed node's state from cache and dependency metadata.

    Nodes in the required subgraph (required_nodes + all ancestors) get Missing/Stale/UpToDate.
    Nodes outside the subgraph with existing output get Orphan.
    Nodes outside the subgraph with no output get state=None (excluded from execution).
    """
    # BFS to collect all nodes in the required subgraph
    required: dict[Path, Node] = {}
    frontier = list(required_nodes)
    while frontier:
        n = frontier.pop()
        if n.relative_path in required:
            continue
        required[n.relative_path] = n
        frontier.extend(p for p in n.parents if p.relative_path not in required)

    # ORPHAN pass: output exists from a prior run but isn't needed now; skipped
    # by the executor unless autoclean=True, in which case it gets deleted
    for node in nodes:
        if node.relative_path not in required:
            node.state = (
                NodeState.ORPHAN
                if (node.path is not None and node.path.exists())
                else None
            )

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
                if (
                    hash_file.exists()
                    and _content_hash(p.path) == hash_file.read_text().strip()
                ):
                    continue  # parent re-ran but content unchanged
                stale = True
                break
        if _has_changed_invalidation(node):
            stale = True
        node.state = NodeState.STALE if stale else NodeState.UP_TO_DATE


class _ConstraintFormatter:
    def __init__(self, constraints: dict[str, Any]):
        self.constraints = constraints

    def __format__(self, name: str) -> str:
        if not name:
            raise ValueError(
                "constraint placeholder requires a name, e.g. {constraint:threads}"
            )
        try:
            return str(self.constraints[name])
        except KeyError as exc:
            raise KeyError(f"unknown constraint placeholder: {name}") from exc


def _quote_command_substitution(value: Any) -> Any:
    if isinstance(value, _ConstraintFormatter):
        return value
    return shlex.quote(str(value))


def resolve_command(node: Node) -> str | None:
    """Format node.command with input/output paths and config values.

    Substitutions: {input_name} -> parent.path, {output_name} -> node.path, {config_key} -> value.
    """
    if node.command is None:
        return None
    call = node.rule_call
    if call._command_realized:
        return call._realized_command
    if callable(node.command):
        if call is None:
            raise RuntimeError("callable command is missing its RuleCall")
        result = node.command(call.command_args())
        if not isinstance(result, str) or not result.strip():
            raise TypeError(
                f"Python command callback {node.command.__qualname__!r} must return "
                f"a non-empty shell string, got {result!r}"
            )
        call._realized_command = result
        call._command_realized = True
        return result
    pos_input_names = [
        n for n, t in node.rule.inputs.specs.items() if _is_node_input_contract(t)
    ]
    subs: dict[str, Any] = {}
    for iname, parent in zip(pos_input_names, node.parents):
        subs[iname] = parent.path
    subs.update(node.config)
    command_constraints = {
        "threads": node.rule.constraints.get("threads", node.rule.resources["threads"])
    }
    command_constraints.update(node.rule.constraints)
    for name, value in command_constraints.items():
        subs.setdefault(name, value)
    subs["constraint"] = _ConstraintFormatter(command_constraints)
    for oname, onode in node.output_nodes.items():
        subs[oname] = onode.path
    subs["workdir"] = node.path.parent
    quoted = {k: _quote_command_substitution(v) for k, v in subs.items()}
    result = node.command.format(**quoted)
    if call is not None:
        call._realized_command = result
        call._command_realized = True
    return result
