from __future__ import annotations

import hashlib
import os
import shlex
from pathlib import Path
from typing import Any

import tomlkit

from necroflow.ascii_render import _node_label, render_ascii
from necroflow.nodes import (
    Node,
    NodeType,
    NodeTypeMeta,
)
from necroflow.fingerprints import IDENTITY_FORMAT, command_ast, python_identity
from necroflow.rule_call import RuleCall
from necroflow.rules import parse_resource

_HASH_CHUNK_SIZE = 1024 * 1024


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
            # not len(part): fs limits NAME_MAX in bytes, not Unicode chars.
            if length > name_max:
                raise ValueError(
                    f"path component too long ({length} > NAME_MAX {name_max}): {part!r}"
                )
    if path_max is not None:
        length = len(os.fsencode(os.fspath(path)))
        if length > path_max:
            raise ValueError(f"path too long ({length} > PATH_MAX {path_max}): {path}")


def _update_hash_from_file(digest, path: Path) -> None:
    with path.open("rb") as file:
        while chunk := file.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)


def _content_hash(path: Path) -> str:
    """SHA-256 of a file's bytes, or of all non-.rip files in a directory."""
    h = hashlib.sha256()
    if path.is_file():
        _update_hash_from_file(h, path)
    else:
        for f in sorted(path.rglob("*")):
            if f.is_file() and ".rip" not in f.parts:
                h.update(str(f.relative_to(path)).encode())
                _update_hash_from_file(h, f)
    return h.hexdigest()


def _accumulated_config(
    call: RuleCall, _visited: dict[Path, dict] | None = None
) -> dict:
    """Merge call config over every ancestor config, nearest wins."""
    if _visited is None:
        _visited = {}
    cached = _visited.get(call.relative_path)
    if cached is not None:
        return cached
    config = {}
    for parent in call.parent_calls:
        config.update(_accumulated_config(parent, _visited))
    config.update(call.config)
    _visited[call.relative_path] = config
    return config


def _parent_metadata(parent: Node, hash_cache: dict[Path, str]) -> dict:
    data = {
        "node_key": parent.relative_path.as_posix(),
        "mutable": parent.rule_call.mutable,
    }
    if not parent.rule_call.mutable:
        data["consumed_sha256"] = current_output_hash(parent, hash_cache)
    return data


def write_dependencies(
    call: RuleCall, hash_cache: dict[Path, str] | None = None
) -> None:
    """Persist call lineage, consumed parent hashes, output hashes, invalidators."""
    if hash_cache is None:
        hash_cache = {}
    data = {
        "rule": call.rule.__name__,
        "mutable": call.mutable,
        "config": _accumulated_config(call),
        "outputs": [
            {
                "name": output.output_name,
                "filename": output.path.name,
                "type": (
                    f"{output.node_type.__module__}.{output.node_type.__qualname__}"
                ),
            }
            for output in call.outputs
        ],
        "parents": [_parent_metadata(parent, hash_cache) for parent in call.parents],
        "identity": {
            "format": IDENTITY_FORMAT,
            "rule_hash": call.rule_hash,
            "provenance_hash": call.provenance_hash,
        },
        "recipe": call.rule_identity,
    }
    if call.shellpath is not None:
        data["execution"] = {"shellpath": call.shellpath}
    if call.command is not None:
        command_data = {
            "kind": "python" if callable(call.command) else "shell",
            "realized": resolve_command(call),
        }
        if callable(call.command):
            _tree, source_path = command_ast(call.command)
            command_data["source"] = os.path.relpath(source_path, call.dag.nodes_dir)
            command_data["python"] = python_identity()
        else:
            command_data["template"] = call.command
        data["command"] = command_data
    rip = call.workdir / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "dependencies.toml").write_text(tomlkit.dumps(data))
    for output in call.outputs:
        if output.path.exists():
            digest = _content_hash(output.path)
            (rip / (output.path.name + ".hash")).write_text(digest)
            hash_cache[output.relative_path] = digest
            invalidator = output.node_type.invalidator
            if invalidator is not None:
                token = invalidator(output)
                if not isinstance(token, str):
                    raise TypeError(
                        f"invalidator for {output.node_type.__name__} must return str, "
                        f"got {type(token).__name__}"
                    )
                (rip / (output.path.name + ".invalidation")).write_text(token)


def _output_mtime(path: Path) -> int:
    """Newest output mtime; directory entries detect rename and deletion."""
    if path.is_dir():
        entries = [path]
        entries.extend(
            entry
            for entry in path.rglob("*")
            if ".rip" not in entry.relative_to(path).parts
        )
        return max(entry.stat().st_mtime_ns for entry in entries)
    return path.stat().st_mtime_ns


def current_output_hash(node: Node, memo: dict[Path, str]) -> str:
    """Return current bytes hash, trusting stored hash while mtime proves safety."""
    cached = memo.get(node.relative_path)
    if cached is not None:
        return cached
    hash_file = node.rule_call.workdir / ".rip" / (node.path.name + ".hash")
    digest = None
    if hash_file.exists() and _output_mtime(node.path) <= hash_file.stat().st_mtime_ns:
        stored = hash_file.read_text().strip()
        if len(stored) == 64 and all(char in "0123456789abcdef" for char in stored):
            digest = stored
    if digest is None:
        digest = _content_hash(node.path)
    memo[node.relative_path] = digest
    return digest


class _ShellArguments:
    """Render a tuple of paths as independently quoted shell arguments."""

    def __init__(self, values: tuple[Path, ...]):
        self.values = values

    def __str__(self) -> str:
        return " ".join(shlex.quote(str(value)) for value in self.values)


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
    if isinstance(value, _ShellArguments):
        return str(value)
    return shlex.quote(str(value))


def resolve_command(call: RuleCall) -> str | None:
    """Resolve one RuleCall command from input, output, config, resource paths."""
    if call.command is None:
        return None
    if call._realized_command is not None:
        return call._realized_command
    if callable(call.command):
        result = call.command(call.command_args())
        if not isinstance(result, str) or not result.strip():
            raise TypeError(
                f"Python command callback {call.command.__qualname__!r} must return "
                f"a non-empty shell string, got {result!r}"
            )
        call._realized_command = result
        return result
    command_inputs = call.command_args().inputs
    subs: dict[str, Any] = {
        name: _ShellArguments(value) if isinstance(value, tuple) else value
        for name, value in command_inputs.items()
    }
    subs.update(call.config)
    command_constraints = {
        "threads": call.rule.constraints.get("threads", call.rule.resources["threads"])
    }
    command_constraints.update(call.rule.constraints)
    for name, value in command_constraints.items():
        subs.setdefault(name, value)
    subs["constraint"] = _ConstraintFormatter(command_constraints)
    for output_name, output in call.output_nodes.items():
        subs[output_name] = output.path
    subs["workdir"] = call.workdir
    quoted = {key: _quote_command_substitution(value) for key, value in subs.items()}
    result = call.command.format(**quoted)
    call._realized_command = result
    return result


class DAG:
    """Shared registry and executor for canonical content-addressed rule calls."""

    def __init__(self, outdir):
        self._calls: dict[Path, RuleCall] = {}
        self._nodes: dict[Path, Node] = {}
        self._required: set[Path] = set()
        self._labels_by_path: dict[Path, set[str]] = {}
        self.outdir = Path(outdir).expanduser().resolve()
        self.last_execution_report = None

    @property
    def nodes_dir(self) -> Path:
        return self.outdir

    @property
    def calls(self) -> dict[Path, RuleCall]:
        return self._calls

    def intern(self, call: RuleCall) -> RuleCall:
        """Return the canonical RuleCall for this relative call path."""
        existing = self._calls.get(call.relative_path)
        if existing is not None:
            expected = {
                name: (node.node_type, node.relative_path)
                for name, node in existing.output_nodes.items()
            }
            candidate = {
                name: (node.node_type, node.relative_path)
                for name, node in call.output_nodes.items()
            }
            if candidate != expected:
                raise ValueError(
                    f"fingerprint collision for {call.relative_path}: "
                    "declared outputs do not match the canonical RuleCall"
                )
            return existing
        self._calls[call.relative_path] = call
        for node in call.output_nodes.values():
            if node.relative_path in self._nodes:
                raise ValueError(f"duplicate output path: {node.relative_path}")
            self._nodes[node.relative_path] = node
        return call

    def require(self, nodes) -> None:
        """Add canonical Nodes to the set requested for execution."""
        for node in nodes:
            if not isinstance(node, Node):
                raise TypeError(
                    f"DAG requirements must be Nodes, got {type(node).__name__}"
                )
            if node.rule_call.dag is not self:
                raise ValueError("required Node belongs to a different DAG")
            self._required.add(node.relative_path)

    def _record_binding(self, node: Node, label: str) -> None:
        """Record one qualified Pipeline label as DAG-wide diagnostic metadata."""
        self._labels_by_path.setdefault(node.relative_path, set()).add(label)

    def labels_for(self, node: Node) -> tuple[str, ...]:
        """Return distinct labels recorded across all Pipelines for a Node.

        These labels are DAG-wide diagnostic metadata, not authoritative
        names for any particular Pipeline. They are sorted for deterministic
        display and may contain several aliases when Pipelines bind the same
        canonical Node under different names.
        """
        return tuple(sorted(self._labels_by_path.get(node.relative_path, set())))

    def label_for(self, node: Node) -> str | None:
        """Return the Node's sole DAG-wide label, or ``None`` if ambiguous.

        This is a best-effort display helper for execution events and
        diagnostics that lack Pipeline context. It deliberately refuses to
        choose arbitrarily when :meth:`labels_for` finds zero or several
        distinct labels.
        """
        labels = self.labels_for(node)
        return labels[0] if len(labels) == 1 else None

    @property
    def nodes(self) -> list:
        return list(self._nodes.values())

    @property
    def required_nodes(self) -> list:
        return [n for path, n in self._nodes.items() if path in self._required]

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        required_paths = {n.relative_path for n in self.required_nodes}

        def label(node: Node) -> str:
            return _node_label(node) + (
                " ★" if node.relative_path in required_paths else ""
            )

        header = f"DAG  {len(self._nodes)} nodes  ({len(self._required)} required)"
        return render_ascii(self.nodes, header, label=label)

    def save(self, path) -> None:
        """Write the ASCII DAG render to a file."""
        Path(path).write_text(str(self) + "\n", encoding="utf-8")

    def run(self, **kwargs):
        from necroflow.executor import run

        self.last_execution_report = run(self, **kwargs)
        return self.last_execution_report
