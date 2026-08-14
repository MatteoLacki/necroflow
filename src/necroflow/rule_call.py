from __future__ import annotations

import os
import shlex
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, TYPE_CHECKING

import tomlkit

from necroflow.contexts import CommandArgs, NamedValues
from necroflow.fingerprints import (
    IDENTITY_FORMAT,
    command_ast,
    compute_identity,
    python_identity,
)
from necroflow.fs import _content_hash

if TYPE_CHECKING:
    from necroflow.nodes import Node


class RuleCallState(Enum):
    MISSING = "missing"
    STALE = "stale"
    UP_TO_DATE = "up_to_date"
    ORPHAN = "orphan"
    READY = "ready"
    RUNNING = "running"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


def _safe_path_component(value: str, *, kind: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or len(path.parts) != 1 or value in {".", ".."}:
        raise ValueError(f"{kind} must be one relative path component: {value!r}")
    return value


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


@dataclass
class RuleCall:
    """One concrete invocation shared by all of its output Nodes."""

    dag: Any
    rule: Any
    inputs: NamedValues[Node | tuple[Node, ...]]
    config: dict[str, Any]
    command: str | Callable | None
    shellpath: str | None = None
    output_nodes: dict[str, Node] = field(default_factory=dict)
    parents: list[Node] = field(init=False)
    parent_calls: list[RuleCall] = field(init=False)
    state: RuleCallState | None = None
    rule_identity: dict[str, Any] = field(init=False)
    rule_hash: str = field(init=False)
    provenance_hash: str = field(init=False)
    relative_path: Path = field(init=False)
    _realized_command: str | None = None

    def __post_init__(self) -> None:
        # Cached once: read in hot graph-traversal loops via
        # Node.parents, so this must not become a rebuild-per-access property.
        self.parents = [
            item
            for value in self.inputs.values()
            for item in (value if isinstance(value, tuple) else (value,))
        ]
        seen: set[Path] = set()
        self.parent_calls = []
        for parent in self.parents:
            parent_call = parent.rule_call
            if parent_call.relative_path in seen:
                continue
            seen.add(parent_call.relative_path)
            self.parent_calls.append(parent_call)
        self.rule_identity, self.rule_hash, self.provenance_hash = compute_identity(
            self
        )
        rule_component = _safe_path_component(self.rule.__name__, kind="rule name")
        self.relative_path = Path(rule_component) / self.provenance_hash

    def _constraints(self) -> dict[str, Any]:
        values = {
            "threads": self.rule.constraints.get(
                "threads", self.rule.resources["threads"]
            )
        }
        values.update(self.rule.constraints)
        return values

    @property
    def workdir(self) -> Path:
        return self.dag.nodes_dir / self.relative_path

    @property
    def mutable(self) -> bool:
        """Return whether consumers ignore this call's content-only edits."""
        return self.rule.mutable

    @property
    def state_file(self) -> Path:
        return self.workdir / ".rip" / "state"

    @property
    def is_compromised(self) -> bool:
        return (
            self.state_file.exists()
            and self.state_file.read_text().strip() != "up_to_date"
        )

    @property
    def resources(self) -> dict[str, int]:
        return self.rule.resources

    def mark_running(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text("running")

    def mark_done(self, state: str) -> None:
        self.state_file.write_text(state)

    def _accumulated_config(self, visited: dict[Path, dict] | None = None) -> dict:
        """Merge this call config over every ancestor config, nearest wins."""
        if visited is None:
            visited = {}
        cached = visited.get(self.relative_path)
        if cached is not None:
            return cached
        config = {}
        for parent in self.parent_calls:
            config.update(parent._accumulated_config(visited))
        config.update(self.config)
        visited[self.relative_path] = config
        return config

    def write_dependencies(self, hash_cache: dict[Path, str] | None = None) -> None:
        """Persist lineage, consumed hashes, output hashes, and invalidators."""
        from necroflow.planning import current_output_hash

        if hash_cache is None:
            hash_cache = {}
        parents = []
        for parent in self.parents:
            metadata = {
                "node_key": parent.relative_path.as_posix(),
                "mutable": parent.rule_call.mutable,
            }
            if not parent.rule_call.mutable:
                metadata["consumed_sha256"] = current_output_hash(parent, hash_cache)
            parents.append(metadata)
        data = {
            "rule": self.rule.__name__,
            "mutable": self.mutable,
            "config": self._accumulated_config(),
            "outputs": [
                {
                    "name": output.output_name,
                    "filename": output.path.name,
                    "type": (
                        f"{output.node_type.__module__}.{output.node_type.__qualname__}"
                    ),
                }
                for output in self.outputs
            ],
            "parents": parents,
            "identity": {
                "format": IDENTITY_FORMAT,
                "rule_hash": self.rule_hash,
                "provenance_hash": self.provenance_hash,
            },
            "recipe": self.rule_identity,
        }
        if self.shellpath is not None:
            data["execution"] = {"shellpath": self.shellpath}
        if self.command is not None:
            command_data = {
                "kind": "python" if callable(self.command) else "shell",
                "realized": self.resolve(),
            }
            if callable(self.command):
                _tree, source_path = command_ast(self.command)
                command_data["source"] = os.path.relpath(
                    source_path, self.dag.nodes_dir
                )
                command_data["python"] = python_identity()
            else:
                command_data["template"] = self.command
            data["command"] = command_data
        rip = self.workdir / ".rip"
        rip.mkdir(parents=True, exist_ok=True)
        (rip / "dependencies.toml").write_text(tomlkit.dumps(data))
        for output in self.outputs:
            if not output.path.exists():
                continue
            digest = _content_hash(output.path)
            (rip / (output.path.name + ".hash")).write_text(digest)
            hash_cache[output.relative_path] = digest
            invalidator = output.node_type.invalidator
            if invalidator is None:
                continue
            token = invalidator(output)
            if not isinstance(token, str):
                raise TypeError(
                    f"invalidator for {output.node_type.__name__} must return str, "
                    f"got {type(token).__name__}"
                )
            (rip / (output.path.name + ".invalidation")).write_text(token)

    @property
    def outputs(self) -> tuple[Node, ...]:
        return tuple(self.output_nodes.values())

    def command_args(self) -> CommandArgs:
        named_inputs = {
            name: (
                tuple(parent.path for parent in value)
                if isinstance(value, tuple)
                else value.path
            )
            for name, value in self.inputs.items()
        }
        outputs = {name: node.path for name, node in self.output_nodes.items()}
        return CommandArgs(
            inputs=NamedValues(named_inputs),
            config=NamedValues(self.config),
            outputs=NamedValues(outputs),
            constraints=NamedValues(self._constraints()),
            workdir=self.workdir,
        )

    def resolve(self) -> str | None:
        """Resolve this call command from input, output, config, and resources."""
        if self.command is None:
            return None
        if self._realized_command is not None:
            return self._realized_command
        if callable(self.command):
            result = self.command(self.command_args())
            if not isinstance(result, str) or not result.strip():
                raise TypeError(
                    f"Python command callback {self.command.__qualname__!r} must return "
                    f"a non-empty shell string, got {result!r}"
                )
            self._realized_command = result
            return result
        command_inputs = self.command_args().inputs
        substitutions: dict[str, Any] = {
            name: _ShellArguments(value) if isinstance(value, tuple) else value
            for name, value in command_inputs.items()
        }
        substitutions.update(self.config)
        command_constraints = {
            "threads": self.rule.constraints.get(
                "threads", self.rule.resources["threads"]
            )
        }
        command_constraints.update(self.rule.constraints)
        for name, value in command_constraints.items():
            substitutions.setdefault(name, value)
        substitutions["constraint"] = _ConstraintFormatter(command_constraints)
        for output_name, output in self.output_nodes.items():
            substitutions[output_name] = output.path
        substitutions["workdir"] = self.workdir
        quoted = {
            key: _quote_command_substitution(value)
            for key, value in substitutions.items()
        }
        self._realized_command = self.command.format(**quoted)
        return self._realized_command
