from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, TYPE_CHECKING

import tomlkit

from necroflow.containers import (
    CONTAINER_POLICY,
    Docker,
    docker_argv,
    split_prefix,
)
from necroflow.contexts import CommandArgs, NamedValues
from necroflow.hashers import Hasher, load_hasher, tagged_hash
from necroflow.fingerprints import (
    IDENTITY_FORMAT,
    PROVENANCE_HASH_DOMAIN,
    _rule_identity,
    canonical_bytes,
    command_ast,
    hash_rule_identity,
    python_identity,
)

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
    input_values: NamedValues[Any]
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
    _container_input: str | None = None
    _thread_cap: int | None = None

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
        self.rule_identity, self.rule_hash, self.provenance_hash = (
            self.compute_identity()
        )
        rule_component = _safe_path_component(self.rule.__name__, kind="rule name")
        self.relative_path = Path(rule_component) / self.provenance_hash

    def compute_identity(self) -> tuple[dict[str, Any], str, str]:
        """Return this call's canonical recipe and two identity hashes."""
        rule_identity = _rule_identity(
            rule_name=self.rule.__name__,
            command=self.command,
            recipe_identity=self.rule.recipe_identity,
            input_types=self.rule.inputs.specs,
            output_types=self.rule.outputs.specs,
        )
        rule_hash = hash_rule_identity(rule_identity)
        return rule_identity, rule_hash, self.compute_provenance_hash(rule_hash)

    def _parent_identity(self) -> list[dict[str, Any]]:
        """Return canonical parent lineage grouped by declared input name."""
        parents = []
        for name, parent in self.inputs.items():
            if isinstance(parent, tuple):
                parents.append(
                    {
                        "name": name,
                        "group": [
                            {
                                "provenance_hash": item.provenance_hash,
                                "output": item.output_name or "",
                            }
                            for item in parent
                        ],
                    }
                )
            else:
                parents.append(
                    {
                        "name": name,
                        "provenance_hash": parent.provenance_hash,
                        "output": parent.output_name or "",
                    }
                )
        return parents

    def _positional_input_order(self) -> list[str]:
        """Return callback-visible positional input names in declaration order."""
        return [
            name
            for name in self.rule.inputs.specs
            if name in self.inputs or name in self.input_values
        ]

    def compute_provenance_hash(self, rule_hash: str) -> str:
        """Hash config, execution context, parent lineage, and mixed values."""
        execution_context: dict[str, Any] = {}
        if self.shellpath is not None:
            execution_context["shellpath"] = self.shellpath
        if self.rule.container_capable:
            execution_context["container_policy"] = CONTAINER_POLICY
        identity = {
            "domain": PROVENANCE_HASH_DOMAIN,
            "rule_hash": rule_hash,
            "config": self.config,
            "execution_context": execution_context,
            "parents": self._parent_identity(),
        }
        if self.input_values:
            identity["input_values"] = dict(self.input_values)
            identity["input_order"] = self._positional_input_order()
        return hashlib.sha256(canonical_bytes(identity, path="provenance")).hexdigest()

    def _constraints(self) -> dict[str, Any]:
        values = dict(self.rule.constraints)
        if values.get("threads") == "all":
            values["threads"] = self.resources["threads"]
        else:
            values.setdefault("threads", self.resources["threads"])
        return values

    @property
    def workdir(self) -> Path:
        return self.dag.nodes_dir / self.relative_path

    @property
    def state_file(self) -> Path:
        return self.workdir / ".rip" / "state"

    def log_path(self) -> Path:
        """Return this call's captured job-output path."""
        return self.workdir / ".rip" / "job.log"

    def run(self, log_path: Path) -> None:
        """Execute this call's materializer or resolved shell command."""
        self.workdir.mkdir(parents=True, exist_ok=True)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w") as log:
            materializer = self.rule.materializer
            if materializer is not None:
                materializer(self, log)
                return
            command = self.resolve()
            if command is None:
                raise RuntimeError(
                    f"rule {self.rule.__name__!r} has neither a command nor a "
                    "materializer"
                )
            container = self.container
            if container is not None:
                argv = docker_argv(self, container, command)
                subprocess.run(argv, check=True, stdout=log, stderr=log)
                return
            options = {
                "shell": True,
                "check": True,
                "stdout": log,
                "stderr": log,
            }
            if self.shellpath is not None:
                options["executable"] = self.shellpath
            subprocess.run(command, **options)

    @property
    def container(self) -> Docker | None:
        """Return the Docker settings selected by the resolved command, if any."""
        self.resolve()
        if self._container_input is None:
            return None
        return self.config[self._container_input]

    @property
    def is_compromised(self) -> bool:
        return (
            self.state_file.exists()
            and self.state_file.read_text().strip() != "up_to_date"
        )

    @property
    def resources(self) -> dict[str, int]:
        resources = self.rule.resources
        if (
            self.rule.constraints.get("threads") == "all"
            and self._thread_cap is not None
        ):
            resources["threads"] = self._thread_cap
        return resources

    def bind_thread_cap(self, cap: int) -> None:
        """Resolve an all-threads declaration for this execution."""
        if self.rule.constraints.get("threads") == "all" and self._thread_cap != cap:
            self._thread_cap = cap
            self._realized_command = None

    def mark_running(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text("running")

    def mark_done(self, state: str) -> None:
        self.state_file.write_text(state)

    def output_size_bytes(self) -> int:
        """Return total non-metadata file bytes in this call's workdir."""
        if not self.workdir.exists():
            return 0
        return sum(
            path.stat().st_size
            for path in self.workdir.rglob("*")
            if path.is_file() and ".rip" not in path.parts
        )

    def remove_workdir(self) -> bool:
        """Delete this call's workdir unless it is already absent."""
        if not self.workdir.exists():
            return False
        shutil.rmtree(self.workdir)
        return True

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

    def write_dependencies(
        self,
        hash_cache: dict[Path, str] | None = None,
        hasher: Hasher | None = None,
    ) -> None:
        """Persist lineage, consumed hashes, output hashes, and invalidators.

        Hashing uses this call's own `threads` resource: the executor still
        holds those threads for the call while it is being completed.
        """
        from necroflow.planning import current_output_hash

        if hash_cache is None:
            hash_cache = {}
        hasher = load_hasher(hasher)
        threads = self.resources["threads"]
        parents = []
        for parent in self.parents:
            parents.append(
                {
                    "node_key": parent.relative_path.as_posix(),
                    "consumed_hash": current_output_hash(
                        parent, hash_cache, hasher, threads
                    ),
                }
            )
        data = {
            "rule": self.rule.__name__,
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
        if self.input_values:
            data["input_order"] = self._positional_input_order()
            data["input_values"] = [
                {
                    "name": name,
                    "type": f"{type(value).__module__}.{type(value).__qualname__}",
                    "value": repr(value),
                }
                for name, value in self.input_values.items()
            ]
        if self.shellpath is not None:
            data["execution"] = {"shellpath": self.shellpath}
        if self.container is not None:
            data.setdefault("execution", {})["container_input"] = self._container_input
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
            digest = tagged_hash(hasher, output.path, threads)
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
        named_inputs = {}
        for name in self.rule.inputs.specs:
            if name in self.inputs:
                value = self.inputs[name]
                named_inputs[name] = (
                    tuple(parent.path for parent in value)
                    if isinstance(value, tuple)
                    else value.path
                )
            elif name in self.input_values:
                named_inputs[name] = self.input_values[name]
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
            self._container_input, body = split_prefix(result, self.rule.docker_inputs)
            if not body.strip():
                raise TypeError(
                    f"Python command callback {self.command.__qualname__!r} returned "
                    "an empty container command body"
                )
            self._realized_command = body
            return body
        command_inputs = self.command_args().inputs
        substitutions: dict[str, Any] = {
            name: _ShellArguments(value) if isinstance(value, tuple) else value
            for name, value in command_inputs.items()
        }
        substitutions.update(self.config)
        command_constraints = self._constraints()
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
        self._container_input, template = split_prefix(
            self.command, self.rule.docker_inputs
        )
        self._realized_command = template.format(**quoted)
        return self._realized_command
