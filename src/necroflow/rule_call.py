from __future__ import annotations

import hashlib
import inspect
import os
import shlex
import shutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

import tomlkit

from necroflow.containers import CONTAINER_POLICY, Docker, split_prefix
from necroflow.contexts import CommandArgs, NamedValues
from necroflow.hashers import OutputHashes, load_hasher
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


# Values `.rip/state` may hold. Only "up_to_date" is trusted; any other value,
# including a "running" left by a crash, compromises the call.
STORED_STATES = ("running", "up_to_date", "failed", "interrupted")


def ancestor_call_keys(calls: Iterable[RuleCall]) -> set[Path]:
    """Return the keys of `calls` and of every call they transitively consume."""
    keys: set[Path] = set()
    frontier = list(calls)
    while frontier:
        call = frontier.pop()
        if call.relative_path not in keys:
            keys.add(call.relative_path)
            frontier.extend(call.parent_calls)
    return keys


def provenance_config(record: Mapping) -> dict:
    """Decode null placeholders in a parsed dependencies document."""
    config = record.get("config", {})
    # TOML containers cannot hold None; unwrap before restoring null values.
    if hasattr(config, "unwrap"):
        config = config.unwrap()
    for path in record.get("config_null_paths", []):
        target = config
        for component in path[:-1]:
            target = target[component]
        target[path[-1]] = None
    return config


def _encode_config(config: Mapping) -> tuple[dict, list[list[str | int]]]:
    """Represent nulls separately, retaining unambiguous mapping/array paths."""
    null_paths = []

    def encode(value, path):
        if value is None:
            null_paths.append(path)
            return ""
        if isinstance(value, Mapping):
            return {key: encode(item, [*path, key]) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [encode(item, [*path, index]) for index, item in enumerate(value)]
        return value

    return encode(config, []), null_paths


def _safe_path_component(value: str, *, kind: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or len(path.parts) != 1 or value in {".", ".."}:
        raise ValueError(f"{kind} must be one relative path component: {value!r}")
    return value


class _ShellArguments:
    """Render a tuple of paths as independently quoted shell arguments."""

    def __init__(self, values: tuple[Path, ...]):
        self.values = values

    @staticmethod
    def parse_spec(spec: str) -> tuple[str, str]:
        if not spec:
            return "args", ""
        if ",flag=" in spec or ",sep=" in spec:
            raise ValueError("flag= and sep= cannot be combined")
        if spec.startswith("flag="):
            flag = spec[5:]
            if not flag.startswith("-") or any(c.isspace() for c in flag):
                raise ValueError("flag= requires one nonempty flag, e.g. flag=-f")
            return "flag", flag
        if spec.startswith("sep=") and spec[4:]:
            return "sep", spec[4:]
        raise ValueError(f"invalid tuple format {spec!r}; use flag=-f or sep=,")

    def __format__(self, spec: str) -> str:
        mode, value = self.parse_spec(spec)
        if mode == "sep":
            return shlex.quote(value.join(str(item) for item in self.values))
        paths = [shlex.quote(str(item)) for item in self.values]
        if mode == "flag":
            return " ".join(f"{shlex.quote(value)} {path}" for path in paths)
        return " ".join(paths)

    def __str__(self) -> str:
        return format(self, "")


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
        return value
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
    resource_requests: dict[str, Any] | None = None
    parents: list[Node] = field(init=False)
    parent_calls: list[RuleCall] = field(init=False)
    rule_identity: dict[str, Any] = field(init=False)
    rule_hash: str = field(init=False)
    provenance_hash: str = field(init=False)
    relative_path: Path = field(init=False)
    # (selected Docker input or None, command body), cached after first use.
    _realized: tuple[str | None, str] | None = None
    _thread_cap: int | None = None

    def __post_init__(self) -> None:
        if self.resource_requests is None:
            self.resource_requests = dict(self.rule.constraints)
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
        from necroflow.construction import Executable

        rule_identity = _rule_identity(
            rule_name=self.rule.__name__,
            command=(
                self.rule.command
                if isinstance(self.rule.command, Executable)
                else self.command
            ),
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
        """Command-facing constraints: raw declared values, threads resolved."""
        values = dict(self.resource_requests)
        if values.get("threads", "all") == "all":
            values["threads"] = self.resources["threads"]
        return values

    @property
    def workdir(self) -> Path:
        return self.dag.nodes_dir / self.relative_path

    @property
    def metadata_dir(self) -> Path:
        return self.workdir / ".rip"

    @property
    def state_file(self) -> Path:
        return self.metadata_dir / "state"

    def log_path(self) -> Path:
        """Return this call's captured job-output path."""
        return self.metadata_dir / "job.log"

    @property
    def container_input(self) -> str | None:
        """Return the Docker input named by the command's `{name}:` prefix, if any."""
        if self.command is None:
            return None
        return self._realize()[0]

    @property
    def container(self) -> Docker | None:
        """Return the Docker settings selected by the resolved command, if any."""
        name = self.container_input
        return None if name is None else self.config[name]

    @property
    def is_compromised(self) -> bool:
        return (
            self.state_file.exists()
            and self.state_file.read_text().strip() != "up_to_date"
        )

    @property
    def resources(self) -> dict[str, int | float]:
        """Numeric scheduler resources; `threads="all"` is the bound thread cap."""
        from necroflow.rules import default_thread_cap, parse_resource

        cap = default_thread_cap() if self._thread_cap is None else self._thread_cap
        result = {
            name: cap if name == "threads" and value == "all" else parse_resource(value)
            for name, value in self.resource_requests.items()
        }
        result.setdefault("threads", 1)
        return result

    def bind_thread_cap(self, cap: int) -> None:
        """Resolve an all-threads declaration for this execution."""
        if self.resource_requests.get("threads") == "all" and self._thread_cap != cap:
            self._thread_cap = cap
            self._realized = None

    def write_state(self, state: str) -> None:
        """Persist this call's run state in `.rip/state`."""
        if state not in STORED_STATES:
            raise ValueError(f"unknown RuleCall state: {state!r}")
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
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

    def clear_outputs(self) -> None:
        """Delete any declared outputs left from a previous attempt or run.

        Identity (and so the workdir path) does not change when only a
        parent's bytes change, so a replay reuses its existing directory.
        Declared outputs are cleared before every command attempt so a
        replay never sees a prior output file or directory; other files
        written under `{workdir}` are left alone.
        """
        for output in self.outputs:
            path = output.path
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

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

    def validate_dependencies(self) -> None:
        """Reject unserializable provenance before executing or clearing outputs."""
        try:
            tomlkit.dumps(self._dependency_record([""] * len(self.parents)))
        except Exception as exc:
            raise ValueError(
                f"Cannot serialize provenance for {self.relative_path}: {exc}"
            ) from exc

    def write_dependencies(self, hashes: OutputHashes | None = None) -> None:
        """Persist lineage, consumed parent hashes, output hashes, and invalidators.

        Hashing uses this call's own `threads` resource: the executor still
        holds those threads for the call while it is being completed. A parent
        rehashed here, e.g. after a hasher switch, gets its stored hash refreshed.
        """
        if hashes is None:
            hashes = OutputHashes(load_hasher(None), store=True)
        threads = self.resources["threads"]
        consumed = [hashes.current(parent, threads) for parent in self.parents]
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        record = tomlkit.dumps(self._dependency_record(consumed))
        (self.metadata_dir / "dependencies.toml").write_text(record)
        for output in self.outputs:
            if not output.path.exists():
                continue
            hashes.record(output, threads)
            token = output.invalidation_token()
            if token is not None:
                output.invalidation_file.write_text(token)

    def read_dependencies(self) -> list[dict] | None:
        """Return recorded parent entries, or None when missing or malformed."""
        path = self.metadata_dir / "dependencies.toml"
        if not path.exists():
            return None
        try:
            parents = tomlkit.parse(path.read_text()).get("parents")
        except Exception:
            return None
        if not isinstance(parents, list) or not all(
            isinstance(parent, dict) for parent in parents
        ):
            return None
        return parents

    def _dependency_record(self, consumed: list[str | None]) -> dict[str, Any]:
        """Return the `dependencies.toml` document for these consumed hashes."""
        config, null_paths = _encode_config(self._accumulated_config())
        data = {
            "rule": self.rule.__name__,
            "resources": self.resources,
            "config": config,
            "outputs": [
                {
                    "name": output.output_name,
                    "filename": output.path.name,
                    "type": (f"{type(output).__module__}.{type(output).__qualname__}"),
                }
                for output in self.outputs
            ],
            "parents": [
                {"node_key": parent.relative_path.as_posix(), "consumed_hash": digest}
                for parent, digest in zip(self.parents, consumed)
            ],
            "identity": {
                "format": IDENTITY_FORMAT,
                "rule_hash": self.rule_hash,
                "provenance_hash": self.provenance_hash,
            },
            "recipe": self.rule_identity,
        }
        if null_paths:
            data["config_null_paths"] = null_paths
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
        if self.container_input is not None:
            data.setdefault("execution", {})["container_input"] = self.container_input
        if self.command is not None:
            data["command"] = self._command_record()
        return data

    def _command_record(self) -> dict[str, Any]:
        """Return how this call's command was declared and realized."""
        from necroflow.construction import Executable

        if isinstance(self.rule.command, Executable):
            return {
                "kind": "python-construction",
                "source": os.path.relpath(
                    inspect.getsourcefile(self.rule.command.function),
                    self.dag.nodes_dir,
                ),
                "python": python_identity(),
                "template": self.command,
                "realized": self.resolve(),
            }
        record = {
            "kind": "python" if callable(self.command) else "shell",
            "realized": self.resolve(),
        }
        if callable(self.command):
            _tree, source_path = command_ast(self.command)
            record["source"] = os.path.relpath(source_path, self.dag.nodes_dir)
            record["python"] = python_identity()
        else:
            record["template"] = self.command
        return record

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
        return self._realize()[1]

    def _realize(self) -> tuple[str | None, str]:
        """Return (selected Docker input or None, command body), cached.

        A template's `{name}:` Docker prefix is split off before formatting;
        a Python callback's prefix is split off the string it returns.
        """
        if self._realized is None:
            template = (
                self._call_command_callback()
                if callable(self.command)
                else self.command
            )
            container_input, body = split_prefix(template, self.rule.docker_inputs)
            if callable(self.command) and not body.strip():
                raise TypeError(
                    f"Python command callback {self.command.__qualname__!r} returned "
                    "an empty container command body"
                )
            self.rule._validate_command(
                self.rule.__name__,
                self.rule.inputs,
                self.rule.outputs,
                body,
                self.resource_requests,
            )
            self._realized = (
                container_input,
                body.format(**self._template_substitutions()),
            )
        return self._realized

    def _call_command_callback(self) -> str:
        """Invoke a builder and validate its returned, still-unrendered template."""
        result = self.command(self.command_args())
        if not isinstance(result, str) or not result.strip():
            raise TypeError(
                f"Python command callback {self.command.__qualname__!r} must return "
                f"a non-empty shell string, got {result!r}"
            )
        return result

    def _template_substitutions(self) -> dict[str, Any]:
        """Return shell-quoted values for every string-template placeholder."""
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
        return {
            key: _quote_command_substitution(value)
            for key, value in substitutions.items()
        }
