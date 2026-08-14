from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, TYPE_CHECKING

from necroflow.contexts import CommandArgs, NamedValues
from necroflow.fingerprints import compute_identity

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
