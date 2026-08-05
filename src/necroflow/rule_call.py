from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

from necroflow.contexts import CommandArgs, NamedValues
from necroflow.fingerprints import compute_hashes

if TYPE_CHECKING:
    from necroflow.nodes import Node


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
    rule_hash: str = field(init=False)
    provenance_hash: str = field(init=False)
    relative_path: Path = field(init=False)
    _realized_command: str | None = None

    def __post_init__(self) -> None:
        # Cached once: read in hot loops (topo sort, connected components) via
        # Node.parents, so this must not become a rebuild-per-access property.
        self.parents = [
            item
            for value in self.inputs.values()
            for item in (value if isinstance(value, tuple) else (value,))
        ]
        self.rule_hash, self.provenance_hash = compute_hashes(self)
        rule_component = _safe_path_component(self.rule.__name__, kind="rule name")
        self.relative_path = (
            Path(rule_component) / self.rule_hash / self.provenance_hash
        )

    def _constraints(self) -> dict[str, Any]:
        values = {
            "threads": self.rule.constraints.get(
                "threads", self.rule.resources["threads"]
            )
        }
        values.update(self.rule.constraints)
        return values

    @property
    def fingerprint(self) -> str:
        """Compatibility alias for the invocation-specific provenance hash."""

        return self.provenance_hash

    @property
    def workdir(self) -> Path:
        return self.dag.nodes_dir / self.relative_path

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
