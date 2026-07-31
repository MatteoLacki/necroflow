from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

from necroflow.contexts import CommandArgs, NamedValues

if TYPE_CHECKING:
    from necroflow.nodes import Node


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
    _rule_hash: str | None = None
    _provenance_hash: str | None = None
    _relative_path: Path | None = None
    _realized_command: str | None = None
    _command_realized: bool = False

    def _constraints(self) -> dict[str, Any]:
        values = {
            "threads": self.rule.constraints.get(
                "threads", self.rule.resources["threads"]
            )
        }
        values.update(self.rule.constraints)
        return values

    @property
    def parents(self) -> list[Node]:
        """Return all input Nodes in declaration and tuple-element order."""
        result: list[Node] = []
        for value in self.inputs.values():
            result.extend(value if isinstance(value, tuple) else (value,))
        return result

    @property
    def rule_hash(self) -> str:
        if self._rule_hash is None:
            raise RuntimeError("RuleCall rule hash was not compiled")
        return self._rule_hash

    @property
    def provenance_hash(self) -> str:
        if self._provenance_hash is None:
            raise RuntimeError("RuleCall provenance hash was not compiled")
        return self._provenance_hash

    @property
    def fingerprint(self) -> str:
        """Compatibility alias for the invocation-specific provenance hash."""

        return self.provenance_hash

    @property
    def relative_path(self) -> Path:
        if self._relative_path is None:
            raise RuntimeError("RuleCall path was not compiled")
        return self._relative_path

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
