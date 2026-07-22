from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from necroflow.contexts import CommandArgs, FingerprintArgs, NamedValues
from necroflow.fingerprints import DEFAULT_FINGERPRINT_PROVIDER

if TYPE_CHECKING:
    from necroflow.nodes import Node


@dataclass
class RuleCall:
    """One concrete invocation shared by all of its output Nodes."""

    pipeline: Any
    rule: Any
    parents: list[Node]
    config: dict[str, Any]
    command: str | Callable | None
    execution_context: dict[str, Any] = field(default_factory=dict)
    output_nodes: dict[str, Node] = field(default_factory=dict)
    fingerprint_provider: str = DEFAULT_FINGERPRINT_PROVIDER
    _full_fingerprint: str | None = None
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

    def fingerprint_args(self) -> FingerprintArgs:
        named_parents = {
            name: parent
            for (name, _annotation), parent in zip(self.rule._pos_inputs, self.parents)
        }
        return FingerprintArgs(
            rule_name=self.rule.__name__,
            command=self.command,
            inputs=NamedValues(named_parents),
            config=NamedValues(self.config),
            input_types=NamedValues(self.rule.inputs.specs),
            output_types=NamedValues(self.rule.outputs.specs),
            constraints=NamedValues(self._constraints()),
            execution_context=NamedValues(self.execution_context),
            repeat=self.rule.repeat,
            recipe_identity=self.rule.recipe_identity,
        )

    @property
    def full_fingerprint(self) -> str:
        if self._full_fingerprint is None:
            raise RuntimeError("RuleCall fingerprint was not compiled")
        return self._full_fingerprint

    def command_args(self) -> CommandArgs:
        named_inputs = {
            name: parent.path
            for (name, _annotation), parent in zip(self.rule._pos_inputs, self.parents)
        }
        outputs = {name: node.path for name, node in self.output_nodes.items()}
        first_output = next(iter(self.output_nodes.values()))
        return CommandArgs(
            inputs=NamedValues(named_inputs),
            config=NamedValues(self.config),
            outputs=NamedValues(outputs),
            constraints=NamedValues(self._constraints()),
            workdir=first_output.path.parent,
        )
