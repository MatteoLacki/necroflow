from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, ClassVar

from necroflow.contexts import NamedValues
from necroflow.fs import _check_path_limits
from necroflow.rule_call import RuleCall, _safe_path_component


@dataclass(eq=False)
class Node:
    """Typed output identified by its final, node-store-relative path."""

    filename: ClassVar[str | None] = None
    invalidator: ClassVar[Callable | None] = None

    output_name: str
    relative_path: Path
    path: Path
    rule_call: RuleCall
    info: str | None = None

    def __post_init__(self):
        if self.info is None:
            doc = type(self).__doc__
            if doc:
                self.info = doc.strip()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Node):
            return NotImplemented
        return self.relative_path == other.relative_path

    def __hash__(self) -> int:
        return hash(self.relative_path)

    @property
    def parents(self) -> list[Node]:
        return self.rule_call.parents

    @property
    def config(self) -> dict[str, Any]:
        return self.rule_call.config

    @property
    def rule(self) -> Any:
        return self.rule_call.rule

    @property
    def command(self) -> str | Callable | None:
        return self.rule_call.command

    @property
    def output_nodes(self) -> dict[str, Node]:
        return self.rule_call.output_nodes

    @property
    def rule_hash(self) -> str:
        return self.rule_call.rule_hash

    @property
    def provenance_hash(self) -> str:
        return self.rule_call.provenance_hash

    @property
    def hash_file(self) -> Path:
        """Stored tagged content digest of this output."""
        return self.rule_call.metadata_dir / f"{self.path.name}.hash"

    @property
    def invalidation_file(self) -> Path:
        """Invalidator token recorded when this output was produced."""
        return self.rule_call.metadata_dir / f"{self.path.name}.invalidation"

    def invalidation_token(self) -> str | None:
        """Return the Node invalidator's current token, or None without one."""
        invalidator = type(self).invalidator
        if invalidator is None:
            return None
        token = invalidator(self)
        if not isinstance(token, str):
            raise TypeError(
                f"invalidator for {type(self).__name__} must return str, "
                f"got {type(token).__name__}"
            )
        return token

    @classmethod
    def make_outputs(
        cls,
        pipeline,
        rule,
        node_inputs: NamedValues[Node | tuple[Node, ...]],
        input_values: NamedValues[Any],
        config: dict,
        command,
        outputs_specs: dict,
        resource_requests: dict | None = None,
    ) -> list[Node]:
        shellpath = pipeline.shellpath if command is not None else None
        call = RuleCall(
            dag=pipeline.dag,
            rule=rule,
            inputs=node_inputs,
            input_values=input_values,
            config=config,
            command=command,
            shellpath=shellpath,
            resource_requests=resource_requests,
        )
        workdir = call.workdir
        nodes: list[Node] = []
        output_paths: dict[Path, str] = {}
        for oname, otype in outputs_specs.items():
            output_filename = otype.filename
            assert output_filename is not None
            filename = _safe_path_component(
                output_filename, kind=f"output {oname!r} filename"
            )
            relative_path = call.relative_path / filename
            if relative_path in output_paths:
                previous_name = output_paths[relative_path]
                previous_type = outputs_specs[previous_name]
                raise ValueError(
                    f"rule {rule.__name__!r} declares duplicate output path "
                    f"{relative_path}: outputs {previous_name!r} "
                    f"({previous_type.__name__}) and {oname!r} "
                    f"({otype.__name__}) resolve to the same filename "
                    f"{relative_path.name!r} in one hashed workdir. "
                    "Change one output's Node.filename to a distinct filename "
                    "(use a separate Node subclass if both outputs share a type); "
                    "renaming only the output variable does not change its filename."
                )
            output_paths[relative_path] = oname
            nodes.append(
                otype(
                    output_name=oname,
                    relative_path=relative_path,
                    path=workdir / filename,
                    rule_call=call,
                )
            )
        for node in nodes:
            _check_path_limits(node.path)
        all_outputs: dict[str, Node] = {n.output_name: n for n in nodes}
        call.output_nodes = all_outputs
        canonical = pipeline.dag.intern(call)
        return [canonical.output_nodes[name] for name in outputs_specs]


def _is_nodetype(ann) -> bool:
    return inspect.isclass(ann) and issubclass(ann, Node)
