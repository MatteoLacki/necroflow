from __future__ import annotations

import inspect
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from necroflow.contexts import NamedValues
from necroflow.fs import _check_path_limits
from necroflow.rule_call import RuleCall, _safe_path_component


class NodeType:
    """Base class for node types. Subclass to define types.

    class Fastq(NodeType): ...
    class SortedBam(Bam): filename = "sorted.bam"

    Filename-less subclasses are input-only type contracts. Every Rule output
    must use a NodeType whose filename resolves to a string.
    """

    filename: str | None = None
    invalidator = None

    def __new__(cls, *args, **kwargs):
        raise TypeError(
            f"{cls.__name__} is a NodeType declaration, not a Node constructor; "
            "create managed Nodes by calling a Rule with a Pipeline"
        )


@dataclass
class Node:
    output_name: str
    node_type: type[NodeType]
    relative_path: Path
    path: Path
    rule_call: RuleCall
    info: str | None = None

    def __post_init__(self):
        if self.info is None:
            doc = self.node_type.__doc__
            if doc:
                self.info = doc.strip()

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

    @classmethod
    def make_outputs(
        cls,
        pipeline,
        rule,
        node_inputs: NamedValues[Node | tuple[Node, ...]],
        config: dict,
        command,
        outputs_specs: dict,
    ) -> list[Node]:
        shellpath = pipeline.shellpath if command is not None else None
        call = RuleCall(
            dag=pipeline.dag,
            rule=rule,
            inputs=node_inputs,
            config=config,
            command=command,
            shellpath=shellpath,
        )
        workdir = call.workdir
        nodes: list[Node] = []
        output_paths: set[Path] = set()
        for oname, otype in outputs_specs.items():
            output_filename = otype.filename
            assert output_filename is not None
            filename = _safe_path_component(
                output_filename, kind=f"output {oname!r} filename"
            )
            relative_path = call.relative_path / filename
            if relative_path in output_paths:
                raise ValueError(
                    f"rule {rule.__name__!r} declares duplicate output path "
                    f"{relative_path}"
                )
            output_paths.add(relative_path)
            nodes.append(
                Node(
                    output_name=oname,
                    node_type=otype,
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


def _topo_sort(nodes: list[Node]) -> list[Node]:
    """Return nodes in topological order (parents before children) via Kahn's algorithm.

    Only edges between nodes in the provided list are considered.
    """
    key_to_node = {n.relative_path: n for n in nodes}
    children: dict[Path, list[Node]] = {n.relative_path: [] for n in nodes}
    in_degree: dict[Path, int] = {n.relative_path: 0 for n in nodes}
    for n in nodes:
        for p in n.parents:
            if p.relative_path in key_to_node:
                children[p.relative_path].append(n)
                in_degree[n.relative_path] += 1
    queue: deque[Node] = deque(n for n in nodes if in_degree[n.relative_path] == 0)
    result: list[Node] = []
    while queue:
        n = queue.popleft()
        result.append(n)
        for child in children[n.relative_path]:
            in_degree[child.relative_path] -= 1
            if in_degree[child.relative_path] == 0:
                queue.append(child)
    return result


def _is_nodetype(ann) -> bool:
    return inspect.isclass(ann) and issubclass(ann, NodeType)
