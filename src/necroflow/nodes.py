from __future__ import annotations

import inspect
from types import UnionType
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, get_args, get_origin

from necroflow.fingerprints import validate_fingerprint_result
from necroflow.rule_call import RuleCall

_COMPROMISED_STATES = {"running", "failed", "interrupted"}


class NodeState(Enum):
    MISSING = "missing"
    STALE = "stale"
    UP_TO_DATE = "up_to_date"
    ORPHAN = "orphan"
    READY = "ready"
    RUNNING = "running"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class NodeTypeMeta(type):
    """Metaclass for declarative node types."""

    def __call__(cls, output_name: str | None = None) -> Node:
        raise TypeError(
            f"{cls.__name__} is a NodeType declaration, not a Node constructor; "
            "create managed Nodes by calling a Rule with a Pipeline"
        )

    def __repr__(cls) -> str:
        return cls.__name__


class NodeType(metaclass=NodeTypeMeta):
    """Base class for node types. Subclass to define types.

    class Fastq(NodeType): ...
    class SortedBam(Bam): filename = "sorted.bam"
    """

    filename: str | None = None
    invalidator = None

    @staticmethod
    def _type_name(ann) -> str:
        origin = get_origin(ann)
        if origin is UnionType:
            return "|".join(
                sorted(NodeType._type_name(member) for member in get_args(ann))
            )
        return ann.__name__ if hasattr(ann, "__name__") else repr(ann)


@dataclass
class Node:
    output_name: str
    node_type: type[NodeType]
    parents: list[Node]
    config: dict[str, Any]
    rule: Any
    command: str | Callable | None
    path: Path
    rule_call: RuleCall
    output_nodes: dict[str, Node] = field(default_factory=dict)
    state: NodeState | None = None
    info: str | None = None
    pipeline_label: str | None = None
    execution_context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.info is None:
            doc = self.node_type.__doc__
            if doc:
                self.info = doc.strip()

    @property
    def full_fingerprint(self) -> str:
        """Full version-2 digest shared by co-outputs of one rule call."""

        return self.rule_call.full_fingerprint

    @property
    def fingerprint(self) -> str:
        """The 16-character path form of the full fingerprint."""

        return self.full_fingerprint[:16]

    @property
    def key(self) -> str:
        """Unique key for a node: rule_name/fingerprint/filename.
        Distinct for co-outputs because filename differs."""
        rule_name = self.rule.__name__
        filename = self.node_type.filename or self.output_name
        return f"{rule_name}/{self.fingerprint}/{filename}"

    @property
    def state_file(self) -> Path:
        return self.path.parent / ".rip" / "state"

    @property
    def is_compromised(self) -> bool:
        return (
            self.state_file.exists()
            and self.state_file.read_text().strip() in _COMPROMISED_STATES
        )

    def mark_running(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text("running")

    def mark_done(self, state: str) -> None:
        self.state_file.write_text(state)

    @classmethod
    def make_outputs(
        cls,
        pipeline,
        rule,
        parents: list[Node],
        config: dict,
        command,
        outputs_specs: dict,
    ) -> list[Node]:
        from necroflow.dag import _check_path_limits

        execution_context = pipeline.execution_context if command is not None else {}
        call = RuleCall(
            pipeline=pipeline,
            rule=rule,
            parents=parents,
            config=config,
            command=command,
            execution_context=execution_context,
            fingerprint_provider=pipeline.fingerprint_provider,
        )
        value = pipeline.fingerprint_function(call.fingerprint_args())
        call._full_fingerprint = validate_fingerprint_result(
            value, provider=pipeline.fingerprint_provider
        )
        workdir = pipeline.nodes_dir / rule.__name__ / call.full_fingerprint[:16]
        nodes: list[Node] = [
            Node(
                output_name=oname,
                node_type=otype,
                parents=parents,
                config=config,
                rule=rule,
                command=command,
                path=workdir / (otype.filename or oname),
                execution_context=call.execution_context,
                rule_call=call,
            )
            for oname, otype in outputs_specs.items()
        ]
        for node in nodes:
            _check_path_limits(node.path)
        all_outputs: dict[str, Node] = {n.output_name: n for n in nodes}
        for n in nodes:
            n.output_nodes = all_outputs
        call.output_nodes = all_outputs
        return nodes


def _topo_sort(nodes: list[Node]) -> list[Node]:
    """Return nodes in topological order (parents before children) via Kahn's algorithm.

    Only edges between nodes in the provided list are considered.
    """
    key_to_node = {n.key: n for n in nodes}
    children: dict[str, list[Node]] = {n.key: [] for n in nodes}
    in_degree: dict[str, int] = {n.key: 0 for n in nodes}
    for n in nodes:
        for p in n.parents:
            if p.key in key_to_node:
                children[p.key].append(n)
                in_degree[n.key] += 1
    queue: deque[Node] = deque(n for n in nodes if in_degree[n.key] == 0)
    result: list[Node] = []
    while queue:
        n = queue.popleft()
        result.append(n)
        for child in children[n.key]:
            in_degree[child.key] -= 1
            if in_degree[child.key] == 0:
                queue.append(child)
    return result


def _is_nodetype(ann) -> bool:
    return inspect.isclass(ann) and issubclass(ann, NodeType)


def iter_connected_components(nodes: list[Node]):
    """Yield each connected component of nodes as a list (undirected parent↔child edges)."""
    node_keys = {n.key for n in nodes}
    adj: dict[str, list[Node]] = {n.key: [] for n in nodes}
    for n in nodes:
        for p in n.parents:
            if p.key in node_keys:
                adj[n.key].append(p)
                adj[p.key].append(n)

    visited: set[str] = set()
    key_to_node = {n.key: n for n in nodes}
    for n in nodes:
        if n.key in visited:
            continue
        frontier = [n]
        component: list[Node] = []
        while frontier:
            cur = frontier.pop()
            if cur.key in visited:
                continue
            visited.add(cur.key)
            component.append(key_to_node[cur.key])
            frontier.extend(nb for nb in adj[cur.key] if nb.key not in visited)
        yield component
