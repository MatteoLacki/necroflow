from __future__ import annotations

import hashlib
import inspect
from types import UnionType
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, get_args, get_origin

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
    """Metaclass so that Fastq("output_name") returns a Node, not a Fastq instance."""

    def __call__(cls, output_name: str | None = None) -> Node:
        return Node(output_name=output_name, node_type=cls)

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
    output_name: str | None = None
    node_type: type[NodeType] | None = None
    parents: list[Node] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    rule: Any | None = None
    command: str | Callable | None = None
    path: Path | None = None
    output_nodes: dict[str, Node] = field(default_factory=dict)
    state: NodeState | None = None
    info: str | None = None
    pipeline_label: str | None = None
    execution_context: dict[str, Any] = field(default_factory=dict)
    rule_call: RuleCall | None = None

    def __post_init__(self):
        if self.info is None and self.node_type is not None:
            doc = self.node_type.__doc__
            if doc:
                self.info = doc.strip()

    @property
    def full_fingerprint(self) -> str:
        """Full version-2 digest shared by co-outputs of one rule call."""

        if self.rule_call is not None:
            return self.rule_call.full_fingerprint
        return hashlib.sha256(
            f"necroflow.unbound/v2:{self.output_name}:{self.node_type!r}".encode()
        ).hexdigest()

    @property
    def fingerprint(self) -> str:
        """The 16-character path form of the full fingerprint."""

        return self.full_fingerprint[:16]

    @property
    def key(self) -> str:
        """Unique key for a node: rule_name/fingerprint/filename.
        Distinct for co-outputs because filename differs."""
        rule_name = self.rule.__name__ if self.rule else "unknown"
        filename = (
            self.node_type.filename
            if self.node_type and self.node_type.filename
            else self.output_name or "output"
        )
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
        cls, rule, parents: list[Node], config: dict, command, outputs_specs: dict
    ) -> list[Node]:
        call = RuleCall(rule=rule, parents=parents, config=config, command=command)
        nodes = [
            cls(
                output_name=oname,
                node_type=otype,
                parents=parents,
                config=config,
                rule=rule,
                command=command,
                execution_context=call.execution_context,
                rule_call=call,
            )
            for oname, otype in outputs_specs.items()
        ]
        all_outputs = {n.output_name: n for n in nodes}
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
