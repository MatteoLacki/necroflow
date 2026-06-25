from __future__ import annotations

import hashlib
import inspect
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


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

    @staticmethod
    def _type_name(ann) -> str:
        return ann.__name__ if hasattr(ann, "__name__") else repr(ann)


@dataclass
class Node:
    output_name: str | None = None
    node_type: type[NodeType] | None = None
    parents: list[Node] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    rule: Any | None = None
    command: str | list[str] | None = None
    path: Path | None = None
    output_nodes: dict[str, Node] = field(default_factory=dict)
    state: NodeState | None = None
    info: str | None = None
    pipeline_label: str | None = None

    def __post_init__(self):
        if self.info is None and self.node_type is not None:
            doc = self.node_type.__doc__
            if doc:
                self.info = doc.strip()

    @property
    def fingerprint(self) -> str:
        """16-char hex fingerprint of the rule call that produced this node.

        Shared by all co-outputs of the same call — output_name is excluded from
        this hash but included in parent references so downstream nodes that
        consume different co-outputs still get distinct hashes.

        Constraints intentionally excluded: they describe execution resources
        (threads, memory), not the computation itself. If the output already
        exists on disk, the constraints used to produce it are irrelevant.
        """
        h = hashlib.sha256()
        h.update((self.rule.__name__ if self.rule else "").encode())
        cmd = self.command
        h.update((cmd if isinstance(cmd, str) else repr(cmd) if cmd else "").encode())
        for k, v in sorted(self.config.items()):
            h.update(f"{k}={v!r}".encode())
        for p in self.parents:
            h.update(p.fingerprint.encode())
            h.update((p.output_name or "").encode())
        if self.rule:
            for k, v in sorted(self.rule.inputs.specs.items()):
                h.update(f"i:{k}={NodeType._type_name(v)}".encode())
            for k, v in sorted(self.rule.outputs.specs.items()):
                h.update(f"o:{k}={NodeType._type_name(v)}".encode())
        return h.hexdigest()[:16]

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


    @classmethod
    def make_outputs(cls, rule, parents: list[Node], config: dict, command, outputs_specs: dict) -> list[Node]:
        nodes = [
            cls(
                output_name=oname,
                node_type=otype,
                parents=parents,
                config=config,
                rule=rule,
                command=command,
            )
            for oname, otype in outputs_specs.items()
        ]
        all_outputs = {n.output_name: n for n in nodes}
        for n in nodes:
            n.output_nodes = all_outputs
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
