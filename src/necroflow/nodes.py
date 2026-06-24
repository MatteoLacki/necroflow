from __future__ import annotations

import inspect
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


def _is_nodetype(ann) -> bool:
    return inspect.isclass(ann) and issubclass(ann, NodeType)
