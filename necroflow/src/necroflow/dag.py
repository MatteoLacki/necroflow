from __future__ import annotations
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable


class NodeType:
    """Lightweight named node type. Use as annotation and as factory.

    Fastq = NodeType("fastq")

    @rule
    def align(fastq: Fastq, *, ref):
        return Bam("bam"), Bam("log")
    """

    def __init__(self, name: str):
        self.name = name

    def __call__(self, output_name: str | None = None) -> Node:
        return Node(output_name=output_name, node_type=self)

    def __repr__(self) -> str:
        return f"NodeType({self.name!r})"


def node_types(names: str) -> tuple[NodeType, ...]:
    return tuple(NodeType(n) for n in names.split())


@dataclass
class Node:
    output_name: str | None = None
    node_type: NodeType | None = None
    parents: list[Node] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    rule: Callable | None = None


def rule(_fn: Callable | None = None, **resources):
    """Decorator: intercepts call, injects parents + config into returned Node(s).

    Positional args = parent Nodes. All kwargs = config.
    Annotations must be NodeType instances; types are checked at call time.
    Resources declared at decoration time: @rule(threads=4).
    """

    def decorator(fn: Callable) -> Callable:
        _params = [
            (name, param)
            for name, param in inspect.signature(fn).parameters.items()
            if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD)
        ]

        def wrapper(*args, **kwargs):
            for (name, param), val in zip(_params, args):
                ann = param.annotation
                is_nodetype = isinstance(ann, NodeType)
                is_node = isinstance(val, Node)

                if is_nodetype and not is_node:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} expected {ann!r}, got {type(val).__name__!r}"
                    )
                if is_node and not is_nodetype:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} is a Node but annotation is not a NodeType"
                    )
                if is_nodetype and is_node and val.node_type is not ann:
                    got = repr(val.node_type)
                    raise TypeError(
                        f"{fn.__name__}: {name!r} expected {ann!r}, got {got}"
                    )

            parents = [a for a in args if isinstance(a, Node)]
            result = fn(*parents, **kwargs)
            nodes = (result,) if isinstance(result, Node) else tuple(result)
            for node in nodes:
                node.parents = parents
                node.config = kwargs
                node.rule = wrapper
            return result

        wrapper.__name__ = fn.__name__
        wrapper.__wrapped__ = fn
        wrapper.resources = resources
        return wrapper

    if _fn is not None:
        return decorator(_fn)
    return decorator
