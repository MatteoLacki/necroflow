from __future__ import annotations
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable


class NodeTypeMeta(type):
    """Metaclass so that Fastq("output_name") returns a Node, not a Fastq instance."""

    def __call__(cls, output_name: str | None = None) -> Node:
        return Node(output_name=output_name, node_type=cls)

    def __repr__(cls) -> str:
        return cls.__name__


class NodeType(metaclass=NodeTypeMeta):
    """Base class for node types. Subclass to define types.

    class Fastq(NodeType): ...
    class SortedBam(Bam): ...   # inherits — accepted wherever Bam expected

    Fastq("label")  # creates Node(node_type=Fastq, output_name="label")
    """


def node_types(names: str) -> tuple[type[NodeType], ...]:
    """Create NodeType subclasses from a space-separated string of names."""
    return tuple(type(n, (NodeType,), {}) for n in names.split())


def _is_nodetype(ann) -> bool:
    return inspect.isclass(ann) and issubclass(ann, NodeType)


@dataclass
class Node:
    output_name: str | None = None
    node_type: type[NodeType] | None = None
    parents: list[Node] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    rule: Callable | None = None


def rule(_fn: Callable | None = None, **resources):
    """Decorator: intercepts call, injects parents + config into returned Node(s).

    All parameters must be annotated.
    Positional params must be annotated with a NodeType subclass.
    Keyword-only params are type-checked via isinstance at call time (unions supported).
    Resources declared at decoration time: @rule(threads=4).
    """

    def decorator(fn: Callable) -> Callable:
        sig = inspect.signature(fn)

        pos_params = [
            (name, param)
            for name, param in sig.parameters.items()
            if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD)
        ]
        kw_params = {
            name: param
            for name, param in sig.parameters.items()
            if param.kind == param.KEYWORD_ONLY
        }

        # Enforce annotations on all params at decoration time
        for name, param in pos_params + list(kw_params.items()):
            if param.annotation is inspect.Parameter.empty:
                raise TypeError(
                    f"{fn.__name__}: parameter {name!r} must have a type annotation"
                )

        def wrapper(*args, **kwargs):
            # Check positional (Node) args
            for (name, param), val in zip(pos_params, args):
                ann = param.annotation
                is_nt = _is_nodetype(ann)
                is_node = isinstance(val, Node)

                if is_nt and not is_node:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} expected {ann.__name__}, got {type(val).__name__!r}"
                    )
                if is_node and not is_nt:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} is a Node but annotation is not a NodeType"
                    )
                if is_nt and is_node:
                    if val.node_type is None or not issubclass(val.node_type, ann):
                        got = val.node_type.__name__ if val.node_type else "None"
                        raise TypeError(
                            f"{fn.__name__}: {name!r} expected {ann.__name__}, got {got}"
                        )

            # Check keyword args via isinstance (supports str | int unions)
            for name, val in kwargs.items():
                if name not in kw_params:
                    continue
                ann = kw_params[name].annotation
                if _is_nodetype(ann):
                    continue
                try:
                    ok = isinstance(val, ann)
                except TypeError:
                    ok = True  # complex generics (list[str] etc.) — skip
                if not ok:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} expected {ann}, got {type(val).__name__!r}"
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
