from __future__ import annotations
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Node:
    output_name: str | None = None
    parents: list[Node] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    rule: Callable | None = None


def rule(_fn: Callable | None = None, **resources):
    """Decorator: intercepts call, injects parents + config into returned Node(s).

    Positional args = parent Nodes (must be annotated Node). All kwargs = config.
    Resources (threads, memory, …) declared at decoration time: @rule(threads=4).
    """

    def decorator(fn: Callable) -> Callable:
        _params = [
            (name, param)
            for name, param in inspect.signature(fn).parameters.items()
            if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD)
        ]

        def wrapper(*args, **kwargs):
            for (name, param), val in zip(_params, args):
                annotated_node = param.annotation is Node
                is_node = isinstance(val, Node)
                if annotated_node and not is_node:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} annotated as Node, got {type(val).__name__!r}"
                    )
                if is_node and not annotated_node:
                    raise TypeError(
                        f"{fn.__name__}: {name!r} is a Node but missing Node annotation"
                    )
            parents = [a for a in args if isinstance(a, Node)]
            result = fn(*parents, **kwargs)
            nodes = (result,) if isinstance(result, Node) else tuple(result)
            for node in nodes:
                node.parents = parents
                node.config = kwargs
                node.rule = wrapper
            from necroflow.pipeline import _active_pipeline

            p = _active_pipeline.get()
            if p is not None:
                for node in nodes:
                    p._register(node)
            return result

        wrapper.__name__ = fn.__name__
        wrapper.__wrapped__ = fn
        wrapper.resources = resources
        return wrapper

    if _fn is not None:
        return decorator(_fn)
    return decorator
