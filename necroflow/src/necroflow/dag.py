from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Node:
    rule_name: str
    parents: list[Node]
    config: dict[str, Any]
    output_name: str | None = None  # set for multi-output rules


def rule(fn=None, *, outputs: list[str] | None = None):
    """Decorator turning a function into a DAG rule.

    Single-output (default): returns Node.
    Multi-output: @rule(outputs=["a", "b"]) returns tuple[Node, ...].
    """
    def decorator(fn):
        def wrapper(*parents: Node, config: dict | None = None):
            cfg = config or {}
            if outputs is None:
                return Node(rule_name=fn.__name__, parents=list(parents), config=cfg)
            return tuple(
                Node(rule_name=fn.__name__, parents=list(parents), config=cfg, output_name=name)
                for name in outputs
            )
        wrapper.__name__ = fn.__name__
        wrapper.__wrapped__ = fn
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator


def input_rule(fn=None, *, outputs: list[str] | None = None):
    """Like @rule but takes no parent nodes — wraps external inputs."""
    def decorator(fn):
        def wrapper(*, config: dict | None = None):
            cfg = config or {}
            if outputs is None:
                return Node(rule_name=fn.__name__, parents=[], config=cfg)
            return tuple(
                Node(rule_name=fn.__name__, parents=[], config=cfg, output_name=name)
                for name in outputs
            )
        wrapper.__name__ = fn.__name__
        wrapper.__wrapped__ = fn
        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator
