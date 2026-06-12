from __future__ import annotations
import inspect
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any


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
    rule: Any | None = None
    command: str | list[str] | None = None


class Inputs:
    """Declare rule inputs: NodeType values = positional Node args; plain types = config kwargs."""

    def __init__(self, **specs):
        self.specs = specs


class Outputs:
    """Declare rule outputs by name: Outputs(bam=Bam, log=Log)."""

    def __init__(self, **specs):
        self.specs = specs


class Constraints:
    """Declare scheduler constraints: Constraints(threads=4, memory="8G")."""

    def __init__(self, **kwargs):
        self.specs = kwargs


class Rules:
    """Container for registered rules. Names must be unique."""

    def __init__(self):
        object.__setattr__(self, "_registry", {})

    def register(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | list[str],
        constraints: Constraints | None = None,
    ) -> None:
        registry = object.__getattribute__(self, "_registry")
        if name in registry:
            raise ValueError(f"Rule {name!r} already registered")

        pos_inputs = [(n, t) for n, t in inputs.specs.items() if _is_nodetype(t)]
        kw_inputs = {n: t for n, t in inputs.specs.items() if not _is_nodetype(t)}

        output_names = list(outputs.specs.keys())
        multi = len(output_names) > 1
        ReturnType = namedtuple(f"{name}_outputs", output_names) if multi else None

        constraints_dict = constraints.specs if constraints else {}

        def wrapper(*args, **kwargs):
            for (pname, ptype), val in zip(pos_inputs, args):
                if not isinstance(val, Node):
                    raise TypeError(
                        f"{name}: {pname!r} expected Node, got {type(val).__name__!r}"
                    )
                if val.node_type is None or not issubclass(val.node_type, ptype):
                    got = val.node_type.__name__ if val.node_type else "None"
                    raise TypeError(
                        f"{name}: {pname!r} expected {ptype.__name__}, got {got}"
                    )

            for kname, val in kwargs.items():
                if kname not in kw_inputs:
                    continue
                ktype = kw_inputs[kname]
                try:
                    ok = isinstance(val, ktype)
                except TypeError:
                    ok = True
                if not ok:
                    raise TypeError(
                        f"{name}: {kname!r} expected {ktype}, got {type(val).__name__!r}"
                    )

            parents = [a for a in args if isinstance(a, Node)]
            nodes = [
                Node(
                    output_name=oname,
                    node_type=otype,
                    parents=parents,
                    config=kwargs,
                    rule=wrapper,
                    command=command,
                )
                for oname, otype in outputs.specs.items()
            ]

            return ReturnType(*nodes) if multi else nodes[0]

        wrapper.__name__ = name
        wrapper.constraints = constraints_dict
        wrapper.inputs = inputs
        wrapper.outputs = outputs
        wrapper.command = command

        registry[name] = wrapper
        object.__setattr__(self, name, wrapper)
