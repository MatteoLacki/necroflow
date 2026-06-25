from __future__ import annotations

from collections import namedtuple

from necroflow.nodes import Node, _is_nodetype


class Inputs:
    """Declare rule inputs: NodeType values = positional Node args; plain types = config kwargs."""

    def __init__(self, **specs):
        self.specs = specs


class Outputs:
    """Declare rule outputs by name: Outputs(bam=Bam, log=Log)."""

    def __init__(self, **specs):
        self.specs = specs


class Constraints:
    """Declare scheduler constraints: Constraints(threads=4, ram="250Mi")."""

    def __init__(self, **kwargs):
        self.specs = kwargs


class Rule:
    """A registered rule: validates inputs and produces output Nodes when called."""

    def __init__(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | list[str],
        constraints: Constraints | None = None,
        info: str | None = None,
    ):
        self.__name__ = name
        self.inputs = inputs
        self.outputs = outputs
        self.command = command
        self.constraints = constraints.specs if constraints else {}
        self.info = info
        self._pos_inputs = [(n, t) for n, t in inputs.specs.items() if _is_nodetype(t)]
        self._kw_inputs = {n: t for n, t in inputs.specs.items() if not _is_nodetype(t)}
        output_names = list(outputs.specs.keys())
        self._multi = len(output_names) > 1
        self._return_type = namedtuple(f"{name}_outputs", output_names) if self._multi else None

    def __call__(self, *args, **kwargs):
        name = self.__name__
        if len(args) < len(self._pos_inputs):
            missing = [pname for pname, _ in self._pos_inputs[len(args):]]
            raise TypeError(f"{name}: missing required inputs: {missing!r}")
        if len(args) > len(self._pos_inputs):
            raise TypeError(
                f"{name}: too many positional inputs: expected {len(self._pos_inputs)}, got {len(args)}"
            )
        missing_kw = [kname for kname in self._kw_inputs if kname not in kwargs]
        if missing_kw:
            raise TypeError(f"{name}: missing required inputs: {missing_kw!r}")
        for (pname, ptype), val in zip(self._pos_inputs, args):
            if not isinstance(val, Node):
                raise TypeError(f"{name}: {pname!r} expected Node, got {type(val).__name__!r}")
            if val.node_type is None or not issubclass(val.node_type, ptype):
                got = val.node_type.__name__ if val.node_type else "None"
                raise TypeError(f"{name}: {pname!r} expected {ptype.__name__}, got {got}")
        for kname, val in kwargs.items():
            if kname not in self._kw_inputs:
                continue
            ktype = self._kw_inputs[kname]
            try:
                ok = isinstance(val, ktype)
            except TypeError:
                ok = True
            if not ok:
                raise TypeError(f"{name}: {kname!r} expected {ktype}, got {type(val).__name__!r}")

        parents = [a for a in args if isinstance(a, Node)]
        nodes = Node.make_outputs(self, parents, kwargs, self.command, self.outputs.specs)
        return self._return_type(*nodes) if self._multi else nodes[0]


class Rules:
    """Container for registered rules. Names must be unique."""

    def __init__(self):
        self._registry: dict = {}

    def register(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | list[str],
        constraints: Constraints | None = None,
        info: str | None = None,
    ) -> None:
        if name in self._registry:
            raise ValueError(f"Rule {name!r} already registered")
        rule = Rule(name, inputs, outputs, command, constraints, info)
        self._registry[name] = rule
        self.__dict__[name] = rule
