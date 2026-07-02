from __future__ import annotations

from collections import namedtuple

from necroflow.nodes import Node, _is_nodetype

_SI_SUFFIXES = {"K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15}
_BIN_SUFFIXES = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50}


def parse_resource(s: str | int) -> int:
    """Parse a resource value with optional unit suffix.

    SI (1000-based):     K  M  G  T  P
    Binary (1024-based): Ki Mi Gi Ti Pi
    Plain integer string or int passed through as-is.
    """
    if isinstance(s, int):
        return s
    s = s.strip()
    for suffix, mult in _BIN_SUFFIXES.items():  # binary first — longer suffixes
        if s.endswith(suffix):
            return int(s[: -len(suffix)]) * mult
    for suffix, mult in _SI_SUFFIXES.items():
        if s.endswith(suffix):
            return int(s[: -len(suffix)]) * mult
    return int(s)


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

    @property
    def resources(self) -> dict[str, int]:
        result = {k: parse_resource(v) for k, v in self.constraints.items()}
        result.setdefault("threads", 1)
        return result

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

    def rule(self, fn=None, **constraints):
        """Decorator to register a rule from a function definition.

        Usage:
            rule = R.rule

            @rule
            def sort_bam(bam: Bam) -> SortedBam[sorted_bam]:
                "Sort BAM by coordinate."
                command = "samtools sort {bam} -o {sorted_bam}"

            @rule(threads=4)
            def align(fastq: Fastq, ref: str) -> Bam[bam], Log[log]:
                "Align reads with BWA-MEM."
                command = "bwa mem {ref} {fastq} > {bam}"

        Requires ``from __future__ import annotations`` in the calling module
        so that ``Type[name]`` return annotations are not evaluated at definition time.
        """
        import ast
        import inspect
        import re
        import textwrap

        def _pascal_to_snake(name: str) -> str:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

        def decorator(fn):
            rule_name = fn.__name__
            info = fn.__doc__.strip() if fn.__doc__ else None

            # inputs: resolve parameter annotations (strings if future annotations active)
            raw_anns = fn.__annotations__
            inputs_specs = {}
            for pname, ann in raw_anns.items():
                if pname == 'return':
                    continue
                if isinstance(ann, str):
                    ann = eval(ann, fn.__globals__)  # noqa: PGH001
                inputs_specs[pname] = ann

            # outputs: parse return annotation
            return_ann = raw_anns.get('return')
            outputs_specs = {}
            if return_ann is not None:
                if not isinstance(return_ann, str):
                    # no future annotations; must be a plain unevaluated type
                    outputs_specs[_pascal_to_snake(return_ann.__name__)] = return_ann
                else:
                    try:
                        expr_tree = ast.parse(return_ann.strip(), mode='eval')
                    except SyntaxError:
                        raise ValueError(
                            f"rule {rule_name!r}: cannot parse return annotation {return_ann!r}"
                        )
                    items = (
                        expr_tree.body.elts
                        if isinstance(expr_tree.body, ast.Tuple)
                        else [expr_tree.body]
                    )
                    for item in items:
                        if isinstance(item, ast.Subscript):
                            type_name = item.value.id
                            out_name = item.slice.id
                            outputs_specs[out_name] = fn.__globals__[type_name]
                        elif isinstance(item, ast.Name):
                            type_obj = fn.__globals__[item.id]
                            outputs_specs[_pascal_to_snake(item.id)] = type_obj
                        else:
                            raise ValueError(
                                f"rule {rule_name!r}: return annotation items must be "
                                f"Type[name] or Type, got {ast.dump(item)}"
                            )

            # command: find `command = "..."` assignment in function body
            src = textwrap.dedent(inspect.getsource(fn))
            func_tree = ast.parse(src)
            func_def = next(
                n for n in ast.walk(func_tree)
                if isinstance(n, ast.FunctionDef) and n.name == rule_name
            )
            command = None
            for stmt in func_def.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and stmt.targets[0].id == 'command'
                ):
                    expr = ast.Expression(body=stmt.value)
                    ast.fix_missing_locations(expr)
                    command = eval(  # noqa: PGH001
                        compile(expr, '<rule>', 'eval'), fn.__globals__
                    )
                    break

            if command is None:
                raise ValueError(f"rule {rule_name!r}: no 'command = ...' found in body")

            constraints_obj = Constraints(**constraints) if constraints else None
            self.register(
                rule_name,
                Inputs(**inputs_specs),
                Outputs(**outputs_specs),
                command,
                constraints_obj,
                info,
            )
            return self.__dict__[rule_name]

        return decorator(fn) if fn is not None else decorator
