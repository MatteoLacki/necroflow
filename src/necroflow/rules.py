from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable
import re

from necroflow.nodes import Node, _is_nodetype

BUILTIN_COMMAND_PLACEHOLDERS = {"workdir"}

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


def _pascal_to_snake(name: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


class Rule:
    """A registered rule: validates inputs and produces output Nodes when called."""

    def __init__(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | list[str] | None,
        constraints: Constraints | None = None,
        info: str | None = None,
        repeat: int = 1,
        recipe_identity: str | None = None,
        materializer: Callable | None = None,
    ):
        self.__name__ = name
        self.inputs = inputs
        self.outputs = outputs
        self.command = command
        self.recipe_identity = recipe_identity
        self.materializer = materializer
        self.constraints = constraints.specs if constraints else {}
        self.repeat = self._validate_repeat(repeat)
        self.info = info
        self._pos_inputs = [(n, t) for n, t in inputs.specs.items() if _is_nodetype(t)]
        self._kw_inputs = {n: t for n, t in inputs.specs.items() if not _is_nodetype(t)}
        reserved = BUILTIN_COMMAND_PLACEHOLDERS & (set(inputs.specs) | set(outputs.specs))
        if reserved:
            raise ValueError(
                f"Rule {name!r}: reserved command placeholder name used as input/output: "
                f"{sorted(reserved)}"
            )
        output_names = list(outputs.specs.keys())
        self._multi = len(output_names) > 1
        self._return_type = namedtuple(f"{name}_outputs", output_names) if self._multi else None
        if command is not None:
            self._validate_command(name, inputs, outputs, command)

    @staticmethod
    def _validate_repeat(repeat: int) -> int:
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ValueError(f"repeat must be a positive integer, got {repeat!r}")
        return repeat

    @staticmethod
    def _validate_command(name, inputs, outputs, command):
        text = command if isinstance(command, str) else " ".join(str(c) for c in command)
        placeholders = set(re.findall(r'\{(\w+)\}', text))
        all_names = set(inputs.specs) | set(outputs.specs) | BUILTIN_COMMAND_PLACEHOLDERS
        unknown = placeholders - all_names
        missing_outputs = set(outputs.specs) - placeholders
        errors = []
        if unknown:
            errors.append(f"unknown placeholders: {sorted(unknown)}")
        if missing_outputs:
            errors.append(f"outputs not referenced in command: {sorted(missing_outputs)}")
        if errors:
            raise ValueError(f"Rule {name!r}: " + "; ".join(errors))

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


def _parse_rule_fn(fn) -> tuple:
    """Extract (rule_name, inputs_specs, outputs_specs, info) from a decorator-style rule function.

    Outputs are taken from a ``return Type[name]`` statement in the function body (preferred),
    or from the ``->`` return annotation as a fallback (requires ``from __future__ import
    annotations`` when using ``Type[name]`` subscript syntax).
    """
    import ast
    import inspect
    import textwrap

    def _parse_output_items(items):
        specs = {}
        for item in items:
            if isinstance(item, ast.Subscript):
                specs[item.slice.id] = fn.__globals__[item.value.id]
            elif isinstance(item, ast.Name):
                specs[_pascal_to_snake(item.id)] = fn.__globals__[item.id]
            else:
                raise ValueError(
                    f"rule {rule_name!r}: output items must be Type[name] or Type"
                )
        return specs

    rule_name = fn.__name__
    info = fn.__doc__.strip() if fn.__doc__ else None

    raw_anns = fn.__annotations__
    inputs_specs = {}
    for pname, ann in raw_anns.items():
        if pname == 'return':
            continue
        if isinstance(ann, str):
            ann = eval(ann, fn.__globals__)  # noqa: PGH001
        inputs_specs[pname] = ann

    # Prefer return Type[name] in the function body over -> annotation
    outputs_specs = {}
    body_return = None
    try:
        src = textwrap.dedent(inspect.getsource(fn))
        func_tree = ast.parse(src)
        func_def = next(
            n for n in ast.walk(func_tree)
            if isinstance(n, ast.FunctionDef) and n.name == rule_name
        )
        for stmt in func_def.body:
            if isinstance(stmt, ast.Return) and stmt.value is not None:
                body_return = stmt.value
                break
    except (OSError, StopIteration):
        pass

    if body_return is not None:
        items = body_return.elts if isinstance(body_return, ast.Tuple) else [body_return]
        outputs_specs = _parse_output_items(items)
    else:
        return_ann = raw_anns.get('return')
        if return_ann is not None:
            if not isinstance(return_ann, str):
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
                outputs_specs = _parse_output_items(items)

    return rule_name, inputs_specs, outputs_specs, info


class Rules:
    """Container for registered rules. Names must be unique."""

    def __init__(self):
        self._registry: dict = {}

    def register(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | list[str] | None,
        constraints: Constraints | None = None,
        info: str | None = None,
        repeat: int = 1,
        recipe_identity: str | None = None,
        materializer: Callable | None = None,
    ) -> None:
        if name in self._registry:
            raise ValueError(f"Rule {name!r} already registered")
        rule = Rule(
            name,
            inputs,
            outputs,
            command,
            constraints,
            info,
            repeat,
            recipe_identity,
            materializer,
        )
        self._registry[name] = rule
        self.__dict__[name] = rule

    def text_file(
        self,
        name: str,
        output: type,
        *,
        input_name: str = "text",
        encoding: str = "utf-8",
        output_name: str | None = None,
    ) -> None:
        """Register a built-in rule that writes a string config value to a file.

        The text is written directly by Python, not passed through the shell.
        """
        if input_name in BUILTIN_COMMAND_PLACEHOLDERS:
            raise ValueError(f"text_file input_name {input_name!r} is reserved")
        if not _is_nodetype(output):
            raise TypeError(f"text_file output must be a NodeType, got {output!r}")
        oname = output_name or _pascal_to_snake(output.__name__)
        recipe = (
            f"necroflow.text_file/v1:encoding={encoding}:"
            f"input={input_name}:output={oname}"
        )

        def materializer(node, log) -> None:
            node.path.write_text(node.config[input_name], encoding=encoding)

        self.register(
            name,
            Inputs(**{input_name: str}),
            Outputs(**{oname: output}),
            None,
            info=f"Write {input_name!r} to {output.__name__}.",
            recipe_identity=recipe,
            materializer=materializer,
        )

    def command(self, cmd: str | list[str], **constraints):
        """Decorator to register a rule, with the command as the decorator argument.

        Usage:
            r = Rules()

            @r.command("ln -s {path} {fastq}")
            def raw_fastq(path: str):
                "Symlink a raw FASTQ file into the output tree."
                return Fastq[fastq]

            @r.command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4)
            def align(fastq: Fastq, ref: str):
                "Align reads with BWA-MEM."
                return Bam[bam], Log[log]
        """
        repeat = constraints.pop("repeat", 1)

        def decorator(fn):
            rule_name, inputs_specs, outputs_specs, info = _parse_rule_fn(fn)
            constraints_obj = Constraints(**constraints) if constraints else None
            self.register(
                rule_name,
                Inputs(**inputs_specs),
                Outputs(**outputs_specs),
                cmd,
                constraints_obj,
                info,
                repeat,
            )
            return self.__dict__[rule_name]
        return decorator

    def rule(self, fn=None, **constraints):
        """Decorator to register a rule; command is a ``command = ...`` assignment in the body.

        Usage:
            rule = R.rule

            @rule
            def sort_bam(bam: Bam):
                "Sort BAM by coordinate."
                command = "samtools sort {bam} -o {sorted_bam}"
                return SortedBam[sorted_bam]

            @rule(threads=4)
            def align(fastq: Fastq, ref: str):
                "Align reads with BWA-MEM."
                command = "bwa mem {ref} {fastq} > {bam} 2> {log}"
                return Bam[bam], Log[log]
        """
        import ast
        import inspect
        import textwrap

        repeat = constraints.pop("repeat", 1)

        def decorator(fn):
            rule_name, inputs_specs, outputs_specs, info = _parse_rule_fn(fn)

            src = textwrap.dedent(inspect.getsource(fn))
            func_tree = ast.parse(src)
            func_def = next(
                n for n in ast.walk(func_tree)
                if isinstance(n, ast.FunctionDef) and n.name == rule_name
            )
            cmd = None
            for stmt in func_def.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and stmt.targets[0].id == 'command'
                ):
                    expr = ast.Expression(body=stmt.value)
                    ast.fix_missing_locations(expr)
                    cmd = eval(compile(expr, '<rule>', 'eval'), fn.__globals__)  # noqa: PGH001
                    break

            if cmd is None:
                raise ValueError(f"rule {rule_name!r}: no 'command = ...' found in body")

            constraints_obj = Constraints(**constraints) if constraints else None
            self.register(
                rule_name,
                Inputs(**inputs_specs),
                Outputs(**outputs_specs),
                cmd,
                constraints_obj,
                info,
                repeat,
            )
            return self.__dict__[rule_name]

        return decorator(fn) if fn is not None else decorator
