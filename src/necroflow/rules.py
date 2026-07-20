from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable
import re
from types import UnionType
from string import Formatter
from typing import get_args, get_origin, overload

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
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _union_members(ann) -> tuple:
    return get_args(ann) if get_origin(ann) is UnionType else ()


def _is_nodetype_union(ann) -> bool:
    members = _union_members(ann)
    return bool(members) and all(_is_nodetype(member) for member in members)


def _is_node_input_contract(ann) -> bool:
    return _is_nodetype(ann) or _is_nodetype_union(ann)


def _validate_input_contracts(rule_name: str, inputs: Inputs) -> None:
    for name, ann in inputs.specs.items():
        members = _union_members(ann)
        if not members:
            continue
        has_nodetype = any(_is_nodetype(member) for member in members)
        if has_nodetype and not all(_is_nodetype(member) for member in members):
            raise TypeError(
                f"Rule {rule_name!r}: input {name!r} mixes NodeType and non-NodeType "
                "union members; use only NodeType alternatives for positional "
                "node inputs, or only plain types for config inputs"
            )


def _type_contract_name(ann) -> str:
    members = _union_members(ann)
    if members:
        return " | ".join(sorted(_type_contract_name(member) for member in members))
    return ann.__name__ if hasattr(ann, "__name__") else repr(ann)


def _matches_node_type(actual, expected) -> bool:
    members = _union_members(expected)
    if members:
        return any(_matches_node_type(actual, member) for member in members)
    return issubclass(actual, expected)


class Rule:
    """A declared rule: validates inputs and produces output Nodes when called."""

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
        _validate_input_contracts(name, inputs)
        self._pos_inputs = [
            (n, t) for n, t in inputs.specs.items() if _is_node_input_contract(t)
        ]
        self._kw_inputs = {
            n: t for n, t in inputs.specs.items() if not _is_node_input_contract(t)
        }
        reserved = BUILTIN_COMMAND_PLACEHOLDERS & (
            set(inputs.specs) | set(outputs.specs)
        )
        if reserved:
            raise ValueError(
                f"Rule {name!r}: reserved command placeholder name used as input/output: "
                f"{sorted(reserved)}"
            )
        output_names = list(outputs.specs.keys())
        self._multi = len(output_names) > 1
        self._return_type = (
            namedtuple(f"{name}_outputs", output_names) if self._multi else None
        )
        if command is not None:
            self._validate_command(name, inputs, outputs, command, self.constraints)

    @staticmethod
    def _validate_repeat(repeat: int) -> int:
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ValueError(f"repeat must be a positive integer, got {repeat!r}")
        return repeat

    @staticmethod
    def _validate_command(name, inputs, outputs, command, constraints):
        pieces = [command] if isinstance(command, str) else [str(c) for c in command]
        placeholders: set[str] = set()
        constraint_placeholders: set[str] = set()
        for piece in pieces:
            for _literal, field_name, format_spec, _conversion in Formatter().parse(
                piece
            ):
                if field_name is None:
                    continue
                if field_name == "constraint":
                    if format_spec:
                        constraint_placeholders.add(format_spec)
                    else:
                        placeholders.add(field_name)
                    continue
                # Keep the top-level field name for advanced format expressions.
                placeholders.add(field_name.split(".", 1)[0].split("[", 1)[0])
        constraint_names = set(constraints) | {"threads"}
        all_names = (
            set(inputs.specs)
            | set(outputs.specs)
            | BUILTIN_COMMAND_PLACEHOLDERS
            | constraint_names
        )
        unknown = placeholders - all_names
        unknown_constraints = constraint_placeholders - constraint_names
        missing_outputs = set(outputs.specs) - placeholders
        errors = []
        if unknown:
            errors.append(f"unknown placeholders: {sorted(unknown)}")
        if unknown_constraints:
            errors.append(
                f"unknown constraint placeholders: {sorted(unknown_constraints)}"
            )
        if missing_outputs:
            errors.append(
                f"outputs not referenced in command: {sorted(missing_outputs)}"
            )
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
            missing = [pname for pname, _ in self._pos_inputs[len(args) :]]
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
                raise TypeError(
                    f"{name}: {pname!r} expected Node, got {type(val).__name__!r}"
                )
            if val.node_type is None or not _matches_node_type(val.node_type, ptype):
                got = val.node_type.__name__ if val.node_type else "None"
                raise TypeError(
                    f"{name}: {pname!r} expected {_type_contract_name(ptype)}, got {got}"
                )
        for kname, val in kwargs.items():
            if kname not in self._kw_inputs:
                continue
            ktype = self._kw_inputs[kname]
            try:
                ok = isinstance(val, ktype)
            except TypeError:
                ok = True
            if not ok:
                raise TypeError(
                    f"{name}: {kname!r} expected {ktype}, got {type(val).__name__!r}"
                )

        parents = [a for a in args if isinstance(a, Node)]
        nodes = Node.make_outputs(
            self, parents, kwargs, self.command, self.outputs.specs
        )
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

    namespace = dict(fn.__globals__)
    namespace.update(inspect.getclosurevars(fn).nonlocals)

    def _parse_output_items(items):
        specs = {}
        for item in items:
            if isinstance(item, ast.Subscript):
                specs[item.slice.id] = namespace[item.value.id]
            elif isinstance(item, ast.Name):
                specs[_pascal_to_snake(item.id)] = namespace[item.id]
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
        if pname == "return":
            continue
        if isinstance(ann, str):
            ann = eval(ann, namespace)  # noqa: PGH001
        inputs_specs[pname] = ann

    # Prefer return Type[name] in the function body over -> annotation
    outputs_specs = {}
    body_return = None
    try:
        src = textwrap.dedent(inspect.getsource(fn))
        func_tree = ast.parse(src)
        func_def = next(
            n
            for n in ast.walk(func_tree)
            if isinstance(n, ast.FunctionDef) and n.name == rule_name
        )
        for stmt in func_def.body:
            if isinstance(stmt, ast.Return) and stmt.value is not None:
                body_return = stmt.value
                break
    except (OSError, StopIteration):
        pass

    if body_return is not None:
        items = (
            body_return.elts if isinstance(body_return, ast.Tuple) else [body_return]
        )
        outputs_specs = _parse_output_items(items)
    else:
        return_ann = raw_anns.get("return")
        if return_ann is not None:
            if not isinstance(return_ann, str):
                outputs_specs[_pascal_to_snake(return_ann.__name__)] = return_ann
            else:
                try:
                    expr_tree = ast.parse(return_ann.strip(), mode="eval")
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


def _make_rule(
    *,
    name: str,
    inputs: dict,
    outputs: dict,
    command: str | list[str] | None,
    constraints: dict | None = None,
    info: str | None = None,
    repeat: int = 1,
    recipe_identity: str | None = None,
    materializer: Callable | None = None,
) -> Rule:
    """Build a Rule from the internal dictionary representation."""
    return Rule(
        name=name,
        inputs=Inputs(**inputs),
        outputs=Outputs(**outputs),
        command=command,
        constraints=Constraints(**constraints) if constraints else None,
        info=info,
        repeat=repeat,
        recipe_identity=recipe_identity,
        materializer=materializer,
    )


def command(
    cmd: str | list[str], /, *, repeat: int = 1, **constraints
) -> Callable[[Callable], Rule]:
    """Declare a shell-command rule from a typed function signature.

    The decorated function is parsed as a declaration and replaced by the
    resulting callable Rule; its body is never executed.
    """

    def decorator(fn: Callable) -> Rule:
        rule_name, inputs, outputs, info = _parse_rule_fn(fn)
        return _make_rule(
            name=rule_name,
            inputs=inputs,
            outputs=outputs,
            command=cmd,
            constraints=constraints,
            info=info,
            repeat=repeat,
        )

    return decorator


def _validate_builtin_declaration(fn: Callable, kind: str) -> tuple:
    """Parse and validate the single-string-input, single-output built-in shape."""
    import inspect

    signature = inspect.signature(fn)
    parameters = list(signature.parameters.values())
    if len(parameters) != 1:
        raise TypeError(f"{kind} rule {fn.__name__!r} must declare exactly one input")
    parameter = parameters[0]
    if parameter.kind is not inspect.Parameter.POSITIONAL_OR_KEYWORD:
        raise TypeError(
            f"{kind} rule {fn.__name__!r} input must be a normal named parameter"
        )
    if parameter.default is not inspect.Parameter.empty:
        raise TypeError(f"{kind} rule {fn.__name__!r} input must not have a default")

    name, inputs, outputs, info = _parse_rule_fn(fn)
    if len(inputs) != 1 or next(iter(inputs.values()), None) is not str:
        raise TypeError(f"{kind} rule {name!r} input must be annotated as str")
    if len(outputs) != 1:
        raise TypeError(f"{kind} rule {name!r} must declare exactly one output")
    output_name, output = next(iter(outputs.items()))
    if not _is_nodetype(output):
        raise TypeError(
            f"{kind} rule {name!r} output must be a NodeType, got {output!r}"
        )
    return name, parameter.name, output_name, output, info


def _make_text_file_rule(
    name: str,
    output: type,
    *,
    input_name: str,
    encoding: str,
    output_name: str | None,
    info: str | None,
) -> Rule:
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

    return _make_rule(
        name=name,
        inputs={input_name: str},
        outputs={oname: output},
        command=None,
        info=info or f"Write {input_name!r} to {output.__name__}.",
        recipe_identity=recipe,
        materializer=materializer,
    )


def text_file_rule(
    name: str,
    output: type,
    *,
    input_name: str = "text",
    encoding: str = "utf-8",
    output_name: str | None = None,
) -> Rule:
    """Return a built-in rule that writes a string config value to a file."""
    return _make_text_file_rule(
        name,
        output,
        input_name=input_name,
        encoding=encoding,
        output_name=output_name,
        info=None,
    )


@overload
def text_file(fn: Callable, /) -> Rule: ...


@overload
def text_file(*, encoding: str = "utf-8") -> Callable[[Callable], Rule]: ...


def text_file(fn: Callable | None = None, /, *, encoding: str = "utf-8"):
    """Declare a built-in text-file rule, optionally selecting its encoding."""

    def decorator(declaration: Callable) -> Rule:
        name, input_name, output_name, output, info = _validate_builtin_declaration(
            declaration, "text_file"
        )
        return _make_text_file_rule(
            name,
            output,
            input_name=input_name,
            encoding=encoding,
            output_name=output_name,
            info=info,
        )

    return decorator(fn) if fn is not None else decorator


def _make_symlink_file_rule(
    name: str,
    output: type,
    *,
    path_arg: str,
    output_name: str | None,
    info: str | None,
) -> Rule:
    if path_arg in BUILTIN_COMMAND_PLACEHOLDERS:
        raise ValueError(f"symlink_file path_arg {path_arg!r} is reserved")
    if not _is_nodetype(output):
        raise TypeError(f"symlink_file output must be a NodeType, got {output!r}")
    oname = output_name or _pascal_to_snake(output.__name__)
    return _make_rule(
        name=name,
        inputs={path_arg: str},
        outputs={oname: output},
        command=f"ln -s $(realpath {{{path_arg}}}) {{{oname}}}",
        info=info or f"Symlink an external file into {output.__name__}.",
    )


def symlink_file_rule(
    name: str,
    output: type,
    *,
    path_arg: str = "path",
    output_name: str | None = None,
) -> Rule:
    """Return a rule that symlinks an external path into the output tree."""
    return _make_symlink_file_rule(
        name,
        output,
        path_arg=path_arg,
        output_name=output_name,
        info=None,
    )


def symlink_file(fn: Callable, /) -> Rule:
    """Declare a built-in external-file symlink rule."""
    name, path_arg, output_name, output, info = _validate_builtin_declaration(
        fn, "symlink_file"
    )
    return _make_symlink_file_rule(
        name,
        output,
        path_arg=path_arg,
        output_name=output_name,
        info=info,
    )
