from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable
import re
from types import UnionType
from string import Formatter
from typing import Any, Generic, TypeVar, cast, get_args, get_origin, overload

from necroflow.nodes import Node, NodeType, _is_nodetype
from necroflow.fingerprints import validate_command_callback

BUILTIN_COMMAND_PLACEHOLDERS = {"workdir"}

_ReturnT = TypeVar("_ReturnT")

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


def output(node_type: type[NodeType]) -> Node:
    """Declare an output in a decorated rule body.

    The decorator parses ``name = output(NodeType)`` assignments without
    executing them.  The ``Node`` return annotation gives static analyzers the
    real value shape produced when the resulting rule is called.
    """
    raise RuntimeError(
        "output() is declaration-only; use it as name = output(NodeType) "
        "inside a decorated rule declaration"
    )


class Rule(Generic[_ReturnT]):
    """A declared rule: validates inputs and produces output Nodes when called."""

    def __init__(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | Callable | None,
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
        if isinstance(command, list):
            raise TypeError(
                f"Rule {name!r}: argv list commands were removed in fingerprint v2; "
                "use a shell string or a Python callback returning a shell string"
            )
        if callable(command):
            validate_command_callback(command)
        elif command is not None:
            self._validate_command(name, inputs, outputs, command, self.constraints)

    @staticmethod
    def _validate_repeat(repeat: int) -> int:
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ValueError(f"repeat must be a positive integer, got {repeat!r}")
        return repeat

    @staticmethod
    def _validate_command(name, inputs, outputs, command, constraints):
        pieces = [command]
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
        errors = []
        if unknown:
            errors.append(f"unknown placeholders: {sorted(unknown)}")
        if unknown_constraints:
            errors.append(
                f"unknown constraint placeholders: {sorted(unknown_constraints)}"
            )
        if errors:
            raise ValueError(f"Rule {name!r}: " + "; ".join(errors))

    @property
    def resources(self) -> dict[str, int]:
        result = {k: parse_resource(v) for k, v in self.constraints.items()}
        result.setdefault("threads", 1)
        return result

    def __call__(self, *args: Any, **kwargs: Any) -> _ReturnT:
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
        value = self._return_type(*nodes) if self._multi else nodes[0]
        return cast(_ReturnT, value)


def _parse_rule_fn(fn) -> tuple:
    """Parse a typed rule signature and assignment-based output declarations."""
    import ast
    import builtins
    import inspect
    import textwrap

    namespace = dict(fn.__globals__)
    namespace.update(inspect.getclosurevars(fn).nonlocals)
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

    try:
        src = textwrap.dedent(inspect.getsource(fn))
        func_tree = ast.parse(src)
        func_def = next(
            node
            for node in ast.walk(func_tree)
            if isinstance(node, ast.FunctionDef) and node.name == rule_name
        )
    except (OSError, StopIteration) as exc:
        raise ValueError(
            f"rule {rule_name!r}: cannot inspect declaration source"
        ) from exc

    body = list(func_def.body)
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body = body[1:]

    if not body or not isinstance(body[-1], ast.Return) or body[-1].value is None:
        raise ValueError(
            f"rule {rule_name!r}: declaration must end with return output_name"
        )

    declarations = {}
    for stmt in body[:-1]:
        if not (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
            and isinstance(stmt.value, ast.Call)
            and isinstance(stmt.value.func, ast.Name)
            and namespace.get(stmt.value.func.id) is output
        ):
            raise ValueError(
                f"rule {rule_name!r}: body may contain only "
                "name = output(NodeType) declarations before the final return"
            )
        if len(stmt.value.args) != 1 or stmt.value.keywords:
            raise ValueError(
                f"rule {rule_name!r}: output() requires exactly one positional NodeType"
            )
        type_expr = stmt.value.args[0]
        if not isinstance(type_expr, ast.Name):
            raise ValueError(
                f"rule {rule_name!r}: output() argument must be a concrete NodeType name"
            )
        try:
            output_type = namespace[type_expr.id]
        except KeyError:
            try:
                output_type = getattr(builtins, type_expr.id)
            except AttributeError as exc:
                raise ValueError(
                    f"rule {rule_name!r}: output() argument must be a concrete NodeType name"
                ) from exc
        output_name = stmt.targets[0].id
        if not _is_nodetype(output_type):
            raise TypeError(
                f"rule {rule_name!r}: output {output_name!r} must be a "
                f"NodeType, got {output_type!r}"
            )
        if output_name in declarations:
            raise ValueError(
                f"rule {rule_name!r}: duplicate output declaration {output_name!r}"
            )
        declarations[output_name] = output_type

    body_return = body[-1].value
    if isinstance(body_return, ast.Subscript):
        raise ValueError(
            f"rule {rule_name!r}: Type[name] output syntax was removed; use "
            "name = output(Type) followed by return name"
        )
    items = body_return.elts if isinstance(body_return, ast.Tuple) else [body_return]
    if not items or not all(isinstance(item, ast.Name) for item in items):
        raise ValueError(
            f"rule {rule_name!r}: final return must contain only declared output names"
        )
    returned_names = [item.id for item in items]
    if len(returned_names) != len(set(returned_names)):
        raise ValueError(
            f"rule {rule_name!r}: each output must be returned exactly once"
        )
    undeclared = [name for name in returned_names if name not in declarations]
    unused = [name for name in declarations if name not in returned_names]
    if undeclared or unused:
        details = []
        if undeclared:
            details.append(f"undeclared outputs returned: {undeclared}")
        if unused:
            details.append(f"declared outputs not returned: {unused}")
        raise ValueError(f"rule {rule_name!r}: " + "; ".join(details))

    outputs_specs = {name: declarations[name] for name in returned_names}
    return rule_name, inputs_specs, outputs_specs, info


def _make_rule(
    *,
    name: str,
    inputs: dict,
    outputs: dict,
    command: str | Callable | None,
    constraints: dict | None = None,
    info: str | None = None,
    repeat: int = 1,
    recipe_identity: str | None = None,
    materializer: Callable | None = None,
) -> Rule[Any]:
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


def _decorator_command(
    cmd: str | Callable, /, *, repeat: int = 1, **constraints
) -> Callable[[Callable[..., _ReturnT]], Rule[_ReturnT]]:
    """Declare a shell-command rule from a typed function signature.

    The function body is declaration-only: assign each output with
    ``name = output(NodeType)``, then return those names in output order.
    """

    def decorator(fn: Callable[..., _ReturnT]) -> Rule[_ReturnT]:
        rule_name, inputs, outputs, info = _parse_rule_fn(fn)
        return cast(
            Rule[_ReturnT],
            _make_rule(
                name=rule_name,
                inputs=inputs,
                outputs=outputs,
                command=cmd,
                constraints=constraints,
                info=info,
                repeat=repeat,
            ),
        )

    return decorator


def command(
    cmd: str | Callable,
    *declarations,
    name: str | None = None,
    doc: str | None = None,
    repeat: int = 1,
    **constraints,
):
    """Create a factory rule or return the decorator-sugar adapter."""
    if isinstance(cmd, list):
        raise TypeError(
            "argv list commands were removed in fingerprint v2; use a shell "
            "string or a Python callback returning a shell string"
        )
    if not isinstance(cmd, str) and not callable(cmd):
        raise TypeError(
            f"command requires a shell string or Python callback, got {type(cmd).__name__}"
        )
    if declarations:
        if len(declarations) not in (2, 3):
            raise TypeError(
                "factory command requires Inputs, Outputs, and optional Constraints"
            )
        if name is None:
            raise TypeError("factory command requires an explicit name=")
        if constraints:
            raise TypeError(
                "factory command declarations cannot use constraint keywords"
            )
        inputs, outputs = declarations[:2]
        factory_constraints = declarations[2] if len(declarations) == 3 else None
        if not isinstance(inputs, Inputs) or not isinstance(outputs, Outputs):
            raise TypeError("factory command requires Inputs and Outputs declarations")
        if factory_constraints is not None and not isinstance(
            factory_constraints, Constraints
        ):
            raise TypeError("factory command constraints must be a Constraints object")
        return _make_rule(
            name=name,
            inputs=inputs.specs,
            outputs=outputs.specs,
            command=cmd,
            constraints=factory_constraints.specs if factory_constraints else None,
            info=doc,
            repeat=repeat,
        )
    if name is not None or doc is not None:
        raise TypeError("name= and doc= are only valid for factory commands")
    return _decorator_command(cmd, repeat=repeat, **constraints)


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
) -> Rule[Node]:
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
) -> Rule[Node]:
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
def text_file(fn: Callable[..., _ReturnT], /) -> Rule[_ReturnT]: ...


@overload
def text_file(
    *, encoding: str = "utf-8"
) -> Callable[[Callable[..., _ReturnT]], Rule[_ReturnT]]: ...


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
) -> Rule[Node]:
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
) -> Rule[Node]:
    """Return a rule that symlinks an external path into the output tree."""
    return _make_symlink_file_rule(
        name,
        output,
        path_arg=path_arg,
        output_name=output_name,
        info=None,
    )


def symlink_file(fn: Callable[..., _ReturnT], /) -> Rule[_ReturnT]:
    """Declare a built-in external-file symlink rule."""
    name, path_arg, output_name, output_type, info = _validate_builtin_declaration(
        fn, "symlink_file"
    )
    return cast(
        Rule[_ReturnT],
        _make_symlink_file_rule(
            name,
            output_type,
            path_arg=path_arg,
            output_name=output_name,
            info=info,
        ),
    )
