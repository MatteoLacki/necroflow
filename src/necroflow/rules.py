from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import re
from types import UnionType
from string import Formatter
from typing import (
    Annotated,
    Any,
    Generic,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    overload,
)

from necroflow.contexts import NamedValues
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
        """Store named declarations in their insertion order."""
        self.specs = specs


class Outputs:
    """Declare rule outputs by name: Outputs(bam=Bam, log=Log)."""

    def __init__(self, **specs):
        """Store named declarations in their insertion order."""
        self.specs = specs


class Constraints:
    """Declare scheduler constraints: Constraints(threads=4, ram="250Mi")."""

    def __init__(self, **kwargs):
        """Store named scheduler constraints in declaration order."""
        self.specs = kwargs


@dataclass(frozen=True)
class Many:
    """Set inclusive size bounds for a variadic Node input."""

    min: int = 1
    max: int | None = None

    def __post_init__(self) -> None:
        """Reject non-integral, negative, or contradictory bounds."""
        if isinstance(self.min, bool) or not isinstance(self.min, int) or self.min < 0:
            raise ValueError(
                f"Many.min must be a non-negative integer, got {self.min!r}"
            )
        if self.max is not None and (
            isinstance(self.max, bool) or not isinstance(self.max, int)
        ):
            raise ValueError(f"Many.max must be an integer or None, got {self.max!r}")
        if self.max is not None and self.max < self.min:
            raise ValueError(
                f"Many.max must be greater than or equal to min, got "
                f"min={self.min!r}, max={self.max!r}"
            )


@dataclass(frozen=True)
class _NodeInputContract:
    element_type: Any
    variadic: bool = False
    many: Many | None = None


def _pascal_to_snake(name: str) -> str:
    """Convert a NodeType class name into its default output name."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _union_members(ann) -> tuple:
    """Return members of either supported union spelling, or an empty tuple."""
    return get_args(ann) if get_origin(ann) in (UnionType, Union) else ()


def _is_nodetype_union(ann) -> bool:
    """Return whether an annotation is a non-empty all-NodeType union."""
    members = _union_members(ann)
    return bool(members) and all(_is_nodetype(member) for member in members)


def _node_input_contract(
    rule_name: str, input_name: str, annotation: Any
) -> _NodeInputContract | None:
    """Classify an input annotation as a positional Node contract or config.

    Fixed NodeTypes and all-NodeType unions produce scalar contracts.
    ``tuple[NodeType, ...]`` produces a variadic contract, optionally bounded
    by one ``Many`` item in ``Annotated`` metadata. Invalid or ambiguous Node
    declarations raise immediately; ordinary config annotations return
    ``None`` and are therefore handled as keyword inputs by ``Rule``.
    """
    base = annotation
    metadata: tuple[Any, ...] = ()
    if get_origin(annotation) is Annotated:
        base, *metadata_values = get_args(annotation)
        metadata = tuple(metadata_values)

    many_values = [value for value in metadata if isinstance(value, Many)]
    if len(many_values) > 1:
        raise TypeError(
            f"Rule {rule_name!r}: input {input_name!r} has more than one Many marker"
        )
    many = many_values[0] if many_values else None

    members = _union_members(base)
    if members:
        has_nodetype = any(_is_nodetype(member) for member in members)
        if has_nodetype and not all(_is_nodetype(member) for member in members):
            raise TypeError(
                f"Rule {rule_name!r}: input {input_name!r} mixes NodeType and "
                "non-NodeType union members; use only NodeType alternatives for "
                "positional node inputs, or only plain types for config inputs"
            )

    if get_origin(base) is tuple:
        tuple_args = get_args(base)
        if len(tuple_args) == 2 and tuple_args[1] is Ellipsis:
            element_type = tuple_args[0]
            element_members = _union_members(element_type)
            if element_members:
                has_nodetype = any(_is_nodetype(member) for member in element_members)
                if has_nodetype and not all(
                    _is_nodetype(member) for member in element_members
                ):
                    raise TypeError(
                        f"Rule {rule_name!r}: input {input_name!r} mixes "
                        "NodeType and non-NodeType tuple element union members"
                    )
            if _is_nodetype(element_type) or _is_nodetype_union(element_type):
                return _NodeInputContract(element_type, variadic=True, many=many)
            if many is not None:
                raise TypeError(
                    f"Rule {rule_name!r}: input {input_name!r} uses Many with "
                    "a tuple whose element type is not a NodeType"
                )
            return None
        if many is not None or any(
            _is_nodetype(item) or _is_nodetype_union(item) for item in tuple_args
        ):
            raise TypeError(
                f"Rule {rule_name!r}: input {input_name!r} uses a fixed-length "
                "Node tuple; use tuple[NodeType, ...]"
            )
        return None

    if many is not None:
        raise TypeError(
            f"Rule {rule_name!r}: input {input_name!r} uses Many on a non-variadic "
            "Node tuple"
        )
    if _is_nodetype(base) or _is_nodetype_union(base):
        return _NodeInputContract(base)
    return None


def _type_contract_name(ann) -> str:
    """Render a NodeType contract for validation error messages."""
    members = _union_members(ann)
    if members:
        return " | ".join(sorted(_type_contract_name(member) for member in members))
    return ann.__name__ if hasattr(ann, "__name__") else repr(ann)


def _matches_node_type(actual, expected) -> bool:
    """Return whether an actual NodeType satisfies a type or union contract."""
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
        input_defaults: Mapping[str, Any] | None = None,
    ):
        """Validate and store a rule declaration and derive its call schema."""
        self.__name__ = name
        self.inputs = inputs
        self._validate_outputs(name, outputs)
        self.outputs = outputs
        self.command = command
        self.recipe_identity = recipe_identity
        self.materializer = materializer
        self.constraints = constraints.specs if constraints else {}
        self.repeat = self._validate_repeat(repeat)
        self.info = info
        contracts = {
            input_name: _node_input_contract(name, input_name, annotation)
            for input_name, annotation in inputs.specs.items()
        }
        self._pos_inputs = [
            (input_name, contract)
            for input_name, contract in contracts.items()
            if contract is not None
        ]
        self._kw_inputs = {
            input_name: inputs.specs[input_name]
            for input_name, contract in contracts.items()
            if contract is None
        }
        self._input_defaults = self._validated_input_defaults(input_defaults, contracts)
        self._validate_config_values(self._input_defaults)
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
                f"Rule {name!r}: argv list commands are unsupported; "
                "use a shell string or a Python callback returning a shell string"
            )
        if callable(command):
            validate_command_callback(command)
        elif command is not None:
            self._validate_command(name, inputs, outputs, command, self.constraints)

    @staticmethod
    def _validate_outputs(name: str, outputs: Outputs) -> None:
        """Require concrete NodeTypes with explicit filenames for every output."""
        for output_name, output_type in outputs.specs.items():
            if not _is_nodetype(output_type):
                raise TypeError(
                    f"Rule {name!r}: output {output_name!r} must be a NodeType, "
                    f"got {output_type!r}"
                )
            if output_type.filename is None:
                raise TypeError(
                    f"Rule {name!r}: output {output_name!r} NodeType "
                    f"{output_type.__name__} must define filename"
                )
            if not isinstance(output_type.mutable, bool):
                raise TypeError(
                    f"Rule {name!r}: output {output_name!r} NodeType "
                    f"{output_type.__name__}.mutable must be bool, "
                    f"got {type(output_type.mutable).__name__}"
                )

    def _validated_input_defaults(
        self,
        input_defaults: Mapping[str, Any] | None,
        contracts: dict[str, _NodeInputContract | None],
    ) -> dict[str, Any]:
        """Copy defaults and require them to name scalar/config inputs."""
        if input_defaults is None:
            return {}
        if not isinstance(input_defaults, Mapping):
            raise TypeError(f"Rule {self.__name__!r}: input_defaults must be a mapping")
        defaults = dict(input_defaults)
        unknown = [name for name in defaults if name not in contracts]
        if unknown:
            raise TypeError(
                f"Rule {self.__name__!r}: unknown input defaults: "
                f"{sorted(unknown, key=repr)!r}"
            )
        for name in defaults:
            if contracts[name] is not None:
                raise TypeError(
                    f"Rule {self.__name__!r}: Node input {name!r} "
                    "must not have a default"
                )
        return defaults

    @staticmethod
    def _validate_repeat(repeat: int) -> int:
        """Return a valid positive attempt count or raise immediately."""
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ValueError(f"repeat must be a positive integer, got {repeat!r}")
        return repeat

    @staticmethod
    def _validate_command(name, inputs, outputs, command, constraints):
        """Ensure every static-command placeholder has a declared source."""
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
        """Return integer scheduler resources with one thread by default."""
        result = {k: parse_resource(v) for k, v in self.constraints.items()}
        result.setdefault("threads", 1)
        return result

    def _validate_pipeline(self, pipeline) -> None:
        """Require the positional owner to be a Pipeline."""
        from necroflow.pipeline import Pipeline

        if not isinstance(pipeline, Pipeline):
            raise TypeError(
                f"{self.__name__}: first argument must be the owning Pipeline, "
                f"got {type(pipeline).__name__}"
            )

    def _validate_input_presence(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        """Require exactly the declared Nodes and all normalized config keys."""
        name = self.__name__
        if len(args) < len(self._pos_inputs):
            missing = [pname for pname, _ in self._pos_inputs[len(args) :]]
            raise TypeError(f"{name}: missing required inputs: {missing!r}")
        if len(args) > len(self._pos_inputs):
            raise TypeError(
                f"{name}: too many positional inputs: "
                f"expected {len(self._pos_inputs)}, got {len(args)}"
            )
        missing_kw = [kname for kname in self._kw_inputs if kname not in kwargs]
        if missing_kw:
            raise TypeError(f"{name}: missing required inputs: {missing_kw!r}")

    def _validate_parent_nodes(self, pipeline, args: tuple[Any, ...]) -> None:
        """Validate Node containers, bounds, types, order, and DAG ownership."""
        name = self.__name__
        for (pname, contract), value in zip(self._pos_inputs, args):
            values: tuple[Any, ...]
            if contract.variadic:
                if not isinstance(value, tuple):
                    raise TypeError(
                        f"{name}: {pname!r} expected tuple of Nodes, "
                        f"got {type(value).__name__!r}"
                    )
                values = value
                minimum = contract.many.min if contract.many is not None else 0
                maximum = contract.many.max if contract.many is not None else None
                if len(values) < minimum or (
                    maximum is not None and len(values) > maximum
                ):
                    upper = "unbounded" if maximum is None else str(maximum)
                    raise ValueError(
                        f"{name}: {pname!r} expected between {minimum} and {upper} "
                        f"Nodes, got {len(values)}"
                    )
            else:
                values = (value,)

            for index, parent in enumerate(values):
                position = f"{pname}[{index}]" if contract.variadic else pname
                if not isinstance(parent, Node):
                    raise TypeError(
                        f"{name}: {position!r} expected Node, "
                        f"got {type(parent).__name__!r}"
                    )
                if not _matches_node_type(parent.node_type, contract.element_type):
                    got = parent.node_type.__name__ if parent.node_type else "None"
                    raise TypeError(
                        f"{name}: {position!r} expected "
                        f"{_type_contract_name(contract.element_type)}, got {got}"
                    )
                if parent.rule_call.dag is not pipeline.dag:
                    raise ValueError(f"{name}: {position!r} belongs to a different DAG")

    def _validate_config_values(self, config: dict[str, Any]) -> None:
        """Check supplied config values against runtime-checkable annotations."""
        name = self.__name__
        for key, value in config.items():
            if key not in self._kw_inputs:
                continue
            expected = self._kw_inputs[key]
            try:
                valid = isinstance(value, expected)
            except TypeError:
                valid = True
            if not valid:
                raise TypeError(
                    f"{name}: {key!r} expected {expected}, "
                    f"got {type(value).__name__!r}"
                )

    def _effective_config(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Return declared defaults overlaid with explicit call values."""
        config = dict(self._input_defaults)
        config.update(kwargs)
        return config

    def _compile_outputs(
        self, pipeline, args: tuple[Any, ...], config: dict[str, Any]
    ) -> list[Node]:
        """Compile and intern output Nodes from validated logical inputs."""
        node_inputs = NamedValues(
            {name: value for (name, _contract), value in zip(self._pos_inputs, args)}
        )
        return Node.make_outputs(
            pipeline, self, node_inputs, config, self.command, self.outputs.specs
        )

    def _shape_outputs(self, nodes: list[Node]) -> _ReturnT:
        """Return one Node or the rule-specific named tuple of co-outputs."""
        if self._multi:
            assert self._return_type is not None
            value = self._return_type(*nodes)
        else:
            value = nodes[0]
        return cast(_ReturnT, value)

    def __call__(self, pipeline, /, *args: Any, **kwargs: Any) -> _ReturnT:
        """Validate one invocation and return its canonical output Nodes."""
        self._validate_pipeline(pipeline)
        pipeline._assert_open()
        config = self._effective_config(kwargs)
        self._validate_input_presence(args, config)
        self._validate_parent_nodes(pipeline, args)
        self._validate_config_values(config)
        nodes = self._compile_outputs(pipeline, args, config)
        return self._shape_outputs(nodes)


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
    returned_names = [cast(ast.Name, item).id for item in items]
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


def _decorated_input_defaults(fn: Callable, input_names: set[str]) -> dict[str, Any]:
    """Return Python defaults belonging to annotated rule inputs."""
    import inspect

    parameters = inspect.signature(fn).parameters
    return {
        name: parameter.default
        for name, parameter in parameters.items()
        if name in input_names and parameter.default is not inspect.Parameter.empty
    }


def command(
    cmd: str | Callable,
    *declarations,
    name: str | None = None,
    doc: str | None = None,
    repeat: int = 1,
    **constraints,
):
    """Create a factory rule or return the decorator-sugar adapter.

    ``repeat`` is the maximum number of command attempts, including the first.
    Decorated scalar/config defaults come from the Python signature. Factory
    rules may declare them with ``input_defaults={name: value}``.
    """
    if isinstance(cmd, list):
        raise TypeError(
            "argv list commands are unsupported; use a shell "
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
        input_defaults = constraints.pop("input_defaults", None)
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
        return Rule[Any](
            name=name,
            inputs=inputs,
            outputs=outputs,
            command=cmd,
            constraints=factory_constraints,
            info=doc,
            repeat=repeat,
            input_defaults=input_defaults,
        )
    if name is not None or doc is not None:
        raise TypeError("name= and doc= are only valid for factory commands")

    def decorator(fn: Callable[..., _ReturnT]) -> Rule[_ReturnT]:
        """Build a Rule from a decorated declaration and captured command policy."""
        rule_name, inputs, outputs, info = _parse_rule_fn(fn)
        input_defaults = _decorated_input_defaults(fn, set(inputs))
        return cast(
            Rule[_ReturnT],
            Rule[Any](
                name=rule_name,
                inputs=Inputs(**inputs),
                outputs=Outputs(**outputs),
                command=cmd,
                constraints=Constraints(**constraints) if constraints else None,
                info=info,
                repeat=repeat,
                input_defaults=input_defaults,
            ),
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
) -> Rule[Node]:
    """Build the internal materializer-backed text-file Rule."""
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
        """Write the configured text value to the compiled output path."""
        node.path.write_text(node.config[input_name], encoding=encoding)

    return Rule(
        name=name,
        inputs=Inputs(**{input_name: str}),
        outputs=Outputs(**{oname: output}),
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
def text_file(fn: Callable[..., _ReturnT], /) -> Rule[_ReturnT]:
    """Type signature for direct ``@text_file`` decorator use."""
    ...


@overload
def text_file(
    *, encoding: str = "utf-8"
) -> Callable[[Callable[..., _ReturnT]], Rule[_ReturnT]]:
    """Type signature for configured ``@text_file(...)`` decorator use."""
    ...


def text_file(  # pyright: ignore[reportInconsistentOverload]
    fn: Callable | None = None, *, encoding: str = "utf-8"
):
    """Declare a built-in text-file rule, optionally selecting its encoding."""

    def decorator(declaration: Callable) -> Rule:
        """Convert one validated text-file declaration into a Rule."""
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
    """Build the internal shell-backed external-file symlink Rule."""
    if path_arg in BUILTIN_COMMAND_PLACEHOLDERS:
        raise ValueError(f"symlink_file path_arg {path_arg!r} is reserved")
    if not _is_nodetype(output):
        raise TypeError(f"symlink_file output must be a NodeType, got {output!r}")
    oname = output_name or _pascal_to_snake(output.__name__)
    return Rule(
        name=name,
        inputs=Inputs(**{path_arg: str}),
        outputs=Outputs(**{oname: output}),
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
