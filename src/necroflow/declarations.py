"""Parse rule declarations without executing their bodies."""

from __future__ import annotations

import ast
import inspect
import textwrap
from dataclasses import dataclass
from typing import Any, cast, get_args

from necroflow.command_builders import NestedCommand
from necroflow.resources import is_resource, resource_value


@dataclass(frozen=True)
class Declaration:
    name: str
    inputs: dict
    outputs: dict
    resources: dict
    resource_types: dict
    info: str | None
    command: Any = None


def is_inline_declaration(fn) -> bool:
    """Distinguish bare declarations from legacy one-argument callbacks."""
    if not inspect.isfunction(fn) or fn.__name__ == "<lambda>":
        return False
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        body = next(n.body for n in tree.body if isinstance(n, ast.FunctionDef))
    except (OSError, TypeError, SyntaxError, StopIteration):
        return False
    # A callback may itself use a local variable called command. Only an
    # actual output declaration distinguishes bare @command from that callback.
    from necroflow.rules import outputs

    namespace = {**fn.__globals__, **inspect.getclosurevars(fn).nonlocals}
    return any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and namespace.get(n.func.id) is outputs
        for statement in body
        for n in ast.walk(statement)
    )


def parse_declaration(fn, *, inline=False) -> Declaration:
    """Parse a typed rule signature and assignment-based output declarations."""
    import builtins

    from necroflow.rules import outputs
    from necroflow.nodes import _is_nodetype

    namespace = dict(fn.__globals__)
    namespace.update(inspect.getclosurevars(fn).nonlocals)
    rule_name = fn.__name__
    info = fn.__doc__.strip() if fn.__doc__ else None
    missing_annotations = [
        name
        for name, parameter in inspect.signature(fn).parameters.items()
        if parameter.annotation is inspect.Parameter.empty
    ]
    if missing_annotations:
        raise TypeError(
            f"rule {rule_name!r}: inputs missing type annotations: "
            f"{missing_annotations!r}"
        )

    for parameter in inspect.signature(fn).parameters.values():
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise TypeError(
                f"rule {rule_name!r}: *args and **kwargs declarations are unsupported; use explicit parameters and tuple-valued inputs"
            )

    raw_anns = fn.__annotations__
    inputs_specs = {}
    resources = {}
    resource_types = {}
    for pname, ann in raw_anns.items():
        if pname == "return":
            continue
        if isinstance(ann, str):
            ann = eval(ann, namespace)  # noqa: PGH001
        if is_resource(ann):
            kind = get_args(ann)[0]
            if kind not in (int, float, str):
                raise TypeError(
                    f"rule {rule_name!r}: Resource supports int, float, or str quantities"
                )
            if pname == "threads" and kind is not int:
                raise TypeError("threads must use Resource[int]")
            resource_types[pname] = kind
            default = inspect.signature(fn).parameters[pname].default
            if default is inspect.Parameter.empty:
                raise TypeError(
                    f"rule {rule_name!r}: resource {pname!r} needs a default"
                )
            resources[pname] = resource_value(pname, default, kind)
        else:
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

    command_node = None
    if inline:
        if len(body) < 3:
            raise ValueError(
                f"rule {rule_name!r}: declare outputs, command, then return"
            )
        command_node = body[-2]
        body = [*body[:-2], body[-1]]
    declarations = {}
    for stmt in body[:-1]:
        if not (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], (ast.Name, ast.Tuple))
            and isinstance(stmt.value, ast.Call)
            and isinstance(stmt.value.func, ast.Name)
            and namespace.get(stmt.value.func.id) is outputs
        ):
            raise ValueError(
                f"rule {rule_name!r}: body may contain only "
                "name = outputs(Node) declarations before the final return"
            )
        target = stmt.targets[0]
        targets = target.elts if isinstance(target, ast.Tuple) else [target]
        if (
            not stmt.value.args
            or stmt.value.keywords
            or len(targets) != len(stmt.value.args)
            or not all(isinstance(item, ast.Name) for item in targets)
            or (isinstance(target, ast.Tuple) and len(targets) == 1)
        ):
            raise ValueError(
                f"rule {rule_name!r}: outputs() requires one name per positional Node; "
                "use name = outputs(Type) or a, b = outputs(A, B)"
            )
        for target_name, type_expr in zip(targets, stmt.value.args):
            if not isinstance(type_expr, ast.Name):
                raise ValueError(
                    f"rule {rule_name!r}: outputs() argument must be a concrete Node name"
                )
            try:
                output_type = namespace[type_expr.id]
            except KeyError:
                try:
                    output_type = getattr(builtins, type_expr.id)
                except AttributeError as exc:
                    raise ValueError(
                        f"rule {rule_name!r}: outputs() argument must be a concrete Node name"
                    ) from exc
            output_name = target_name.id
            if not _is_nodetype(output_type):
                raise TypeError(
                    f"rule {rule_name!r}: output {output_name!r} must be a "
                    f"Node, got {output_type!r}"
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
            "name = outputs(Type) followed by return name"
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
    if inline and "command" in {*inputs_specs, *outputs_specs, *resources}:
        raise ValueError(
            f"rule {rule_name!r}: command is reserved in inline declarations"
        )
    if inline and (
        set(inputs_specs) & set(outputs_specs) or set(resources) & set(outputs_specs)
    ):
        raise ValueError(f"rule {rule_name!r}: input/resource and output names overlap")
    cmd = None
    if inline:
        if (
            isinstance(command_node, ast.Assign)
            and len(command_node.targets) == 1
            and isinstance(command_node.targets[0], ast.Name)
            and command_node.targets[0].id == "command"
            and isinstance(command_node.value, ast.Constant)
            and isinstance(command_node.value.value, str)
        ):
            cmd = command_node.value.value
        elif (
            isinstance(command_node, ast.FunctionDef) and command_node.name == "command"
        ):
            cmd = NestedCommand.create(
                fn, command_node, [*inputs_specs, *resources, *outputs_specs]
            )
        else:
            raise ValueError(
                f"rule {rule_name!r}: command must be a literal string or zero-argument def command() after outputs"
            )
    return Declaration(
        rule_name, inputs_specs, outputs_specs, resources, resource_types, info, cmd
    )
