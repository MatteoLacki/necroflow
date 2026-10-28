"""Source-backed adapters for nested command builders."""

from __future__ import annotations

import ast
import copy
import inspect
from dataclasses import dataclass
from pathlib import Path
from types import FunctionType


@dataclass(frozen=True)
class NestedCommand:
    factory: FunctionType
    names: tuple[str, ...]
    tree: str
    source_path: Path
    __qualname__: str

    @classmethod
    def create(cls, declaration, node: ast.FunctionDef, names: list[str]):
        if (
            node.args.posonlyargs
            or node.args.args
            or node.args.kwonlyargs
            or node.args.vararg
            or node.args.kwarg
            or node.decorator_list
        ):
            raise TypeError(
                "nested command must be an undecorated zero-argument function"
            )
        if any(
            isinstance(n, (ast.Yield, ast.YieldFrom, ast.Await, ast.Nonlocal))
            for n in ast.walk(node)
        ):
            raise TypeError(
                "nested command must be synchronous, non-generator, and not assign nonlocals"
            )
        captured = inspect.getclosurevars(declaration).nonlocals
        used = {
            n.id
            for n in ast.walk(node)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
        }
        hidden = used & (captured.keys() - set(names))
        if hidden:
            raise TypeError(
                f"nested command captures undeclared values: {sorted(hidden)}"
            )
        source = Path(inspect.getsourcefile(declaration)).resolve()
        tree = ast.dump(node, annotate_fields=True, include_attributes=False)
        factory_node = ast.FunctionDef(
            name="_build_command",
            args=ast.arguments(
                posonlyargs=[],
                args=[],
                vararg=None,
                kwonlyargs=[ast.arg(arg=name) for name in names],
                kw_defaults=[None] * len(names),
                kwarg=None,
                defaults=[],
            ),
            body=[
                ast.increment_lineno(
                    copy.deepcopy(node), inspect.getsourcelines(declaration)[1] - 1
                ),
                ast.Return(
                    value=ast.Call(
                        func=ast.Name(id="command", ctx=ast.Load()),
                        args=[],
                        keywords=[],
                    )
                ),
            ],
            decorator_list=[],
        )
        module = ast.fix_missing_locations(
            ast.Module(body=[factory_node], type_ignores=[])
        )
        namespace = {}
        exec(compile(module, str(source), "exec"), declaration.__globals__, namespace)
        return cls(
            namespace["_build_command"],
            tuple(names),
            tree,
            source,
            declaration.__qualname__ + ".command",
        )

    def __call__(self, args):
        values = {
            **dict(args.inputs),
            **dict(args.config),
            **dict(args.constraints),
            **dict(args.outputs),
        }
        return self.factory(**{name: values[name] for name in self.names})
