"""Scoped execution of typed command declarations."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Callable

_current_build: ContextVar[Construction | None] = ContextVar(
    "current_build", default=None
)


def current_build():
    build = _current_build.get()
    if build is None:
        raise RuntimeError(
            "outputs() and Shell() require an active @command construction"
        )
    return build


class Shell:
    """Register this invocation's shell template; placeholders are rendered later."""

    def __init__(self, template: str):
        build = current_build()
        if not isinstance(template, str) or not template.strip():
            raise TypeError("Shell requires a non-empty shell string")
        if build.shell is not None:
            raise ValueError("exactly one Shell is allowed per command invocation")
        self.template = template
        build.shell = self


@dataclass(frozen=True)
class Executable:
    function: Callable
    groups: tuple[tuple[str, ...], ...]
    tree: str


class Construction:
    def __init__(self, recipe, call):
        self.recipe = recipe
        self.call = call
        self.index = 0
        self.shell = None

    def outputs(self, types):
        if self.index >= len(self.recipe.groups):
            raise ValueError("unexpected outputs() call")
        names = self.recipe.groups[self.index]
        nodes = tuple(self.call.output_nodes[name] for name in names)
        if tuple(type(node) for node in nodes) != types:
            raise TypeError("outputs() types do not match the declared output group")
        self.index += 1
        return nodes[0] if len(nodes) == 1 else nodes

    def execute(self, bound):
        token = _current_build.set(self)
        try:
            result = self.recipe.function(*bound.args, **bound.kwargs)
            expected = tuple(self.call.output_nodes.values())
            actual = result if isinstance(result, tuple) else (result,)
            if (
                self.index != len(self.recipe.groups)
                or len(actual) != len(expected)
                or any(a is not b for a, b in zip(actual, expected))
            ):
                raise ValueError(
                    "return must contain exactly the declared output artifacts in order"
                )
            if self.shell is None:
                raise ValueError("command invocation must register exactly one Shell")
            return self.shell.template
        finally:
            _current_build.reset(token)
