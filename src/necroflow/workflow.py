"""Scoped Pipeline ownership for synchronous workflow functions."""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar
from functools import wraps
import inspect
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar

if TYPE_CHECKING:
    from necroflow.pipeline import Pipeline

_Params = ParamSpec("_Params")
_Return = TypeVar("_Return")
_active_pipeline: ContextVar[Pipeline | None] = ContextVar(
    "necroflow_active_pipeline", default=None
)


def workflow(
    fn: Callable[Concatenate[Pipeline, _Params], _Return],
) -> Callable[Concatenate[Pipeline, _Params], _Return]:
    """Use the first positional Pipeline as the context for rule calls.

    Workflows execute synchronously and retain their original return value.
    Labels still require explicit Pipeline assignment; finishing construction
    remains the caller's responsibility. Nested workflows restore their
    caller's context, including when a workflow raises an exception.
    """
    for check, kind in (
        (inspect.iscoroutinefunction, "coroutine"),
        (inspect.isgeneratorfunction, "generator"),
        (inspect.isasyncgenfunction, "async-generator"),
    ):
        if check(fn):
            raise TypeError(
                f"Workflow {fn.__qualname__}: only synchronous functions are "
                f"supported; {kind} functions defer execution beyond the "
                "workflow context."
            )

    @wraps(fn)
    def wrapped(*args: Any, **kwargs: Any) -> _Return:
        # Pipeline imports DAG, which imports rules; defer this import so rules
        # can retrieve the context without creating an import cycle.
        from necroflow.pipeline import Pipeline

        message = f"Workflow {fn.__qualname__}: first argument must be an open Pipeline"
        if not args:
            raise TypeError(f"{message}; none was supplied.")
        pipeline = args[0]
        if not isinstance(pipeline, Pipeline):
            raise TypeError(f"{message}; got {type(pipeline).__name__}.")
        if pipeline.finished:
            raise RuntimeError(
                f"{message}; supplied Pipeline has finished construction."
            )
        token = _active_pipeline.set(pipeline)
        try:
            return fn(*args, **kwargs)
        finally:
            _active_pipeline.reset(token)

    return wrapped
