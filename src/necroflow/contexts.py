from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Generic, TypeVar

_T = TypeVar("_T")


class NamedValues(Mapping[str, _T], Generic[_T]):
    """An immutable named mapping with optional attribute access.

    Mapping methods win when a declared name collides with the Mapping API;
    bracket access remains available for every declared name.
    """

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, _T] | None = None, /, **kwargs: _T):
        data = dict(values or {})
        data.update(kwargs)
        object.__setattr__(self, "_values", MappingProxyType(data))

    def __getitem__(self, name: str) -> _T:
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getattr__(self, name: str) -> _T:
        try:
            return self._values[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __repr__(self) -> str:
        values = ", ".join(f"{k}={v!r}" for k, v in self._values.items())
        return f"NamedValues({values})"


@dataclass(frozen=True)
class CommandArgs:
    """Resolved values supplied to a Python command callback."""

    inputs: NamedValues[Path]
    config: NamedValues[Any]
    outputs: NamedValues[Path]
    constraints: NamedValues[Any]
    workdir: Path


@dataclass(frozen=True)
class FingerprintArgs:
    """Logical rule-call values available before output path resolution."""

    rule_name: str
    command: str | Callable[[CommandArgs], str] | None
    inputs: NamedValues[Any]
    config: NamedValues[Any]
    input_types: NamedValues[Any]
    output_types: NamedValues[Any]
    constraints: NamedValues[Any]
    execution_context: NamedValues[Any]
    repeat: int
    recipe_identity: str | None
