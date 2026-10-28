"""Execution-resource annotations and invocation-scoped override policy."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, TypeAlias, TypeVar, get_args, get_origin

_T = TypeVar("_T")
_RESOURCE = object()
Resource: TypeAlias = Annotated[_T, _RESOURCE]


def is_resource(annotation) -> bool:
    return get_origin(annotation) is Annotated and _RESOURCE in get_args(annotation)[1:]


def normalize_resource(name: str, value: object) -> int | str:
    """Validate an explicit request, preserving the deferred all-threads policy."""
    from necroflow.rules import parse_resource

    if not isinstance(name, str):
        raise ValueError("resource names must be strings")
    if name == "threads" and value == "all":
        return "all"
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"resource {name!r} must be a positive integer or unit string")
    try:
        result = parse_resource(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid resource {name!r}: {value!r}") from exc
    if result < 1:
        raise ValueError(f"resource {name!r} must be positive, got {value!r}")
    return result


class ResourcePolicy:
    """Override requests without mutating shared declarations or job configs."""

    def __init__(self, overrides: Mapping | None = None):
        if overrides is not None and not isinstance(overrides, Mapping):
            raise ValueError("resources must map rule names to resource tables")
        self.overrides = {}
        self.contracts = {}
        for rule, values in (overrides or {}).items():
            if not isinstance(rule, str) or not isinstance(values, Mapping):
                raise ValueError("resources must map rule names to resource tables")
            self.overrides[rule] = {
                name: normalize_resource(name, value) for name, value in values.items()
            }

    def apply(self, rule, requests: dict) -> dict:
        name = rule.__name__
        declared = frozenset(rule.constraints)
        if name in self.overrides:
            previous = self.contracts.get(name)
            if previous is not None and previous != declared:
                raise ValueError(
                    f"incompatible resource declarations for rule {name!r}"
                )
            unknown = self.overrides[name].keys() - declared
            if unknown:
                raise ValueError(
                    f"unknown resources for rule {name!r}: {sorted(unknown)}"
                )
        self.contracts[name] = declared
        return {**requests, **self.overrides.get(name, {})}

    def validate(self) -> None:
        unknown = self.overrides.keys() - self.contracts.keys()
        if unknown:
            raise ValueError(f"resource file names unknown rules: {sorted(unknown)}")
