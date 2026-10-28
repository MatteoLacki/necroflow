"""Execution-resource annotations and invocation-scoped override policy."""

from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Annotated, TypeAlias, TypeVar, get_args, get_origin

_T = TypeVar("_T")
_RESOURCE = object()
Resource: TypeAlias = Annotated[_T, _RESOURCE]


def is_resource(annotation) -> bool:
    return get_origin(annotation) is Annotated and _RESOURCE in get_args(annotation)[1:]


def normalize_resource(name: str, value: object) -> int | float | str:
    """Validate an explicit request, preserving the deferred all-threads policy."""
    from necroflow.rules import parse_resource

    if not isinstance(name, str):
        raise ValueError("resource names must be strings")
    if name == "threads" and value == "all":
        return "all"
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(
            f"resource {name!r} must be a positive integer, float, or unit string"
        )
    try:
        result = parse_resource(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid resource {name!r}: {value!r}") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"resource {name!r} must be positive, got {value!r}")
    if name == "threads" and not isinstance(result, int):
        raise ValueError("threads must be a positive integer or 'all'")
    return result


def resource_value(name: str, value: object, kind: type | None = None):
    """Validate a quantity while retaining its declared command-facing type."""
    amount = normalize_resource(name, value)
    if amount == "all":
        return amount
    if kind is str:
        if not isinstance(value, str):
            raise TypeError(f"resource {name!r} expected str")
        return value
    if kind is int and not isinstance(amount, int):
        raise TypeError(f"resource {name!r} expected an integer quantity")
    return float(amount) if kind is float else amount


def memory_bytes(value: str | int) -> int:
    """Convert an integer byte count or SI/binary size such as '16G' or '16Gi'."""
    amount = normalize_resource("memory", value)
    if not isinstance(amount, int):
        raise ValueError("memory size must resolve to an integer byte count")
    return amount


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
            for name, value in values.items():
                normalize_resource(name, value)
            self.overrides[rule] = dict(values)

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
        return {
            key: (
                resource_value(key, value, rule.resource_types.get(key))
                if key in rule.resource_types
                else value
            )
            for key, value in {**requests, **self.overrides.get(name, {})}.items()
        }

    def validate(self) -> None:
        unknown = self.overrides.keys() - self.contracts.keys()
        if unknown:
            raise ValueError(f"resource file names unknown rules: {sorted(unknown)}")
