from __future__ import annotations

import ast
from collections.abc import Mapping, Sequence, Set
from datetime import date, datetime, time
import hashlib
import inspect
from pathlib import Path
import platform
import re
import sys
import textwrap
from types import UnionType
from typing import Any, Callable, get_args, get_origin

from necroflow.contexts import FingerprintArgs

FINGERPRINT_DOMAIN = "necroflow.fingerprint/v2"
DEFAULT_FINGERPRINT_PROVIDER = "necroflow.default_fingerprint/v2"
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}")


class FingerprintValueError(TypeError):
    """Raised when the default fingerprint cannot encode a Python value."""


def python_identity() -> str:
    version = sys.version_info
    return f"{platform.python_implementation()}-{version.major}.{version.minor}.{version.micro}"


def _frame(tag: bytes, payloads: Sequence[bytes]) -> bytes:
    result = bytearray(tag)
    result.extend(len(payloads).to_bytes(8, "big"))
    for payload in payloads:
        result.extend(len(payload).to_bytes(8, "big"))
        result.extend(payload)
    return bytes(result)


def canonical_bytes(value: Any, *, path: str = "value") -> bytes:
    """Encode supported values deterministically with types and boundaries."""

    if value is None:
        return _frame(b"none", ())
    if isinstance(value, bool):
        return _frame(b"bool", (b"1" if value else b"0",))
    if isinstance(value, int):
        return _frame(b"int", (str(value).encode(),))
    if isinstance(value, float):
        if value != value:
            encoded = b"nan"
        elif value == float("inf"):
            encoded = b"inf"
        elif value == float("-inf"):
            encoded = b"-inf"
        else:
            encoded = value.hex().encode()
        return _frame(b"float", (encoded,))
    if isinstance(value, str):
        return _frame(b"str", (value.encode("utf-8"),))
    if isinstance(value, bytes):
        return _frame(b"bytes", (value,))
    if isinstance(value, Path):
        return _frame(b"path", (str(value).encode("utf-8"),))
    if isinstance(value, datetime):
        return _frame(b"datetime", (value.isoformat().encode(),))
    if isinstance(value, date):
        return _frame(b"date", (value.isoformat().encode(),))
    if isinstance(value, time):
        return _frame(b"time", (value.isoformat().encode(),))
    if isinstance(value, Mapping):
        entries = []
        keys = list(value)
        for key in keys:
            if not isinstance(key, str):
                raise FingerprintValueError(
                    f"{path}: fingerprint mappings require string keys, got {type(key).__name__}"
                )
        for key in sorted(keys):
            entries.append(
                _frame(
                    b"entry",
                    (
                        canonical_bytes(key, path=f"{path}.<key>"),
                        canonical_bytes(value[key], path=f"{path}.{key}"),
                    ),
                )
            )
        return _frame(b"mapping", entries)
    if isinstance(value, tuple):
        return _frame(
            b"tuple",
            [
                canonical_bytes(item, path=f"{path}[{i}]")
                for i, item in enumerate(value)
            ],
        )
    if isinstance(value, list):
        return _frame(
            b"list",
            [
                canonical_bytes(item, path=f"{path}[{i}]")
                for i, item in enumerate(value)
            ],
        )
    if isinstance(value, (set, frozenset)):
        items = sorted(canonical_bytes(item, path=f"{path}[]") for item in value)
        return _frame(b"frozenset" if isinstance(value, frozenset) else b"set", items)
    raise FingerprintValueError(
        f"{path}: unsupported fingerprint value {type(value).__name__}; "
        "configure a project fingerprint function to define its identity"
    )


def _type_name(annotation: Any) -> str:
    if get_origin(annotation) is UnionType:
        return "|".join(sorted(_type_name(member) for member in get_args(annotation)))
    module = getattr(annotation, "__module__", "")
    qualname = getattr(annotation, "__qualname__", None)
    if qualname is not None:
        return f"{module}.{qualname}" if module else qualname
    return repr(annotation)


def _unwrapped_function(callback: Callable) -> Callable:
    callback = inspect.unwrap(callback)
    if not inspect.isfunction(callback):
        raise TypeError(
            "Python command callbacks must be source-inspectable functions or lambdas, "
            f"got {type(callback).__name__}"
        )
    if callback.__closure__ or callback.__code__.co_freevars:
        captured = sorted(callback.__code__.co_freevars)
        raise TypeError(
            f"Python command callback {callback.__qualname__!r} must not close over "
            f"values; captured names: {captured}"
        )
    if "<locals>" in callback.__qualname__:
        raise TypeError(
            f"Python command callback {callback.__qualname__!r} must be defined at module scope"
        )
    return callback


def command_ast(callback: Callable) -> tuple[str, Path]:
    """Return a canonical AST dump and defining source path for a command callback."""

    callback = _unwrapped_function(callback)
    try:
        lines, start_line = inspect.getsourcelines(callback)
        source_path = inspect.getsourcefile(callback)
    except (OSError, TypeError) as exc:
        raise TypeError(
            f"Python command callback {callback.__qualname__!r} has no inspectable source"
        ) from exc
    if source_path is None:
        raise TypeError(
            f"Python command callback {callback.__qualname__!r} has no source file"
        )
    source = textwrap.dedent("".join(lines))
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise TypeError(
            f"cannot parse source for Python command callback {callback.__qualname__!r}"
        ) from exc

    if callback.__name__ == "<lambda>":
        absolute_line = callback.__code__.co_firstlineno
        candidates = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Lambda)
            and start_line + node.lineno - 1 == absolute_line
        ]
        if len(candidates) != 1:
            raise TypeError(
                f"Python command lambda at {source_path}:{absolute_line} is ambiguous; "
                "place it on its own source line"
            )
        selected: ast.AST = candidates[0]
    else:
        candidates = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == callback.__name__
        ]
        if len(candidates) != 1:
            raise TypeError(
                f"cannot uniquely locate AST for Python command callback {callback.__qualname__!r}"
            )
        selected = candidates[0]
    return (
        ast.dump(selected, annotate_fields=True, include_attributes=False),
        Path(source_path).resolve(),
    )


def validate_command_callback(callback: Callable) -> None:
    callback = _unwrapped_function(callback)
    parameters = list(inspect.signature(callback).parameters.values())
    if len(parameters) != 1 or parameters[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise TypeError(
            f"Python command callback {callback.__qualname__!r} must accept exactly "
            "one positional CommandArgs argument"
        )
    command_ast(callback)


def _command_identity(command: Any, recipe_identity: str | None) -> Any:
    if recipe_identity is not None:
        return {"kind": "recipe", "identity": recipe_identity}
    if command is None:
        return {"kind": "none"}
    if isinstance(command, str):
        return {"kind": "shell", "template": command}
    if callable(command):
        tree, _source_path = command_ast(command)
        return {
            "kind": "python",
            "python": python_identity(),
            "ast": tree,
        }
    raise TypeError(f"unsupported command identity {type(command).__name__}")


def default_fingerprint(args: FingerprintArgs) -> str:
    """Compute Necroflow's complete version-2 rule-call fingerprint."""

    parents = []
    for name, parent in args.inputs.items():
        parents.append(
            {
                "name": name,
                "fingerprint": parent.fingerprint,
                "output": parent.output_name or "",
            }
        )
    identity = {
        "domain": FINGERPRINT_DOMAIN,
        "rule": args.rule_name,
        "command": _command_identity(args.command, args.recipe_identity),
        "config": dict(args.config.items()),
        # Preserve the v2 wire shape while exposing shellpath directly in the API.
        "execution_context": (
            {"shellpath": args.shellpath} if args.shellpath is not None else {}
        ),
        "parents": parents,
        "input_types": {
            name: _type_name(annotation)
            for name, annotation in args.input_types.items()
        },
        "output_types": {
            name: _type_name(annotation)
            for name, annotation in args.output_types.items()
        },
    }
    return hashlib.sha256(canonical_bytes(identity, path="fingerprint")).hexdigest()


def validate_fingerprint_result(value: Any, *, provider: str) -> str:
    if not isinstance(value, str) or _HEX_DIGEST.fullmatch(value) is None:
        raise TypeError(
            f"fingerprint function {provider!r} must return exactly 64 lowercase "
            f"hexadecimal characters, got {value!r}"
        )
    return value


def validate_fingerprint_function(function: Callable, *, provider: str) -> None:
    parameters = list(inspect.signature(function).parameters.values())
    if len(parameters) != 1 or parameters[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise TypeError(
            f"fingerprint function {provider!r} must accept exactly one positional "
            "FingerprintArgs argument"
        )
