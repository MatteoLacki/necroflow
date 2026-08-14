from __future__ import annotations

import ast
from collections.abc import Mapping, Sequence, Set
from datetime import date, datetime, time
import hashlib
import inspect
from pathlib import Path
import platform
import sys
import textwrap
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, Callable, get_args, get_origin, Union

if TYPE_CHECKING:
    from necroflow.rule_call import RuleCall

# Stamped into .rip/dependencies.toml by write_dependencies and required back by
# any reader of stored provenance, and carried in both hash domains so a format
# bump cannot leave the stored marker and the hashes disagreeing.
IDENTITY_FORMAT = "v4"
RULE_HASH_DOMAIN = f"necroflow.rule-hash/{IDENTITY_FORMAT}"
PROVENANCE_HASH_DOMAIN = f"necroflow.provenance-hash/{IDENTITY_FORMAT}"


class FingerprintValueError(TypeError):
    """Raised when the default fingerprint cannot encode a Python value."""


def python_identity() -> str:
    """Return the implementation and exact version for callback identity."""
    version = sys.version_info
    return f"{platform.python_implementation()}-{version.major}.{version.minor}.{version.micro}"


def _frame(tag: bytes, payloads: Sequence[bytes]) -> bytes:
    """Encode a tagged sequence with unambiguous length boundaries."""
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
        "use a supported deterministic configuration value"
    )


def _type_name(annotation: Any) -> str:
    """Render a deterministic type identity for fingerprint input metadata."""
    origin = get_origin(annotation)
    if origin in (UnionType, Union):
        return "|".join(sorted(_type_name(member) for member in get_args(annotation)))
    if origin is Annotated:
        base, *metadata = get_args(annotation)
        metadata_names = []
        for value in metadata:
            value_type = type(value)
            type_name = f"{value_type.__module__}.{value_type.__qualname__}"
            metadata_names.append(f"{type_name}:{value!r}")
        return f"typing.Annotated[{_type_name(base)},{','.join(metadata_names)}]"
    if origin is tuple:
        members = get_args(annotation)
        if len(members) == 2 and members[1] is Ellipsis:
            return f"builtins.tuple[{_type_name(members[0])},...]"
    module = getattr(annotation, "__module__", "")
    qualname = getattr(annotation, "__qualname__", None)
    if qualname is not None:
        return f"{module}.{qualname}" if module else qualname
    return repr(annotation)


def _unwrapped_function(callback: Callable) -> Callable:
    """Return an inspectable module-level callback with no captured state."""
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
    """Require a source-inspectable callback with one CommandArgs parameter."""
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
    """Return the canonical identity payload for one rule recipe."""
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


def _parent_identity(call: RuleCall) -> list[dict[str, Any]]:
    parents = []
    for name, parent in call.inputs.items():
        if isinstance(parent, tuple):
            parents.append(
                {
                    "name": name,
                    "group": [
                        {
                            "provenance_hash": item.provenance_hash,
                            "output": item.output_name or "",
                        }
                        for item in parent
                    ],
                }
            )
        else:
            parents.append(
                {
                    "name": name,
                    "provenance_hash": parent.provenance_hash,
                    "output": parent.output_name or "",
                }
            )
    return parents


def _rule_identity(
    *, rule_name, command, recipe_identity, mutable, input_types, output_types
) -> dict[str, Any]:
    return {
        "domain": RULE_HASH_DOMAIN,
        "rule": rule_name,
        "command": _command_identity(command, recipe_identity),
        "mutable": mutable,
        "input_types": {
            name: _type_name(annotation) for name, annotation in input_types.items()
        },
        "output_types": {
            name: {
                "type": _type_name(annotation),
                "filename": annotation.filename,
            }
            for name, annotation in output_types.items()
        },
    }


def hash_rule_identity(identity: Mapping[str, Any]) -> str:
    """Hash one canonical local recipe description."""

    return hashlib.sha256(canonical_bytes(identity, path="rule")).hexdigest()


def declared_rule_hash(rule) -> str:
    """Hash a module-level Rule without constructing a configured call."""

    return hash_rule_identity(
        _rule_identity(
            rule_name=rule.__name__,
            command=rule.command,
            recipe_identity=rule.recipe_identity,
            mutable=rule.mutable,
            input_types=rule.inputs.specs,
            output_types=rule.outputs.specs,
        )
    )


def provenance_hash(call: RuleCall, local_rule_hash: str) -> str:
    """Hash one configured invocation and its exact parent lineage."""

    identity = {
        "domain": PROVENANCE_HASH_DOMAIN,
        "rule_hash": local_rule_hash,
        "config": call.config,
        "execution_context": (
            {"shellpath": call.shellpath} if call.shellpath is not None else {}
        ),
        "parents": _parent_identity(call),
    }
    return hashlib.sha256(canonical_bytes(identity, path="provenance")).hexdigest()


def compute_identity(call: RuleCall) -> tuple[dict[str, Any], str, str]:
    """Return the framework-owned v4 recipe and its two identity hashes."""

    local_rule_identity = _rule_identity(
        rule_name=call.rule.__name__,
        command=call.command,
        recipe_identity=call.rule.recipe_identity,
        mutable=call.mutable,
        input_types=call.rule.inputs.specs,
        output_types=call.rule.outputs.specs,
    )
    local_rule_hash = hash_rule_identity(local_rule_identity)
    return (
        local_rule_identity,
        local_rule_hash,
        provenance_hash(call, local_rule_hash),
    )
