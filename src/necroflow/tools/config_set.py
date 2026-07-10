"""Small config-file transformation helper for necroflow pipelines."""

from __future__ import annotations

import argparse
import json
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

import tomlkit

_SUPPORTED_SUFFIXES = {".json", ".toml"}


def _format_name(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        raise ValueError(
            f"unsupported config extension for {path}: expected .json or .toml"
        )
    return suffix.removeprefix(".")


def _load_config(path: Path) -> Any:
    fmt = _format_name(path)
    text = path.read_text(encoding="utf-8")
    if fmt == "json":
        return json.loads(text)
    return tomlkit.parse(text)


def _dump_config(value: Any, *, fmt: str) -> str:
    if fmt == "json":
        return json.dumps(value, indent=2, sort_keys=True) + "\n"
    return tomlkit.dumps(value)


def _split_field(field: str) -> list[str]:
    parts = [part for part in field.split(".") if part]
    if not parts:
        raise ValueError("field path must not be empty")
    return parts


def _read_field(config: Any, field: str) -> Any:
    current = config
    traversed: list[str] = []
    for part in _split_field(field):
        traversed.append(part)
        if not isinstance(current, MutableMapping) or part not in current:
            dotted = ".".join(traversed)
            raise KeyError(f"field not found: {dotted}")
        current = current[part]
    return current


def _new_child_for(parent: Any) -> MutableMapping[str, Any]:
    if parent.__class__.__module__.startswith("tomlkit"):
        return tomlkit.table()
    return {}


def _set_field(config: Any, field: str, value: Any) -> None:
    parts = _split_field(field)
    current = config
    for part in parts[:-1]:
        if not isinstance(current, MutableMapping):
            raise TypeError(f"cannot descend into non-table field: {part}")
        child = current.get(part)
        if child is None:
            child = _new_child_for(current)
            current[part] = child
        elif not isinstance(child, MutableMapping):
            raise TypeError(f"cannot descend into non-table field: {part}")
        current = child
    if not isinstance(current, MutableMapping):
        raise TypeError(f"cannot set field below non-table value: {field}")
    current[parts[-1]] = value


def copy_config_with_field(
    input_path: Path,
    output_path: Path,
    *,
    target_field: str,
    source_path: Path,
    source_field: str,
) -> None:
    input_fmt = _format_name(input_path)
    output_fmt = _format_name(output_path)
    if output_fmt != input_fmt:
        raise ValueError(
            f"output extension must match input extension: {input_path.suffix} != {output_path.suffix}"
        )

    target_config = _load_config(input_path)
    source_config = _load_config(source_path)
    value = _read_field(source_config, source_field)
    _set_field(target_config, target_field, value)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_dump_config(target_config, fmt=input_fmt), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="necroflow-config-set",
        description="Copy a TOML/JSON config and set one field from another config.",
    )
    parser.add_argument(
        "input", type=Path, help="Input config to copy (.toml or .json)."
    )
    parser.add_argument(
        "output", type=Path, help="Output config path with the same extension as input."
    )
    parser.add_argument(
        "--target",
        required=True,
        metavar="FIELD",
        help="Dotted field to set in the copied config, e.g. sage.precursor_tol.",
    )
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        metavar="PATH",
        help="Config file to read the value from (.toml or .json).",
    )
    parser.add_argument(
        "--source-field",
        required=True,
        metavar="FIELD",
        help="Dotted field to read from the source config.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        copy_config_with_field(
            args.input,
            args.output,
            target_field=args.target,
            source_path=args.source,
            source_field=args.source_field,
        )
    except Exception as exc:
        raise SystemExit(f"error: {exc}") from exc


if __name__ == "__main__":
    main()
