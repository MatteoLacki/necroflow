"""Job TOML loading and grid expansion."""

from __future__ import annotations

import importlib.util
import sys
from functools import cache
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

import tomlkit

from necroflow.grid import iter_configs


@dataclass(frozen=True)
class JobConfig:
    """One concrete job config after TOML grid expansion."""

    label: str
    config: dict[str, Any]
    pipeline_spec: str | None
    fingerprint_spec: str | None
    request_labels: list[str] | None


@cache
def load_callable(spec: str, *, kind: str = "callable") -> Callable:
    """Load a user callable from a 'path.py:function_name' spec."""
    if ":" not in spec:
        raise ValueError(f"{kind} spec must be 'file.py:function_name', got {spec!r}")
    path_str, func_name = spec.rsplit(":", 1)
    path = Path(path_str).resolve()
    if not path.exists():
        raise FileNotFoundError(f"{kind} file not found: {path}")
    mod_spec = importlib.util.spec_from_file_location(
        f"_necroflow_user_{kind}_{abs(hash((path, func_name)))}",
        path,
    )
    if mod_spec is None or mod_spec.loader is None:
        raise ImportError(f"could not import {kind} file: {path}")
    module = importlib.util.module_from_spec(mod_spec)
    sys.path.insert(0, str(path.parent))
    try:
        mod_spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    if not hasattr(module, func_name):
        raise AttributeError(f"{kind} function {func_name!r} not found in {path}")
    value = getattr(module, func_name)
    if not callable(value):
        raise TypeError(f"{kind} target {func_name!r} in {path} is not callable")
    return value


def _merge_tables(base: dict[str, Any], child: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge two config tables, preferring child values."""
    merged = dict(base)
    for name, value in child.items():
        inherited = merged.get(name)
        if isinstance(inherited, dict) and isinstance(value, dict):
            value = _merge_tables(inherited, value)
        merged[name] = value
    return merged


def _get_table(config: dict[str, Any], path: str) -> dict[str, Any]:
    """Return a config table by its absolute dotted path."""
    value: Any = config
    for name in path.split("."):
        value = value[name]
    return value


def _resolve_extends(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve inheritance for top-level config tables."""
    cache: dict[str, dict[str, Any]] = {}
    resolving: list[str] = []

    def resolve(path: str) -> dict[str, Any]:
        if path in cache:
            return cache[path]
        if path in resolving:
            cycle = resolving[resolving.index(path) :] + [path]
            raise ValueError(f"config table inheritance cycle: {' -> '.join(cycle)}")
        resolving.append(path)
        value = _get_table(config, path)
        if ".extends" not in value:
            cache[path] = value
            resolving.pop()
            return value
        base_path = value[".extends"]
        if not isinstance(base_path, str) or not all(base_path.split(".")):
            raise ValueError(
                f"config table {path!r} .extends must be a dotted path string"
            )
        try:
            base_value = _get_table(config, base_path)
        except (KeyError, TypeError) as exc:
            raise ValueError(
                f"config table {path!r} extends missing table {base_path!r}"
            ) from exc
        if not isinstance(base_value, dict):
            raise ValueError(f"config table {path!r} extends non-table {base_path!r}")
        base = resolve(base_path)
        child = {name: item for name, item in value.items() if name != ".extends"}
        cache[path] = _merge_tables(base, child)
        resolving.pop()
        return cache[path]

    resolved = dict(config)
    for name, value in config.items():
        if not isinstance(value, dict) or ".extends" not in value:
            continue
        resolved[name] = resolve(name)
    return resolved


def iter_job_configs(
    path: str | Path,
    *,
    require_pipeline: bool = False,
) -> Iterator[JobConfig]:
    """Yield metadata-stripped job configs after TOML grid expansion."""
    job_path = Path(path)
    if not job_path.exists():
        raise FileNotFoundError(f"job file not found: {job_path}")
    doc = tomlkit.parse(job_path.read_text(encoding="utf-8"))
    for label, config_dict in iter_configs(doc, base_stem=job_path.stem):
        config_dict = _resolve_extends(config_dict)
        pipeline_spec = config_dict.get(".pipeline")
        fingerprint_spec = config_dict.get(".fingerprint")
        if require_pipeline and not pipeline_spec:
            raise ValueError(f"job TOML {job_path} has no '.pipeline' key")
        request_labels = config_dict.get(".requests", None)
        if request_labels is not None:
            if not isinstance(request_labels, list) or not all(
                isinstance(label, str) for label in request_labels
            ):
                raise ValueError(
                    f"job TOML {job_path} '.requests' must be a list of strings"
                )
            request_labels = list(request_labels)
        factory_config = {
            k: v for k, v in config_dict.items() if not str(k).startswith(".")
        }
        yield JobConfig(
            label=label,
            config=factory_config,
            pipeline_spec=pipeline_spec,
            fingerprint_spec=fingerprint_spec,
            request_labels=request_labels,
        )
