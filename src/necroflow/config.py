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
        pipeline_spec = config_dict.get(".pipeline")
        fingerprint_spec = config_dict.get(".fingerprint")
        if require_pipeline and not pipeline_spec:
            raise ValueError(f"job TOML {job_path} has no '.pipeline' key")
        request_labels = config_dict.get(".requests", None)
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
