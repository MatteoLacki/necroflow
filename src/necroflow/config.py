"""Job TOML loading, grid expansion, and optional config validation."""
from __future__ import annotations

import importlib.util
import sys
from functools import cache
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import tomlkit

from necroflow.grid import iter_configs


Validator = Callable[[dict[str, Any]], object]


@dataclass(frozen=True)
class JobConfig:
    """One concrete job config after TOML grid expansion."""

    label: str
    config: dict[str, Any]
    pipeline_spec: str | None
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


def _load_validators(validation: Iterable[str | Validator]) -> list[Validator]:
    validators: list[Validator] = []
    for item in validation:
        if isinstance(item, str):
            validators.append(load_callable(item, kind="validation"))
        else:
            validators.append(item)
    return validators


def _run_validators(
    *,
    validators: Iterable[Validator],
    config: dict[str, Any],
    job_path: Path,
    label: str,
) -> None:
    for validator in validators:
        try:
            validator(config)
        except Exception as exc:
            name = getattr(validator, "__name__", repr(validator))
            raise ValueError(
                f"validation {name!r} failed for {job_path} [{label}]: {exc}"
            ) from exc


def iter_job_configs(
    path: str | Path,
    *,
    validation: Iterable[str | Validator] = (),
    require_pipeline: bool = False,
) -> Iterator[JobConfig]:
    """Yield expanded job configs and run optional user validators.

    Validators see the same metadata-stripped config dict that pipeline
    factories receive. They run after ``__grid`` expansion, once per concrete
    job config.
    """
    job_path = Path(path)
    if not job_path.exists():
        raise FileNotFoundError(f"job file not found: {job_path}")
    validators = _load_validators(validation)
    doc = tomlkit.parse(job_path.read_text(encoding="utf-8"))
    for label, config_dict in iter_configs(doc, base_stem=job_path.stem):
        pipeline_spec = config_dict.get(".pipeline")
        if require_pipeline and not pipeline_spec:
            raise ValueError(f"job TOML {job_path} has no '.pipeline' key")
        request_labels = config_dict.get(".requests", None)
        factory_config = {
            k: v for k, v in config_dict.items() if not str(k).startswith(".")
        }
        _run_validators(
            validators=validators,
            config=factory_config,
            job_path=job_path,
            label=label,
        )
        yield JobConfig(
            label=label,
            config=factory_config,
            pipeline_spec=pipeline_spec,
            request_labels=request_labels,
        )
