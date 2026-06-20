from __future__ import annotations

import importlib
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterable

from necroflow import Pipeline


@dataclass(frozen=True)
class PipelineConfig:
    id: str
    label: str
    values: Any


@dataclass(frozen=True)
class PipelineSpec:
    id: str
    label: str
    rules: Any
    build: Callable[[Any, Any], Pipeline]
    configs: tuple[PipelineConfig, ...]
    outdir: Path | str


def load_pipeline_specs(target: str | None = None) -> list[PipelineSpec]:
    """Load pipeline specs from module[:attribute], file.py[:attribute], or examples."""
    if not target:
        from necroflow_gui.example_registry import PIPELINES

        return _validate_specs(PIPELINES)

    module_ref, attr = _split_target(target)
    module = _load_module(module_ref)
    specs = getattr(module, attr)
    return _validate_specs(specs)


def _split_target(target: str) -> tuple[str, str]:
    if ":" in target:
        module_ref, attr = target.split(":", 1)
        return module_ref, attr
    return target, "PIPELINES"


def _load_module(module_ref: str) -> ModuleType:
    path = Path(module_ref)
    if path.suffix == ".py" or path.exists():
        module_path = path.resolve()
        spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Cannot import pipeline module from {module_ref!r}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return importlib.import_module(module_ref)


def _validate_specs(specs: Iterable[PipelineSpec]) -> list[PipelineSpec]:
    result = list(specs)
    seen: set[str] = set()
    for spec in result:
        if not isinstance(spec, PipelineSpec):
            raise TypeError("PIPELINES must contain necroflow_gui.registry.PipelineSpec objects")
        if spec.id in seen:
            raise ValueError(f"Duplicate pipeline id {spec.id!r}")
        seen.add(spec.id)
        if not spec.configs:
            raise ValueError(f"Pipeline {spec.id!r} must define at least one config")
    return result
