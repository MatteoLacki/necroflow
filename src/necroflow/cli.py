"""necroflow CLI — run pipelines from job TOML files with __grid expansion."""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path
from typing import Callable

import tomlkit

from necroflow import DAG
from necroflow.dag import parse_resource, resolve_paths
from necroflow.grid import iter_configs
from necroflow.pipeline import _sinks


_factory_cache: dict[tuple[str, str], Callable] = {}


def _load_factory(spec: str) -> Callable:
    """Load a pipeline factory from 'path/to/file.py:function_name' (resolved from cwd)."""
    if ":" not in spec:
        raise SystemExit(
            f"error: '.pipeline' value must be 'file.py:function_name', got {spec!r}"
        )
    path_str, func_name = spec.rsplit(":", 1)
    path = Path(path_str).resolve()
    cache_key = (str(path), func_name)
    if cache_key in _factory_cache:
        return _factory_cache[cache_key]
    if not path.exists():
        raise SystemExit(f"error: pipeline file not found: {path}")
    mod_spec = importlib.util.spec_from_file_location("_necroflow_user_pipeline", path)
    module = importlib.util.module_from_spec(mod_spec)
    sys.path.insert(0, str(path.parent))
    try:
        mod_spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    if not hasattr(module, func_name):
        raise SystemExit(f"error: function {func_name!r} not found in {path}")
    factory = getattr(module, func_name)
    _factory_cache[cache_key] = factory
    return factory


def _resolve_request(pipeline, labels: list[str]) -> list:
    """Map pipeline_label strings to Node objects."""
    by_label = {n.pipeline_label: n for n in pipeline.nodes if n.pipeline_label}
    missing = [l for l in labels if l not in by_label]
    if missing:
        raise SystemExit(f"error: request labels not found in pipeline: {missing}")
    return [by_label[l] for l in labels]


def _create_link_outputs(
    outdir: Path,
    combos: list[tuple[str, object, list]],
) -> None:
    """Create per-combo symlink dirs and manifests under outdir/{label}/.

    Only requested (sink) outputs get a symlink — ancestors are excluded.
    """
    for label, pipeline, sink_nodes in combos:
        combo_dir = outdir / label

        for node in sink_nodes:
            if node.path is None or not node.path.exists():
                continue
            rel = node.path.relative_to(outdir)
            link = combo_dir / rel
            link.parent.mkdir(parents=True, exist_ok=True)
            if link.is_symlink() or link.exists():
                link.unlink()
            link.symlink_to(Path(os.path.relpath(node.path, link.parent)))

        manifest_lines = ["[outputs]\n"]
        for node in sink_nodes:
            if node.path is not None and node.path.exists():
                rel = node.path.relative_to(outdir)
                key = (
                    node.pipeline_label
                    or node.output_name
                    or (node.node_type.__name__ if node.node_type else "output")
                )
                manifest_lines.append(f'{key} = "{rel.as_posix()}"\n')
        combo_dir.mkdir(parents=True, exist_ok=True)
        (combo_dir / "manifest.toml").write_text("".join(manifest_lines), encoding="utf-8")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="necroflow",
        description=(
            "Run necroflow pipelines from job TOML files. "
            "Each TOML must contain a 'pipeline' key ('file.py:function'). "
            "Keys ending in __grid are expanded as a parameter grid."
        ),
    )
    parser.add_argument(
        "jobs",
        nargs="+",
        metavar="JOB.toml",
        help="Job TOML file(s). Each defines a pipeline, optional request, and config params.",
    )
    parser.add_argument(
        "--outdir", "-o",
        required=True,
        type=Path,
        metavar="DIR",
        help="Output directory.",
    )
    parser.add_argument(
        "-c",
        dest="cores",
        default="all",
        metavar="N|all",
        help="Thread cap: integer or 'all' (default: all available CPUs). E.g. -c16 or -call.",
    )
    parser.add_argument(
        "--constraint",
        action="append",
        default=[],
        dest="constraints",
        metavar="KEY=VALUE",
        help="Resource cap, e.g. --constraint ram=300Mi. Repeatable. Overrides -c for threads.",
    )
    parser.add_argument(
        "--keep-going", "-k",
        action="store_true",
        help="Continue past failures and collect all errors at the end.",
    )
    parser.add_argument(
        "--autoclean",
        action="store_true",
        help="Delete orphan outputs before execution and intermediates as soon as they are no longer needed.",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        dest="dry_run",
        help="Show what would run without executing anything.",
    )

    args = parser.parse_args(argv)

    dag = DAG(args.outdir)
    combos: list[tuple[str, object, list]] = []

    for job_path_str in args.jobs:
        job_path = Path(job_path_str)
        if not job_path.exists():
            raise SystemExit(f"error: job file not found: {job_path}")
        doc = tomlkit.parse(job_path.read_text(encoding="utf-8"))
        for label, config_dict in iter_configs(doc, base_stem=job_path.stem):
            pipeline_spec = config_dict.get(".pipeline")
            if not pipeline_spec:
                raise SystemExit(
                    f"error: job TOML {job_path} has no '.pipeline' key"
                )
            factory = _load_factory(pipeline_spec)
            request_labels = config_dict.get(".requests", None)
            factory_config = {k: v for k, v in config_dict.items() if not k.startswith(".")}
            P = factory(factory_config)
            request = _resolve_request(P, request_labels) if request_labels is not None else _sinks(P)
            dag.add(P, request=request)
            combos.append((label, P, request))

    cores = args.cores.strip()
    resource_caps = {"threads": os.cpu_count() or 1 if cores.lower() == "all" else int(cores)}
    for kv in args.constraints:
        if "=" not in kv:
            raise SystemExit(f"error: --constraint expects KEY=VALUE, got {kv!r}")
        k, v = kv.split("=", 1)
        resource_caps[k.strip()] = parse_resource(v.strip())

    dag.execute(
        resource_caps=resource_caps,
        keep_going=args.keep_going,
        autoclean=args.autoclean,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        return

    for _label, pipeline, _nodes in combos:
        resolve_paths(pipeline.nodes, args.outdir)
    _create_link_outputs(args.outdir, combos)


if __name__ == "__main__":
    main()
