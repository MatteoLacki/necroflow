"""necroflow CLI — run pipelines from TOML configs with __grid expansion."""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path
from typing import Callable

import tomlkit

from necroflow import DAG
from necroflow.dag import resolve_paths
from necroflow.grid import iter_configs
from necroflow.pipeline import _sinks


def _load_factory(spec: str) -> Callable:
    """Load a pipeline factory from 'path/to/file.py:function_name'."""
    if ":" not in spec:
        raise SystemExit(
            f"error: --pipeline must be 'file.py:function_name', got {spec!r}"
        )
    path_str, func_name = spec.rsplit(":", 1)
    path = Path(path_str).resolve()
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
    return getattr(module, func_name)


def _create_link_outputs(
    outdir: Path,
    combos: list[tuple[str, list, list]],
) -> None:
    """Create per-combo symlink trees and manifests under outdir/{label}/."""
    for label, pipeline_nodes, sink_nodes in combos:
        combo_dir = outdir / label

        for node in pipeline_nodes:
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
                    node.output_name
                    or (node.node_type.__name__ if node.node_type else "output")
                )
                manifest_lines.append(f'{key} = "{rel.as_posix()}"\n')
        combo_dir.mkdir(parents=True, exist_ok=True)
        (combo_dir / "manifest.toml").write_text("".join(manifest_lines), encoding="utf-8")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="necroflow",
        description=(
            "Run a necroflow pipeline from one or more TOML configs. "
            "Keys ending in __grid are expanded as a parameter grid."
        ),
    )
    parser.add_argument(
        "--pipeline", "-p",
        required=True,
        metavar="FILE:FUNCTION",
        help="Pipeline factory — path/to/file.py:function_name. "
             "The function receives a plain dict and must return a Pipeline.",
    )
    parser.add_argument(
        "--config", "-c",
        action="append",
        required=True,
        metavar="PATH",
        dest="configs",
        help="TOML config file. May be repeated for multiple configs.",
    )
    parser.add_argument(
        "--outdir", "-o",
        required=True,
        type=Path,
        metavar="DIR",
        help="Output directory.",
    )
    parser.add_argument(
        "--threads", "-t",
        type=int,
        default=None,
        metavar="N",
        help="Maximum parallel threads (default: cpu count).",
    )
    parser.add_argument(
        "--keep-going", "-k",
        action="store_true",
        help="Continue past failures and collect all errors at the end.",
    )
    parser.add_argument(
        "--link-outputs",
        action="store_true",
        help=(
            "After execution, create outdir/{combo_label}/ with symlinks "
            "mirroring the hash folder structure and a manifest.toml listing "
            "sink output paths."
        ),
    )

    args = parser.parse_args(argv)
    factory = _load_factory(args.pipeline)

    dag = DAG(args.outdir)
    # (label, all pipeline nodes, sink nodes)
    combos: list[tuple[str, list, list]] = []

    for config_path_str in args.configs:
        config_path = Path(config_path_str)
        if not config_path.exists():
            raise SystemExit(f"error: config file not found: {config_path}")
        doc = tomlkit.parse(config_path.read_text(encoding="utf-8"))
        for label, config_dict in iter_configs(doc, base_stem=config_path.stem):
            P = factory(config_dict)
            sinks = _sinks(P)
            dag.add(P)
            combos.append((label, list(P.nodes), sinks))

    dag.execute(total_threads=args.threads, keep_going=args.keep_going)

    if args.link_outputs:
        # resolve paths on each pipeline's own node objects; nodes that were
        # deduplicated in the DAG may not have had paths set during execute()
        for _label, pipeline_nodes, _sinks_nodes in combos:
            resolve_paths(pipeline_nodes, args.outdir)
        _create_link_outputs(args.outdir, combos)


if __name__ == "__main__":
    main()
