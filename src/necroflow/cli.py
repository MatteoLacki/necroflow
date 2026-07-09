"""necroflow CLI — run pipelines from job TOML files with __grid expansion.

Terms used in this file
-----------------------
pipeline_spec — the raw '.pipeline' string from the job TOML,
                e.g. 'pipelines/rna.py:build'.  Parsed by _load_factory into
                a file path and a function name.

factory       — a user-supplied Python function loaded from a pipeline_spec.
                Signature: factory(config: dict) -> Pipeline.

job TOML      — a TOML file describing one run: which factory to call, which
                outputs to request, and what config parameters to pass.
                Must contain a '.pipeline' key; all other keys become config.

combo         — one expanded parameter combination produced by __grid
                expansion of a job TOML.  A single job TOML with two
                __grid axes of size M×N yields M*N combos.

request       — the subset of Pipeline nodes that the DAG must produce for a
                given combo.  Defaults to the pipeline's sink nodes (leaves).
                Overridden by '.requests' in the job TOML (list of
                pipeline_label strings).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable

import tomlkit

from necroflow import DAG
from necroflow.config import iter_job_configs, load_callable
from necroflow.dag import parse_resource, resolve_paths
from necroflow.pipeline import _sinks


def _load_factory(spec: str) -> Callable:
    """Load a pipeline factory from 'path/to/file.py:function_name' (resolved from cwd)."""
    try:
        return load_callable(spec, kind="pipeline")
    except Exception as exc:
        raise SystemExit(f"error: {exc}") from exc


def _load_validators(specs: list[str]) -> list[Callable]:
    validators: list[Callable] = []
    for spec in specs:
        try:
            validators.append(load_callable(spec, kind="validation"))
        except Exception as exc:
            raise SystemExit(f"error: {exc}") from exc
    return validators


def _validate_job_config(job_config, validators: list[Callable], job_path: Path) -> None:
    for validator in validators:
        try:
            validator(job_config.config)
        except Exception as exc:
            name = getattr(validator, "__name__", repr(validator))
            raise ValueError(
                f"validation {name!r} failed for {job_path} [{job_config.label}]: {exc}"
            ) from exc


def _dedupe_preserve_order(labels: list[str]) -> list[str]:
    seen = set()
    result = []
    for label in labels:
        if label not in seen:
            seen.add(label)
            result.append(label)
    return result


def _load_reap_labels(path: Path, names: list[str]) -> list[str]:
    if not names:
        return []
    if not path.exists():
        raise SystemExit(f"error: reap file not found: {path}")
    doc = tomlkit.parse(path.read_text(encoding="utf-8"))
    labels: list[str] = []
    for name in names:
        if name not in doc:
            raise SystemExit(f"error: reap target set {name!r} not found in {path}")
        value = doc[name]
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise SystemExit(
                f"error: reap target set {name!r} must be a list of strings"
            )
        labels.extend(value)
    return labels


def _resolve_invalidation_keys(pipeline, labels: list[str]) -> set[str]:
    if not labels:
        return set()
    by_label = {n.pipeline_label: n for n in pipeline.nodes if n.pipeline_label}
    missing = [label for label in labels if label not in by_label]
    if missing:
        raise SystemExit(f"error: invalidation labels not found in pipeline: {missing}")
    return {by_label[label].key for label in labels}


def _resolve_request(pipeline, labels: list[str]) -> list:
    """Map pipeline_label strings to Node objects."""
    by_label = {n.pipeline_label: n for n in pipeline.nodes if n.pipeline_label}
    missing = [l for l in labels if l not in by_label]
    if missing:
        raise SystemExit(f"error: request labels not found in pipeline: {missing}")
    return [by_label[l] for l in labels]


def _create_link_outputs(
    results_dir: Path,
    combos: list[tuple[str, object, list]],
    *,
    nodes_dir: Path | None = None,
) -> None:
    """Create per-combo symlink dirs and manifests under results_dir/{label}/.

    Only requested (sink) outputs get a symlink — ancestors are excluded.
    """
    root = nodes_dir if nodes_dir is not None else results_dir
    for label, pipeline, sink_nodes in combos:
        combo_dir = results_dir / label

        for node in sink_nodes:
            if node.path is None or not node.path.exists():
                continue
            rel = node.path.relative_to(root)
            link = combo_dir / rel
            link.parent.mkdir(parents=True, exist_ok=True)
            if link.is_symlink() or link.exists():
                link.unlink()
            link.symlink_to(Path(os.path.relpath(node.path, link.parent)))

        manifest_lines = ["[outputs]\n"]
        for node in sink_nodes:
            if node.path is not None and node.path.exists():
                rel = node.path.relative_to(root)
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
        "--nodes-dir",
        default=None,
        type=Path,
        metavar="DIR",
        help="Directory for hashed node outputs (default: nodes).",
    )
    parser.add_argument(
        "--results-dir",
        default=None,
        type=Path,
        metavar="DIR",
        help="Directory for per-job symlink outputs (default: results).",
    )
    parser.add_argument(
        "--outdir", "-o",
        default=None,
        type=Path,
        metavar="DIR",
        help="Compatibility alias for using one directory for node outputs and results.",
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
    parser.add_argument(
        "--invalidate",
        action="append",
        default=[],
        metavar="LABEL",
        help="Force an already-requested pipeline label to rerun. Repeatable.",
    )
    parser.add_argument(
        "--reap",
        action="append",
        default=[],
        metavar="NAME",
        help="Force labels from NAME in reap.toml to rerun. Repeatable.",
    )
    parser.add_argument(
        "--reap-file",
        default=Path("reap.toml"),
        type=Path,
        metavar="PATH",
        help="TOML file containing named invalidation label sets (default: reap.toml).",
    )
    parser.add_argument(
        "--validation",
        action="append",
        default=[],
        metavar="PATH.py:FUNCTION",
        help="Validate each expanded job config with a Python callable. Repeatable.",
    )

    args = parser.parse_args(argv)
    if args.outdir is not None and (args.nodes_dir is not None or args.results_dir is not None):
        raise SystemExit("error: --outdir cannot be combined with --nodes-dir or --results-dir")
    if args.outdir is not None:
        nodes_dir = args.outdir
        results_dir = args.outdir
    else:
        nodes_dir = args.nodes_dir if args.nodes_dir is not None else Path("nodes")
        results_dir = args.results_dir if args.results_dir is not None else Path("results")

    invalidation_labels = _dedupe_preserve_order(
        list(args.invalidate) + _load_reap_labels(args.reap_file, args.reap)
    )

    validators = _load_validators(args.validation) if args.validation else []

    dag = DAG(nodes_dir)
    combos: list[tuple[str, object, list]] = []
    forced_stale_keys: set[str] = set()

    for job_path_str in args.jobs:
        job_path = Path(job_path_str)
        try:
            job_configs = iter_job_configs(job_path, require_pipeline=True)
            for job_config in job_configs:
                if not job_config.pipeline_spec:
                    raise SystemExit(
                        f"error: job TOML {job_path} has no '.pipeline' key"
                    )
                if validators:
                    _validate_job_config(job_config, validators, job_path)
                factory = _load_factory(job_config.pipeline_spec)
                P = factory(job_config.config)
                request = (
                    _resolve_request(P, job_config.request_labels)
                    if job_config.request_labels is not None
                    else _sinks(P)
                )
                forced_stale_keys.update(
                    _resolve_invalidation_keys(P, invalidation_labels)
                )
                dag.add(P, request=request)
                combos.append((job_config.label, P, request))
        except SystemExit:
            raise
        except Exception as exc:
            raise SystemExit(f"error: {exc}") from exc

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
        forced_stale_keys=forced_stale_keys,
    )

    if args.dry_run:
        return

    for _label, pipeline, _nodes in combos:
        resolve_paths(pipeline.nodes, nodes_dir)
    _create_link_outputs(results_dir, combos, nodes_dir=nodes_dir)


if __name__ == "__main__":
    main()
