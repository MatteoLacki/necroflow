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
import shutil
from importlib import resources
from pathlib import Path
from typing import Callable

import tomlkit

from necroflow import DAG
from necroflow.config import iter_job_configs, load_callable
from necroflow.dag import parse_resource, resolve_paths
from necroflow.pipeline import _sinks
from necroflow.executor import _apply_shell_execution_context, _normalize_shellpath


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


def _resolve_roots(args) -> tuple[Path, Path]:
    if args.outdir is not None and (args.nodes_dir is not None or args.results_dir is not None):
        raise SystemExit("error: --outdir cannot be combined with --nodes-dir or --results-dir")
    if args.outdir is not None:
        return args.outdir, args.outdir
    return (
        args.nodes_dir if args.nodes_dir is not None else Path("nodes"),
        args.results_dir if args.results_dir is not None else Path("results"),
    )


def _build_dag_from_jobs(args, *, nodes_dir: Path):
    invalidation_labels = _dedupe_preserve_order(
        list(getattr(args, "invalidate", []))
        + _load_reap_labels(getattr(args, "reap_file", Path("reap.toml")), getattr(args, "reap", []))
    )
    validation_specs = getattr(args, "validation", [])
    validators = _load_validators(validation_specs) if validation_specs else []

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
                pipeline = factory(job_config.config)
                request = (
                    _resolve_request(pipeline, job_config.request_labels)
                    if job_config.request_labels is not None
                    else _sinks(pipeline)
                )
                forced_stale_keys.update(
                    _resolve_invalidation_keys(pipeline, invalidation_labels)
                )
                dag.add(pipeline, request=request)
                combos.append((job_config.label, pipeline, request))
        except SystemExit:
            raise
        except Exception as exc:
            raise SystemExit(f"error: {exc}") from exc

    return dag, combos, forced_stale_keys


def _normalize_arg_shellpath(args) -> str | None:
    try:
        return _normalize_shellpath(getattr(args, "shellpath", None))
    except ValueError as exc:
        raise SystemExit(f"error: {exc}") from exc


def _parse_resource_caps(args) -> dict[str, int]:
    cores = args.cores.strip()
    resource_caps = {"threads": os.cpu_count() or 1 if cores.lower() == "all" else int(cores)}
    for kv in args.constraints:
        if "=" not in kv:
            raise SystemExit(f"error: --constraint expects KEY=VALUE, got {kv!r}")
        k, v = kv.split("=", 1)
        resource_caps[k.strip()] = parse_resource(v.strip())
    return resource_caps


def _finalize_link_outputs(combos, *, nodes_dir: Path, results_dir: Path) -> None:
    for _label, pipeline, _nodes in combos:
        resolve_paths(pipeline.nodes, nodes_dir)
    _create_link_outputs(results_dir, combos, nodes_dir=nodes_dir)


def _run(args) -> None:
    nodes_dir, results_dir = _resolve_roots(args)
    dag, combos, forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    dag.execute(
        resource_caps=_parse_resource_caps(args),
        keep_going=args.keep_going,
        autoclean=args.autoclean,
        dry_run=args.dry_run,
        forced_stale_keys=forced_stale_keys,
        shellpath=_normalize_arg_shellpath(args),
    )
    if not args.dry_run:
        _finalize_link_outputs(combos, nodes_dir=nodes_dir, results_dir=results_dir)


def _graph(args) -> None:
    nodes_dir, _results_dir = _resolve_roots(args)
    dag, _combos, _forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    _apply_shell_execution_context(dag, _normalize_arg_shellpath(args))
    dag.resolve_paths(nodes_dir)
    rendered = str(dag)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


def _outputs(args) -> None:
    nodes_dir, results_dir = _resolve_roots(args)
    dag, combos, _forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    _apply_shell_execution_context(dag, _normalize_arg_shellpath(args))
    for label, pipeline, request in combos:
        resolve_paths(pipeline.nodes, nodes_dir)
        print(f"[{label}]")
        for node in request:
            key = (
                node.pipeline_label
                or node.output_name
                or (node.node_type.__name__ if node.node_type else "output")
            )
            rel = node.path.relative_to(nodes_dir)
            print(f"{key}\tnode={node.path}\tresult={results_dir / label / rel}")


def _provenance(args) -> None:
    path = Path(args.path)
    rip = path.parent / ".rip" / "dependencies.toml"
    if not rip.exists():
        raise SystemExit(f"error: provenance metadata not found: {rip}")
    doc = tomlkit.parse(rip.read_text(encoding="utf-8"))
    print(f"path = {path}")
    print(f"rule = {doc.get('rule', '')}")
    print(f"hash = {doc.get('hash', '')}")
    config = doc.get("config", {})
    if config:
        print("[config]")
        for k, v in config.items():
            print(f"{k} = {v!r}")
    execution = doc.get("execution", {})
    if execution:
        print("[execution]")
        for k, v in execution.items():
            print(f"{k} = {v!r}")


def _init(args) -> None:
    dest = Path(args.dir)
    if dest.exists() and any(dest.iterdir()) and not args.force:
        raise SystemExit(f"error: {dest} is not empty; pass --force to overwrite")
    dest.mkdir(parents=True, exist_ok=True)
    template_root = resources.files("necroflow") / "templates" / "canonical"
    for item in template_root.iterdir():
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=args.force)
        else:
            if target.exists() and not args.force:
                raise SystemExit(f"error: {target} exists; pass --force to overwrite")
            target.write_bytes(item.read_bytes())
    print(f"created {dest}")


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


def _add_run_options(parser) -> None:
    parser.add_argument(
        "jobs",
        nargs="+",
        metavar="JOB.toml",
        help="Job TOML file(s). Each defines a pipeline, optional request, and config params.",
    )
    parser.add_argument("--nodes-dir", default=None, type=Path, metavar="DIR", help="Directory for hashed node outputs (default: nodes).")
    parser.add_argument("--results-dir", default=None, type=Path, metavar="DIR", help="Directory for per-job symlink outputs (default: results).")
    parser.add_argument("--outdir", "-o", default=None, type=Path, metavar="DIR", help="Compatibility alias for using one directory for node outputs and results.")
    parser.add_argument("-c", dest="cores", default="all", metavar="N|all", help="Thread cap: integer or 'all' (default: all available CPUs). E.g. -c16 or -call.")
    parser.add_argument("--constraint", action="append", default=[], dest="constraints", metavar="KEY=VALUE", help="Resource cap, e.g. --constraint ram=300Mi. Repeatable. Overrides -c for threads.")
    parser.add_argument("--keep-going", "-k", action="store_true", help="Continue past failures and collect all errors at the end.")
    parser.add_argument("--autoclean", action="store_true", help="Delete orphan outputs before execution and intermediates as soon as they are no longer needed.")
    parser.add_argument("--dry-run", "-n", action="store_true", dest="dry_run", help="Show what would run without executing anything.")
    parser.add_argument("--invalidate", action="append", default=[], metavar="LABEL", help="Force an already-requested pipeline label to rerun. Repeatable.")
    parser.add_argument("--reap", action="append", default=[], metavar="NAME", help="Force labels from NAME in reap.toml to rerun. Repeatable.")
    parser.add_argument("--reap-file", default=Path("reap.toml"), type=Path, metavar="PATH", help="TOML file containing named invalidation label sets (default: reap.toml).")
    parser.add_argument("--validation", action="append", default=[], metavar="PATH.py:FUNCTION", help="Validate each expanded job config with a Python callable. Repeatable.")
    parser.add_argument("--shellpath", default=None, metavar="PATH", help="Executable shell path for string commands, e.g. /bin/bash. Defaults to Python's system shell behavior.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="necroflow",
        description="Run necroflow pipelines from job TOML files.",
    )
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser("init", help="Create a starter necroflow project")
    init_parser.add_argument("dir", nargs="?", default=".", help="Directory to create or populate")
    init_parser.add_argument("--force", action="store_true", help="Overwrite existing template files")
    init_parser.set_defaults(func=_init)

    graph_parser = subparsers.add_parser("graph", help="Render a DAG without executing it")
    _add_run_options(graph_parser)
    graph_parser.add_argument("--output", help="Write graph text to a file instead of stdout")
    graph_parser.set_defaults(func=_graph)

    outputs_parser = subparsers.add_parser("outputs", help="List requested output paths without executing")
    _add_run_options(outputs_parser)
    outputs_parser.set_defaults(func=_outputs)

    provenance_parser = subparsers.add_parser("provenance", help="Show stored provenance for an output path")
    provenance_parser.add_argument("path", help="Path to a cached output file")
    provenance_parser.set_defaults(func=_provenance)

    run_parser = subparsers.add_parser("run", help="Run job TOML files")
    _add_run_options(run_parser)
    run_parser.set_defaults(func=_run)
    return parser


def main(argv=None) -> None:
    argv = list(argv) if argv is not None else None
    commands = {"init", "graph", "outputs", "provenance", "run"}
    if argv and argv[0] not in commands:
        argv = ["run", *argv]
    elif argv is None:
        import sys
        if len(sys.argv) > 1 and sys.argv[1] not in commands:
            argv = ["run", *sys.argv[1:]]
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
