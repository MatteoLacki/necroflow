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
import fcntl
import json
import os
import shutil
from importlib import resources
from pathlib import Path
from typing import Callable

import tomlkit

from necroflow import DAG
from necroflow.config import iter_job_configs, load_callable
from necroflow.dag import (
    NodeState,
    _content_hash,
    _has_changed_invalidation,
    _output_mtime,
    parse_resource,
    resolve_command,
    resolve_paths,
)
from necroflow.pipeline import _sinks
from necroflow.executor import (
    _apply_shell_execution_context,
    _normalize_shellpath,
    _prepare_active,
)


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


def _validate_job_config(
    job_config, validators: list[Callable], job_path: Path
) -> None:
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
    if args.outdir is not None and (
        args.nodes_dir is not None or args.results_dir is not None
    ):
        raise SystemExit(
            "error: --outdir cannot be combined with --nodes-dir or --results-dir"
        )
    if args.outdir is not None:
        return args.outdir, args.outdir
    return (
        args.nodes_dir if args.nodes_dir is not None else Path("nodes"),
        args.results_dir if args.results_dir is not None else Path("results"),
    )


def _build_dag_from_jobs(args, *, nodes_dir: Path):
    invalidation_labels = _dedupe_preserve_order(
        list(getattr(args, "invalidate", []))
        + _load_reap_labels(
            getattr(args, "reap_file", Path("reap.toml")), getattr(args, "reap", [])
        )
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
    resource_caps = {
        "threads": os.cpu_count() or 1 if cores.lower() == "all" else int(cores)
    }
    for kv in args.constraints:
        if "=" not in kv:
            raise SystemExit(f"error: --constraint expects KEY=VALUE, got {kv!r}")
        k, v = kv.split("=", 1)
        resource_caps[k.strip()] = parse_resource(v.strip())
    return resource_caps


def _json_ready(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    try:
        return value.unwrap()
    except AttributeError:
        return str(value)


def _emit_json(payload) -> None:
    print(json.dumps(_json_ready(payload), indent=2, sort_keys=True))


def _node_display_label(node) -> str:
    return (
        node.pipeline_label
        or node.output_name
        or (node.node_type.__name__ if node.node_type else "output")
    )


def _result_relative_path(node) -> Path:
    if node.path is None:
        raise ValueError("node path has not been resolved")
    return Path(_node_display_label(node)) / node.path.name


def _node_json(node, *, nodes_dir: Path | None = None) -> dict:
    data = {
        "key": node.key,
        "label": node.pipeline_label,
        "output_name": node.output_name,
        "rule": node.rule.__name__ if node.rule else "unknown",
        "node_type": node.node_type.__name__ if node.node_type else None,
        "state": node.state.value if isinstance(node.state, NodeState) else node.state,
        "path": str(node.path) if node.path is not None else None,
        "resources": dict(getattr(node.rule, "resources", {})) if node.rule else {},
        "constraints": dict(getattr(node.rule, "constraints", {})) if node.rule else {},
        "config": dict(node.config),
    }
    if nodes_dir is not None and node.path is not None:
        try:
            data["relative_path"] = node.path.relative_to(nodes_dir).as_posix()
        except ValueError:
            data["relative_path"] = str(node.path)
    return data


def _edge_json(nodes: list) -> list[dict]:
    node_keys = {node.key for node in nodes}
    return [
        {"from": parent.key, "to": node.key}
        for node in nodes
        for parent in node.parents
        if parent.key in node_keys
    ]


def _outputs_payload(combos, *, nodes_dir: Path, results_dir: Path) -> dict:
    jobs = []
    for label, pipeline, request in combos:
        resolve_paths(pipeline.nodes, nodes_dir)
        requested = []
        for node in request:
            node_rel = node.path.relative_to(nodes_dir)
            result_rel = _result_relative_path(node)
            requested.append(
                {
                    "label": _node_display_label(node),
                    "node_key": node.key,
                    "rule": node.rule.__name__ if node.rule else "unknown",
                    "node_path": str(node.path),
                    "result_path": str(results_dir / label / result_rel),
                    "relative_path": result_rel.as_posix(),
                    "node_relative_path": node_rel.as_posix(),
                }
            )
        jobs.append({"label": label, "requested": requested})
    return {"jobs": jobs}


def _graph_payload(dag, combos, *, nodes_dir: Path) -> dict:
    dag.resolve_paths(nodes_dir)
    requested = {node.key for node in dag.required_nodes}
    return {
        "nodes": [
            {
                **_node_json(node, nodes_dir=nodes_dir),
                "requested": node.key in requested,
            }
            for node in dag.nodes
        ],
        "edges": _edge_json(dag.nodes),
        "jobs": [
            {
                "label": label,
                "requested": [node.key for node in request],
            }
            for label, _pipeline, request in combos
        ],
    }


def _provenance_payload(path: Path) -> dict:
    rip = path.parent / ".rip" / "dependencies.toml"
    if not rip.exists():
        raise SystemExit(f"error: provenance metadata not found: {rip}")
    doc = tomlkit.parse(rip.read_text(encoding="utf-8"))
    return {
        "path": str(path),
        "rule": doc.get("rule", ""),
        "hash": doc.get("hash", ""),
        "config": _json_ready(doc.get("config", {})),
        "execution": _json_ready(doc.get("execution", {})),
    }


def _classification_reasons(node, forced_stale_keys: set[str]) -> list[dict]:
    if node.state == NodeState.MISSING:
        return [{"kind": "output_missing", "path": str(node.path)}]
    if node.state == NodeState.UP_TO_DATE:
        return [{"kind": "up_to_date"}]
    if node.state == NodeState.FAILED:
        return [{"kind": "blocked_by_failed_parent"}]
    reasons: list[dict] = []
    if node.key in forced_stale_keys:
        reasons.append({"kind": "forced_invalidation"})
    if node.is_compromised:
        reasons.append({"kind": "compromised_prior_state"})
    try:
        if _has_changed_invalidation(node):
            reasons.append({"kind": "invalidator_changed"})
    except Exception as exc:
        reasons.append({"kind": "invalidator_error", "error": str(exc)})
    for parent in node.parents:
        if parent.state in (NodeState.MISSING, NodeState.STALE):
            reasons.append(
                {
                    "kind": "parent_not_up_to_date",
                    "parent_key": parent.key,
                    "parent_label": parent.pipeline_label,
                    "parent_state": parent.state.value if parent.state else None,
                }
            )
        elif node.path is not None and parent.path is not None and parent.path.exists():
            try:
                if _output_mtime(parent.path) > _output_mtime(node.path):
                    hash_file = (
                        parent.path.parent / ".rip" / (parent.path.name + ".hash")
                    )
                    content_changed = not (
                        hash_file.exists()
                        and _content_hash(parent.path) == hash_file.read_text().strip()
                    )
                    if content_changed:
                        reasons.append(
                            {
                                "kind": "parent_content_changed",
                                "parent_key": parent.key,
                                "parent_label": parent.pipeline_label,
                            }
                        )
            except OSError as exc:
                reasons.append(
                    {
                        "kind": "parent_check_error",
                        "parent_key": parent.key,
                        "error": str(exc),
                    }
                )
    if node.state == NodeState.STALE and not reasons:
        reasons.append({"kind": "stale"})
    return reasons


def _explain_payload(args) -> dict:
    nodes_dir, _results_dir = _resolve_roots(args)
    dag, combos, forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    shellpath = _normalize_arg_shellpath(args)
    key_map = _apply_shell_execution_context(dag, shellpath)
    forced_stale_keys = {key_map.get(key, key) for key in forced_stale_keys}
    active, _active_keys, _n_cleaned = _prepare_active(
        dag,
        nodes_dir,
        autoclean=False,
        dry_run=True,
        forced_stale_keys=forced_stale_keys,
    )
    labels = {node.pipeline_label: node for node in active if node.pipeline_label}
    if args.node:
        if args.node not in labels:
            raise SystemExit(f"error: explain label not found: {args.node}")
        wanted = {labels[args.node].key}
        active = [node for node in active if node.key in wanted]
    nodes = []
    for node in sorted(active, key=lambda n: n.key):
        command = None
        try:
            command = resolve_command(node)
        except Exception as exc:
            command = f"<error: {exc}>"
        nodes.append(
            {
                **_node_json(node, nodes_dir=nodes_dir),
                "will_run": node.state in (NodeState.MISSING, NodeState.STALE),
                "command": command,
                "reasons": _classification_reasons(node, forced_stale_keys),
            }
        )
    return {
        "jobs": [
            {"label": label, "requested": [node.key for node in request]}
            for label, _pipeline, request in combos
        ],
        "nodes": nodes,
    }


def _issue(code: str, severity: str, message: str, **extra) -> dict:
    issue = {"code": code, "severity": severity, "message": message}
    issue.update({k: v for k, v in extra.items() if v is not None})
    return issue


def _doctor_payload(args) -> dict:
    issues: list[dict] = []
    nodes_dir, results_dir = _resolve_roots(args)
    try:
        _normalize_arg_shellpath(args)
    except SystemExit as exc:
        issues.append(
            _issue(
                "NF_SHELLPATH_INVALID",
                "error",
                str(exc).removeprefix("error: "),
                suggestion="Use an existing executable shell path or omit --shellpath.",
            )
        )
    try:
        _parse_resource_caps(args)
    except Exception as exc:
        issues.append(
            _issue(
                "NF_RESOURCE_INVALID",
                "error",
                str(exc).removeprefix("error: "),
                suggestion="Use integer resource caps or supported SI/binary suffixes.",
            )
        )
    try:
        dag, combos, forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    except SystemExit as exc:
        message = str(exc).removeprefix("error: ")
        code = "NF_PIPELINE_IMPORT_FAILED"
        if "has no '.pipeline'" in message:
            code = "NF_CONFIG_MISSING_PIPELINE"
        elif "request labels not found" in message:
            code = "NF_REQUEST_LABEL_NOT_FOUND"
        elif "validation" in message and "failed" in message:
            code = "NF_VALIDATION_FAILED"
        issues.append(_issue(code, "error", message))
        return {"ok": False, "issues": issues}
    except Exception as exc:
        issues.append(_issue("NF_CONFIG_PARSE_FAILED", "error", str(exc)))
        return {"ok": False, "issues": issues}

    try:
        dag.resolve_paths(nodes_dir)
    except ValueError as exc:
        issues.append(_issue("NF_OUTPUT_PATH_TOO_LONG", "error", str(exc)))
    for directory, label in ((nodes_dir, "nodes_dir"), (results_dir, "results_dir")):
        try:
            directory.mkdir(parents=True, exist_ok=True)
            probe = directory / ".necroflow-doctor-write-test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            issues.append(
                _issue(
                    "NF_OUTPUT_ROOT_NOT_WRITABLE",
                    "error",
                    f"{label} is not writable: {directory}: {exc}",
                    path=str(directory),
                )
            )
    lock_path = nodes_dir / ".rip" / "necroflow.lock"
    if lock_path.exists():
        try:
            with open(lock_path, "a") as fh:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh, fcntl.LOCK_UN)
        except OSError:
            issues.append(
                _issue(
                    "NF_NODESTORE_LOCKED",
                    "error",
                    f"node store is locked: {nodes_dir}",
                    path=str(lock_path),
                )
            )
    return {"ok": not any(i["severity"] == "error" for i in issues), "issues": issues}


def _finalize_link_outputs(combos, *, nodes_dir: Path, results_dir: Path) -> None:
    for _label, pipeline, _nodes in combos:
        resolve_paths(pipeline.nodes, nodes_dir)
    _create_link_outputs(results_dir, combos, nodes_dir=nodes_dir)


def _run(args) -> None:
    nodes_dir, results_dir = _resolve_roots(args)
    dag, combos, forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    try:
        report = dag.execute(
            resource_caps=_parse_resource_caps(args),
            keep_going=args.keep_going,
            autoclean=args.autoclean,
            dry_run=args.dry_run,
            forced_stale_keys=forced_stale_keys,
            shellpath=_normalize_arg_shellpath(args),
        )
    except ExceptionGroup as exc:
        report = getattr(exc, "execution_report", None)
        if args.keep_going and not args.dry_run:
            _finalize_link_outputs(combos, nodes_dir=nodes_dir, results_dir=results_dir)
            _write_execution_summaries(results_dir, combos, report)
        raise
    if not args.dry_run:
        _finalize_link_outputs(combos, nodes_dir=nodes_dir, results_dir=results_dir)
        _write_execution_summaries(results_dir, combos, report)


def _graph(args) -> None:
    nodes_dir, _results_dir = _resolve_roots(args)
    dag, combos, _forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    _apply_shell_execution_context(dag, _normalize_arg_shellpath(args))
    if args.json:
        _emit_json(_graph_payload(dag, combos, nodes_dir=nodes_dir))
        return
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
    if args.json:
        _emit_json(
            _outputs_payload(combos, nodes_dir=nodes_dir, results_dir=results_dir)
        )
        return
    for label, pipeline, request in combos:
        resolve_paths(pipeline.nodes, nodes_dir)
        print(f"[{label}]")
        for node in request:
            key = _node_display_label(node)
            rel = _result_relative_path(node)
            print(f"{key}\tnode={node.path}\tresult={results_dir / label / rel}")


def _provenance(args) -> None:
    path = Path(args.path)
    payload = _provenance_payload(path)
    if args.json:
        _emit_json(payload)
        return
    print(f"path = {path}")
    print(f"rule = {payload.get('rule', '')}")
    print(f"hash = {payload.get('hash', '')}")
    config = payload.get("config", {})
    if config:
        print("[config]")
        for k, v in config.items():
            print(f"{k} = {v!r}")
    execution = payload.get("execution", {})
    if execution:
        print("[execution]")
        for k, v in execution.items():
            print(f"{k} = {v!r}")


def _doctor(args) -> None:
    payload = _doctor_payload(args)
    if args.json:
        _emit_json(payload)
    else:
        if payload["ok"]:
            print("doctor: ok")
        else:
            for issue in payload["issues"]:
                print(f"{issue['severity']}: {issue['code']}: {issue['message']}")
    if not payload["ok"]:
        raise SystemExit(1)


def _explain(args) -> None:
    payload = _explain_payload(args)
    if args.json:
        _emit_json(payload)
        return
    for node in payload["nodes"]:
        label = node.get("label") or node.get("output_name") or node["key"]
        print(f"{label}")
        print(f"  state: {node.get('state')}")
        print(f"  will_run: {str(node.get('will_run')).lower()}")
        print(f"  rule: {node.get('rule')}")
        print(f"  path: {node.get('path')}")
        if node.get("resources"):
            resources = " ".join(f"{k}={v}" for k, v in node["resources"].items())
            print(f"  resources: {resources}")
        for reason in node.get("reasons", []):
            print(f"  reason: {reason['kind']}")


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


def _requested_with_ancestors(request: list) -> list:
    seen: dict[str, object] = {}
    stack = list(request)
    while stack:
        node = stack.pop()
        if node.key in seen:
            continue
        seen[node.key] = node
        stack.extend(node.parents)
    return list(seen.values())


def _write_execution_summaries(
    results_dir: Path,
    combos: list[tuple[str, object, list]],
    report,
) -> None:
    if report is None:
        return
    for label, _pipeline, request in combos:
        data = tomlkit.document()
        nodes_array = tomlkit.aot()
        for node in sorted(_requested_with_ancestors(request), key=lambda n: n.key):
            event = report.get(node)
            if event is None:
                continue
            values = event.to_toml_dict()
            if node.pipeline_label is not None:
                values["label"] = node.pipeline_label
            if node.output_name is not None:
                values["output_name"] = node.output_name
            table = tomlkit.table()
            for key, value in values.items():
                table[key] = value
            nodes_array.append(table)
        data["nodes"] = nodes_array
        combo_dir = results_dir / label
        combo_dir.mkdir(parents=True, exist_ok=True)
        (combo_dir / "execution.toml").write_text(tomlkit.dumps(data), encoding="utf-8")


def _prune_empty_dirs(path: Path, stop: Path) -> None:
    while path != stop and path.is_dir():
        try:
            path.rmdir()
        except OSError:
            return
        path = path.parent


def _clear_generated_result_links(combo_dir: Path) -> None:
    manifest = combo_dir / "manifest.toml"
    if not manifest.exists():
        return
    try:
        doc = tomlkit.parse(manifest.read_text(encoding="utf-8"))
        paths = [combo_dir / str(rel) for rel in doc.get("outputs", {}).values()]
    except Exception:
        return
    for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
        if path.is_symlink():
            parent = path.parent
            path.unlink()
            _prune_empty_dirs(parent, combo_dir)


def _create_link_outputs(
    results_dir: Path,
    combos: list[tuple[str, object, list]],
    *,
    nodes_dir: Path | None = None,
) -> None:
    """Create per-combo symlink dirs and manifests under results_dir/{label}/.

    Only requested (sink) outputs get a symlink — ancestors are excluded.
    """
    for label, pipeline, sink_nodes in combos:
        combo_dir = results_dir / label
        _clear_generated_result_links(combo_dir)

        manifest_lines = ["[outputs]\n"]
        for node in sink_nodes:
            if node.path is None or not node.path.exists():
                continue
            rel = _result_relative_path(node)
            link = combo_dir / rel
            link.parent.mkdir(parents=True, exist_ok=True)
            if link.is_symlink():
                link.unlink()
            elif link.exists():
                raise FileExistsError(
                    f"refusing to overwrite non-symlink result: {link}"
                )
            link.symlink_to(Path(os.path.relpath(node.path, link.parent)))
            manifest_lines.append(f'{_node_display_label(node)} = "{rel.as_posix()}"\n')

        combo_dir.mkdir(parents=True, exist_ok=True)
        (combo_dir / "manifest.toml").write_text(
            "".join(manifest_lines), encoding="utf-8"
        )


def _add_run_options(parser) -> None:
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
        "--outdir",
        "-o",
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
        "--keep-going",
        "-k",
        action="store_true",
        help="Continue past failures and collect all errors at the end.",
    )
    parser.add_argument(
        "--autoclean",
        action="store_true",
        help="Delete orphan outputs before execution and intermediates as soon as they are no longer needed.",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
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
    parser.add_argument(
        "--shellpath",
        default=None,
        metavar="PATH",
        help="Executable shell path for string commands, e.g. /bin/bash. Defaults to Python's system shell behavior.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="necroflow",
        description="Run necroflow pipelines from job TOML files.",
    )
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser(
        "init", help="Create a starter necroflow project"
    )
    init_parser.add_argument(
        "dir", nargs="?", default=".", help="Directory to create or populate"
    )
    init_parser.add_argument(
        "--force", action="store_true", help="Overwrite existing template files"
    )
    init_parser.set_defaults(func=_init)

    graph_parser = subparsers.add_parser(
        "graph", help="Render a DAG without executing it"
    )
    _add_run_options(graph_parser)
    graph_parser.add_argument(
        "--output", help="Write graph text to a file instead of stdout"
    )
    graph_parser.add_argument(
        "--json", action="store_true", help="Write JSON to stdout"
    )
    graph_parser.set_defaults(func=_graph)

    outputs_parser = subparsers.add_parser(
        "outputs", help="List requested output paths without executing"
    )
    _add_run_options(outputs_parser)
    outputs_parser.add_argument(
        "--json", action="store_true", help="Write JSON to stdout"
    )
    outputs_parser.set_defaults(func=_outputs)

    provenance_parser = subparsers.add_parser(
        "provenance", help="Show stored provenance for an output path"
    )
    provenance_parser.add_argument("path", help="Path to a cached output file")
    provenance_parser.add_argument(
        "--json", action="store_true", help="Write JSON to stdout"
    )
    provenance_parser.set_defaults(func=_provenance)

    doctor_parser = subparsers.add_parser(
        "doctor", help="Check whether job TOMLs are runnable"
    )
    _add_run_options(doctor_parser)
    doctor_parser.add_argument(
        "--json", action="store_true", help="Write JSON to stdout"
    )
    doctor_parser.set_defaults(func=_doctor)

    explain_parser = subparsers.add_parser(
        "explain", help="Explain what would run and why"
    )
    _add_run_options(explain_parser)
    explain_parser.add_argument(
        "--json", action="store_true", help="Write JSON to stdout"
    )
    explain_parser.add_argument(
        "--node", metavar="LABEL", help="Show one pipeline label"
    )
    explain_parser.set_defaults(func=_explain)

    run_parser = subparsers.add_parser("run", help="Run job TOML files")
    _add_run_options(run_parser)
    run_parser.set_defaults(func=_run)
    return parser


def main(argv=None) -> None:
    argv = list(argv) if argv is not None else None
    commands = {"init", "graph", "outputs", "provenance", "doctor", "explain", "run"}
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
