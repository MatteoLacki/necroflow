"""necroflow CLI — run pipelines from job TOML files with __grid expansion.

Terms used in this file
-----------------------
pipeline_spec — the raw '.pipeline' string from the job TOML,
                e.g. 'pipelines/rna.py:build'.  Resolved by load_callable from
                a file path and a function name.

factory       — a user-supplied Python function loaded from a pipeline_spec.
                Signature: factory(Pipeline, config: dict) -> None.

job TOML      — a TOML file describing one run: which factory to call, which
                outputs to request, and what config parameters to pass.
                Must contain a '.pipeline' key; all other keys become config.

combo         — one expanded parameter combination produced by __grid
                expansion of a job TOML.  A single job TOML with two
                __grid axes of size M×N yields M*N combos.

request       — the subset of Pipeline nodes that the DAG must produce for a
                given combo.  Defaults to the pipeline's sink nodes (leaves).
                Overridden by '.requests' in the job TOML (list of
                Pipeline label strings).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import secrets
import shutil
import subprocess
import sys
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Callable, TypeAlias

import tomlkit

from necroflow import (
    DAG,
    Node,
    Pipeline,
    fifo_scheduler,
    make_connected_component_scheduler,
)
from necroflow.config import iter_job_configs, load_callable
from necroflow.dag import (
    NodeState,
    _check_path_limits,
    _content_hash,
    parse_resource,
    resolve_command,
)
from necroflow.pipeline import _normalize_shellpath
from necroflow.graphviz_render import render_png
from necroflow.planning import plan_execution
from necroflow.gc import collect


@dataclass(frozen=True)
class _RequestedOutput:
    label: str
    node: Node


_Combo: TypeAlias = tuple[str, Pipeline, list[_RequestedOutput]]


def _load_validators(specs: list[str]) -> list[Callable]:
    validators: list[Callable] = []
    for spec in specs:
        try:
            validators.append(load_callable(spec, kind="validation"))
        except Exception as exc:
            raise SystemExit(f"error: {exc}") from exc
    return validators


def _load_scheduler(spec: str) -> Callable:
    if spec == "connected-components":
        return make_connected_component_scheduler()
    builtins = {
        "fifo": fifo_scheduler,
    }
    if spec in builtins:
        return builtins[spec]
    try:
        return load_callable(spec, kind="scheduler")
    except Exception as exc:
        raise SystemExit(
            "error: --scheduler must be connected-components, fifo, or "
            f"path.py:function; got {spec!r}: {exc}"
        ) from exc


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
    # Unlike dict, set does not preserve insertion order: it's session specific
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


def _resolve_invalidation_keys(pipeline, labels: list[str]) -> set[Path]:
    if not labels:
        return set()
    missing = [label for label in labels if label not in pipeline.labels]
    if missing:
        raise SystemExit(f"error: invalidation labels not found in pipeline: {missing}")
    return {pipeline[label].relative_path for label in labels}


def _resolve_request(pipeline, labels: list[str] | None) -> list[_RequestedOutput]:
    """Resolve explicit labels, or all labels bound to pipeline sinks."""
    pipeline._assert_finished()
    if labels is None:
        sinks = {node.relative_path for node in pipeline.sinks()}
        return [
            _RequestedOutput(label, pipeline[label])
            for label in pipeline.labels
            if pipeline[label].relative_path in sinks
        ]
    missing = [label for label in labels if label not in pipeline.labels]
    if missing:
        raise SystemExit(f"error: request labels not found in pipeline: {missing}")
    return [_RequestedOutput(label, pipeline[label]) for label in labels]


def _validate_result_paths(results_dir: Path, combos: list[_Combo]) -> None:
    """Validate requested result paths against the destination filesystem."""
    for job_label, _pipeline, request in combos:
        for binding in request:
            path = (
                results_dir
                / job_label
                / _result_relative_path(binding.node, binding.label)
            )
            try:
                _check_path_limits(path.absolute())
            except ValueError as exc:
                raise ValueError(
                    f"result path for Pipeline label {binding.label!r} is invalid: "
                    f"{exc}"
                ) from exc


def _preflight_result_paths(results_dir: Path, combos: list[_Combo]) -> None:
    """Fail a CLI command cleanly when a requested result path is impossible."""
    try:
        _validate_result_paths(results_dir, combos)
    except ValueError as exc:
        raise SystemExit(f"error: {exc}") from exc


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
    combos: list[_Combo] = []
    forced_stale_keys: set[Path] = set()
    shellpath = _normalize_arg_shellpath(args)

    for job_path_str in args.jobs:
        job_path = Path(job_path_str)
        try:
            job_configs = iter_job_configs(
                job_path,
                require_pipeline=True,
                short_names=not getattr(args, "long_names", False),
            )
            for job_config in job_configs:
                if not job_config.pipeline_spec:
                    raise SystemExit(
                        f"error: job TOML {job_path} has no '.pipeline' key"
                    )
                if validators:
                    _validate_job_config(job_config, validators, job_path)
                factory = load_callable(job_config.pipeline_spec, kind="pipeline")
                pipeline = Pipeline(dag, shellpath=shellpath)
                result = factory(pipeline, job_config.config)
                if result is not None:
                    raise TypeError(
                        f"pipeline factory {job_config.pipeline_spec!r} must mutate "
                        "the supplied Pipeline and return None"
                    )
                pipeline.finish()
                request = _resolve_request(pipeline, job_config.request_labels)
                forced_stale_keys.update(
                    _resolve_invalidation_keys(pipeline, invalidation_labels)
                )
                dag.require(binding.node for binding in request)
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


def _node_display_label(node, label: str | None = None) -> str:
    return (
        label
        or node.rule_call.dag.label_for(node)
        or node.output_name
        or (node.node_type.__name__ if node.node_type else "output")
    )


def _result_relative_path(node, label: str | None = None) -> Path:
    if node.path is None:
        raise ValueError("node path has not been resolved")
    return Path(_node_display_label(node, label)) / node.path.name


def _node_json(node, *, nodes_dir: Path | None = None) -> dict:
    data = {
        "key": node.relative_path.as_posix(),
        "label": node.rule_call.dag.label_for(node),
        "labels": list(node.rule_call.dag.labels_for(node)),
        "output_name": node.output_name,
        "rule": node.rule.__name__ if node.rule else "unknown",
        "node_type": node.node_type.__name__ if node.node_type else None,
        "mutable": node.mutable,
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
    node_keys = {node.relative_path for node in nodes}
    return [
        {
            "from": parent.relative_path.as_posix(),
            "to": node.relative_path.as_posix(),
            "mutable": parent.mutable,
        }
        for node in nodes
        for parent in node.parents
        if parent.relative_path in node_keys
    ]


def _outputs_payload(combos, *, nodes_dir: Path, results_dir: Path) -> dict:
    jobs = []
    for label, pipeline, request in combos:
        requested = []
        for binding in request:
            node = binding.node
            node_rel = node.path.relative_to(nodes_dir)
            result_rel = _result_relative_path(node, binding.label)
            requested.append(
                {
                    "label": binding.label,
                    "node_key": node.relative_path.as_posix(),
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
    requested = {node.relative_path for node in dag.required_nodes}
    return {
        "nodes": [
            {
                **_node_json(node, nodes_dir=nodes_dir),
                "requested": node.relative_path in requested,
            }
            for node in dag.nodes
        ],
        "edges": _edge_json(dag.nodes),
        "jobs": [
            {
                "label": label,
                "requested": [
                    binding.node.relative_path.as_posix() for binding in request
                ],
            }
            for label, _pipeline, request in combos
        ],
    }


def _provenance_payload(path: Path) -> dict:
    rip = path.parent / ".rip" / "dependencies.toml"
    if not rip.exists():
        raise SystemExit(f"error: provenance metadata not found: {rip}")
    doc = tomlkit.parse(rip.read_text(encoding="utf-8"))
    identity = doc.get("identity", {})
    return {
        "path": str(path),
        "rule": doc.get("rule", ""),
        "rule_hash": identity.get("rule_hash", ""),
        "provenance_hash": identity.get("provenance_hash", ""),
        "config": _json_ready(doc.get("config", {})),
        "execution": _json_ready(doc.get("execution", {})),
    }


def _explain_payload(args) -> dict:
    nodes_dir, _results_dir = _resolve_roots(args)
    dag, combos, forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    plan = plan_execution(
        dag,
        forced_stale_keys=forced_stale_keys,
        include_advisories=True,
    )
    active = plan.active
    labels = {
        label: pipeline[label]
        for _job_label, pipeline, _request in combos
        for label in pipeline.labels
        if pipeline[label] in active
    }
    if args.node:
        if args.node not in labels:
            raise SystemExit(f"error: explain label not found: {args.node}")
        wanted = {labels[args.node].relative_path}
        active = [node for node in active if node.relative_path in wanted]
    nodes = []
    for node in sorted(active, key=lambda n: n.relative_path):
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
                "reasons": plan.reasons[node.relative_path],
            }
        )
    return {
        "jobs": [
            {
                "label": label,
                "requested": [
                    binding.node.relative_path.as_posix() for binding in request
                ],
            }
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
    except (Exception, SystemExit) as exc:
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

    for job_label, pipeline, _request in combos:
        for node in pipeline.nodes:
            labels = pipeline.labels_for(node)
            if len(labels) > 1:
                issues.append(
                    _issue(
                        "NF_MULTIPLE_LABELS",
                        "info",
                        f"job {job_label!r}: Pipeline labels {labels!r} "
                        "refer to the same node",
                        job=job_label,
                        labels=list(labels),
                        path=node.relative_path.as_posix(),
                    )
                )

    try:
        _validate_result_paths(results_dir, combos)
    except ValueError as exc:
        issues.append(
            _issue(
                "NF_RESULT_PATH_INVALID",
                "error",
                str(exc),
                suggestion="Shorten the job name, Pipeline label, or results root.",
            )
        )

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


def _run(args) -> None:
    nodes_dir, results_dir = _resolve_roots(args)
    dag, combos, forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    _preflight_result_paths(results_dir, combos)

    def materialize(report):
        _materialize_results(results_dir, combos)
        _write_execution_summaries(results_dir, combos, report)

    dag.execute(
        resource_caps=_parse_resource_caps(args),
        scheduler=_load_scheduler(args.scheduler),
        keep_going=args.keep_going,
        autoclean=args.autoclean,
        dry_run=args.dry_run,
        forced_stale_keys=forced_stale_keys,
        on_complete=None if args.dry_run else materialize,
    )


def _gc(args) -> None:
    collect(
        args.nodes_dir,
        args.gc_rules_script,
        yes=args.yes,
        prune_unknown_rules=args.prune_unknown_rules,
    )


def _graph(args) -> None:
    nodes_dir, _results_dir = _resolve_roots(args)
    dag, combos, _forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    if args.json:
        _emit_json(_graph_payload(dag, combos, nodes_dir=nodes_dir))
        return
    if args.png:
        title = ", ".join(Path(j).stem for j in args.jobs)
        render_png(dag, output_path=Path(args.png), title=title)
        return
    rendered = str(dag)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


def _outputs(args) -> None:
    nodes_dir, results_dir = _resolve_roots(args)
    dag, combos, _forced_stale_keys = _build_dag_from_jobs(args, nodes_dir=nodes_dir)
    _preflight_result_paths(results_dir, combos)
    if args.json:
        _emit_json(
            _outputs_payload(combos, nodes_dir=nodes_dir, results_dir=results_dir)
        )
        return
    for label, pipeline, request in combos:
        print(f"[{label}]")
        for binding in request:
            node = binding.node
            key = binding.label
            rel = _result_relative_path(node, binding.label)
            print(f"{key}\tnode={node.path}\tresult={results_dir / label / rel}")


def _provenance(args) -> None:
    path = Path(args.path)
    payload = _provenance_payload(path)
    if args.json:
        _emit_json(payload)
        return
    print(f"path = {path}")
    print(f"rule = {payload.get('rule', '')}")
    print(f"rule_hash = {payload.get('rule_hash', '')}")
    print(f"provenance_hash = {payload.get('provenance_hash', '')}")
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
        if payload["issues"]:
            for issue in payload["issues"]:
                print(f"{issue['severity']}: {issue['code']}: {issue['message']}")
        else:
            print("doctor: ok")
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
    template = resources.files("necroflow") / "templates" / "canonical"
    with resources.as_file(template) as template_root:
        for item in template_root.iterdir():
            target = dest / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=args.force)
            else:
                if target.exists() and not args.force:
                    raise SystemExit(
                        f"error: {target} exists; pass --force to overwrite"
                    )
                target.write_bytes(item.read_bytes())
    print(f"created {dest}")


def _requested_with_ancestors(request: list[_RequestedOutput]) -> list:
    seen: dict[Path, object] = {}
    stack = [binding.node for binding in request]
    while stack:
        node = stack.pop()
        if node.relative_path in seen:
            continue
        seen[node.relative_path] = node
        stack.extend(node.parents)
    return list(seen.values())


def _write_execution_summaries(
    results_dir: Path,
    combos: list[_Combo],
    report,
) -> None:
    if report is None:
        return
    for label, pipeline, request in combos:
        data = tomlkit.document()
        rules_array = tomlkit.aot()
        rule_nodes: dict[Path, list[Node]] = {}
        for node in sorted(
            _requested_with_ancestors(request), key=lambda n: n.relative_path
        ):
            rule_nodes.setdefault(node.rule_call.relative_path, []).append(node)

        total_duration = 0.0
        for rule_key, nodes in rule_nodes.items():
            events = [
                event
                for node in nodes
                if (event := report.get(node.relative_path.as_posix())) is not None
            ]
            if not events:
                continue
            event = next((event for event in events if not event.cached), events[0])
            values = event.to_toml_dict()
            for node_field in ("key", "rule", "output_name", "label", "path"):
                values.pop(node_field, None)
            values = {
                "key": rule_key.as_posix(),
                "name": event.rule,
                **values,
            }
            if event.duration_seconds is not None:
                total_duration += event.duration_seconds
            table = tomlkit.table()
            for key, value in values.items():
                table[key] = value

            outputs = tomlkit.aot()
            for node in nodes:
                output = tomlkit.table()
                output["key"] = node.relative_path.as_posix()
                output["name"] = node.output_name
                output["path"] = str(node.path)
                labels = pipeline.labels_for(node)
                if labels:
                    output["labels"] = labels
                outputs.append(output)
            table["outputs"] = outputs
            rules_array.append(table)
        data["total_duration_seconds"] = total_duration
        data["rules"] = rules_array
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


def _owned_result_paths(combo_dir: Path) -> set[Path]:
    manifest = combo_dir / "manifest.toml"
    if not manifest.exists():
        return set()
    try:
        doc = tomlkit.parse(manifest.read_text(encoding="utf-8"))
        values = doc.get("outputs", {}).values()
        relative_paths = [
            value.get("path") if isinstance(value, dict) else value for value in values
        ]
        if any(not isinstance(value, str) for value in relative_paths):
            raise TypeError("output path must be a string")
    except Exception as exc:
        raise ValueError(f"cannot read result manifest {manifest}: {exc}") from exc

    paths = set()
    for relative in relative_paths:
        rel = Path(relative)
        if relative == "" or rel == Path(".") or rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"unsafe result path in {manifest}: {relative!r}")
        paths.add(combo_dir / rel)
    return paths


def _clear_generated_results(combo_dir: Path, paths: set[Path]) -> None:
    for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
        parent = path.parent
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
        _prune_empty_dirs(parent, combo_dir)


def _copy_result(source: Path, destination: Path) -> None:
    command = ["cp", "-a"]
    if sys.platform == "darwin":
        command.append("-c")
    else:
        command.append("--reflink=auto")
        command.append("--")
    command.extend((str(source), str(destination)))
    subprocess.run(command, check=True)


def _materialize_results(
    results_dir: Path,
    combos: list[_Combo],
) -> None:
    """Copy requested outputs into per-combo result directories.

    GNU ``cp`` opportunistically creates reflinks; macOS requests APFS clones.
    Symlink outputs remain symlinks because ``cp -a`` does not dereference them.
    """
    _validate_result_paths(results_dir, combos)
    for label, _pipeline, requested_outputs in combos:
        combo_dir = results_dir / label
        combo_dir.mkdir(parents=True, exist_ok=True)
        owned_paths = _owned_result_paths(combo_dir)

        manifest = tomlkit.document()
        manifest_outputs = tomlkit.table()
        staged: list[tuple[Path, Path]] = []
        try:
            for binding in requested_outputs:
                node = binding.node
                if node.path is None or not node.path.exists():
                    continue
                rel = _result_relative_path(node, binding.label)
                destination = combo_dir / rel
                if (
                    destination.exists() or destination.is_symlink()
                ) and destination not in owned_paths:
                    raise FileExistsError(
                        f"refusing to overwrite unmanaged result: {destination}"
                    )
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(
                    f".{destination.name}.necroflow-{secrets.token_hex(8)}"
                )
                _copy_result(node.path, temporary)
                staged.append((temporary, destination))

                entry = tomlkit.table()
                entry["path"] = rel.as_posix()
                entry["origin_node_key"] = node.relative_path.as_posix()
                entry["content_sha256"] = _content_hash(temporary)
                manifest_outputs[binding.label] = entry

            _clear_generated_results(combo_dir, owned_paths)
            for temporary, destination in staged:
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temporary, destination)

            manifest["outputs"] = manifest_outputs
            manifest_path = combo_dir / "manifest.toml"
            manifest_temporary = combo_dir / ".manifest.toml.necroflow"
            manifest_temporary.write_text(tomlkit.dumps(manifest), encoding="utf-8")
            os.replace(manifest_temporary, manifest_path)
        finally:
            for temporary, _destination in staged:
                if temporary.is_symlink() or temporary.is_file():
                    temporary.unlink()
                elif temporary.is_dir():
                    shutil.rmtree(temporary)


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
        help="Directory for per-job copied outputs (default: results).",
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
    parser.add_argument(
        "--long-names",
        action="store_true",
        help="Use full nested parameter names in generated job/result labels "
        "instead of the default shortened, deduplicated form.",
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
    graph_parser.add_argument(
        "--png",
        help="Render the DAG as a PNG grouped by dependency depth instead of "
        "ASCII. Requires the 'dev' extra and the system 'dot' binary.",
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
    run_parser.add_argument(
        "--scheduler",
        default="connected-components",
        metavar="NAME|PATH.py:FUNCTION",
        help="Scheduling policy: connected-components (default), fifo, or a local Python callable.",
    )
    run_parser.set_defaults(func=_run)

    gc_parser = subparsers.add_parser(
        "gc", help="Delete cache entries outside the declared rule scope"
    )
    gc_parser.add_argument("--nodes-dir", type=Path, default=Path("nodes"))
    gc_parser.add_argument(
        "--gc-rules-script", required=True, type=Path, metavar="PATH.py"
    )
    gc_parser.add_argument(
        "--prune-unknown-rules",
        action="store_true",
        help="also collect nodes whose rule name the script never declares",
    )
    gc_parser.add_argument("-y", "--yes", action="store_true")
    gc_parser.set_defaults(func=_gc)
    return parser


def main(argv=None) -> None:
    argv = list(argv) if argv is not None else None
    commands = {
        "init",
        "graph",
        "outputs",
        "provenance",
        "doctor",
        "explain",
        "run",
        "gc",
    }
    if argv and argv[0] not in commands:
        argv = ["run", *argv]
    elif argv is None:
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
