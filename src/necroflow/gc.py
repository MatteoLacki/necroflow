"""Provenance-aware garbage collection for a necroflow node store."""

from __future__ import annotations

import inspect
from pathlib import Path
import re
import shutil
import sys

import tomlkit

from necroflow.config import load_module
from necroflow.executor import _acquire_lock
from necroflow.fingerprints import declared_rule_hash
from necroflow.rules import Rule

_HASH_COMPONENT = re.compile(r"[0-9a-f]{64}")


def _load_rule_hashes(script: Path) -> set[str]:
    """Return declared hashes for module-level Rules reachable from pipeline factories."""

    module = load_module(script, kind="gc-pipelines")
    pipelines = getattr(module, "pipelines", None)
    if (
        not isinstance(pipelines, list)
        or not pipelines
        or not all(inspect.isfunction(pipeline) for pipeline in pipelines)
    ):
        raise ValueError(
            f"GC pipelines script {script} must define a non-empty "
            "pipelines list of functions"
        )
    rules: set[Rule] = set()
    pending = list(pipelines)
    visited: set[int] = set()
    while pending:
        factory = pending.pop()
        if id(factory) in visited:
            continue
        visited.add(id(factory))
        for name in factory.__code__.co_names:
            value = factory.__globals__.get(name)
            if isinstance(value, Rule):
                rules.add(value)
            elif inspect.isfunction(value):
                pending.append(value)
    if not rules:
        raise ValueError(
            f"GC pipelines script {script} does not reference any module-level rules"
        )
    return {declared_rule_hash(rule) for rule in rules}


def _entry(call_dir: Path):
    """Return a current-layout GC record, or None when metadata is not current."""

    metadata_path = call_dir / ".rip" / "dependencies.toml"
    try:
        metadata = tomlkit.parse(metadata_path.read_text(encoding="utf-8"))
        identity = metadata["identity"]
        if identity["format"] != "v3":
            return None
        if identity["rule_hash"] != call_dir.parent.name:
            return None
        if identity["provenance_hash"] != call_dir.name:
            return None
        outputs = metadata["outputs"]
        if not isinstance(outputs, list):
            return None
        mutable = any(output.get("mutable") is True for output in outputs)
        parents = [str(parent["node_key"]) for parent in metadata["parents"]]
        for parent_key in parents:
            parts = Path(parent_key).parts
            if (
                len(parts) != 4
                or _HASH_COMPONENT.fullmatch(parts[1]) is None
                or _HASH_COMPONENT.fullmatch(parts[2]) is None
            ):
                return None
    except (
        AttributeError,
        OSError,
        KeyError,
        TypeError,
        ValueError,
        tomlkit.exceptions.ParseError,
    ):
        return None
    return {"path": call_dir, "mutable": mutable, "parents": parents}


def _scan_entries(nodes_dir: Path):
    """Partition node-store folders into current entries and non-current paths."""

    entries = {}
    non_current = []
    if not nodes_dir.exists():
        return entries, non_current
    for rule_dir in nodes_dir.iterdir():
        if not rule_dir.is_dir() or rule_dir.name == ".rip":
            continue
        rule_hash_dirs = [path for path in rule_dir.iterdir() if path.is_dir()]
        if not rule_hash_dirs:
            non_current.append(rule_dir)
            continue
        for rule_hash_dir in rule_hash_dirs:
            if _HASH_COMPONENT.fullmatch(rule_hash_dir.name) is None:
                non_current.append(rule_hash_dir)
                continue
            call_dirs = [path for path in rule_hash_dir.iterdir() if path.is_dir()]
            current_call_dirs = [
                path
                for path in call_dirs
                if _HASH_COMPONENT.fullmatch(path.name) is not None
            ]
            if not current_call_dirs:
                non_current.append(rule_hash_dir)
                continue
            for call_dir in call_dirs:
                if _HASH_COMPONENT.fullmatch(call_dir.name) is None:
                    non_current.append(call_dir)
                    continue
                entry = _entry(call_dir)
                if entry is None:
                    non_current.append(call_dir)
                else:
                    entries[call_dir.relative_to(nodes_dir).as_posix()] = entry
    return entries, non_current


def _size(path: Path) -> int:
    """Return the recursive size in bytes of files below a candidate path."""

    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _incompatible_keys(entries, valid_rule_hashes: set[str]) -> set[str]:
    """Return current entry keys with an obsolete rule in their provenance."""

    memo: dict[str, bool] = {}

    def incompatible(key: str, visiting: set[str]) -> bool:
        if key in memo:
            return memo[key]
        parts = Path(key).parts
        if len(parts) != 3:
            return False
        if parts[1] not in valid_rule_hashes:
            memo[key] = True
            return True
        if key in visiting:
            return False

        next_visiting = visiting | {key}
        for parent_key in entries[key]["parents"]:
            parent_parts = Path(parent_key).parts
            if len(parent_parts) != 4:
                continue
            parent_call = Path(*parent_parts[:-1]).as_posix()
            if parent_call in entries:
                if incompatible(parent_call, next_visiting):
                    memo[key] = True
                    return True
            elif parent_parts[1] not in valid_rule_hashes:
                memo[key] = True
                return True
        memo[key] = False
        return False

    return {key for key in entries if incompatible(key, set())}


def collect(nodes_dir: Path, pipelines_script: Path, *, yes: bool = False) -> None:
    """Report and optionally delete nodes outside the current pipeline provenance."""

    nodes_dir = nodes_dir.expanduser().resolve()
    try:
        valid_rule_hashes = _load_rule_hashes(pipelines_script)
    except Exception as exc:
        raise SystemExit(f"error: {exc}") from exc
    with _acquire_lock(nodes_dir):
        entries, non_current = _scan_entries(nodes_dir)
        incompatible_keys = _incompatible_keys(entries, valid_rule_hashes)
        incompatible = sorted(
            (
                entry["path"]
                for key, entry in entries.items()
                if key in incompatible_keys and not entry["mutable"]
            ),
            key=str,
        )
        non_current = sorted(non_current, key=str)
        batches = (
            ("Incompatible provenance", incompatible),
            ("Non-current layout", non_current),
        )
        for title, paths in batches:
            if not paths:
                continue
            print(f"{title}:")
            for path in paths:
                print(path)
            print()
        candidates = incompatible + non_current
        total_size = sum(_size(path) for path in candidates)
        print(f"Total: {len(candidates)} directories, {total_size} bytes")
        protected_mutable = sum(
            1
            for key, entry in entries.items()
            if key in incompatible_keys and entry["mutable"]
        )
        if protected_mutable:
            print(f"Protected mutable state: {protected_mutable}")
        if not candidates:
            return
        if not yes:
            if not sys.stdin.isatty():
                raise SystemExit("error: confirmation requires a terminal; pass -y")
            if input("Delete these directories? [y/N] ").strip().lower() not in {
                "y",
                "yes",
            }:
                print("No directories deleted.")
                return
        for path in candidates:
            shutil.rmtree(path)
            for parent in (path.parent, path.parent.parent):
                try:
                    parent.rmdir()
                except OSError:
                    break
        print(f"Deleted {len(candidates)} directories.")
