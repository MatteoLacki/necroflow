"""Provenance-aware garbage collection for a necroflow node store."""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import sys

import tomlkit

from necroflow.config import load_module
from necroflow.executor import _acquire_lock
from necroflow.fingerprints import (
    IDENTITY_FORMAT,
    declared_rule_hash,
    hash_rule_identity,
)
from necroflow.rules import Rule

_HASH_COMPONENT = re.compile(r"[0-9a-f]{64}")


def _load_declared_rules(script: Path) -> tuple[set[str], set[str]]:
    """Return the (rule hashes, rule names) a GC scope script preserves.

    The script states its scope explicitly as ``rules = [...]``. A renamed or
    deleted rule therefore fails at import, which is the loud failure a
    destructive tool wants; silent under-discovery would delete live nodes.
    """

    module = load_module(script, kind="gc-rules")
    rules = getattr(module, "rules", None)
    if (
        not isinstance(rules, list)
        or not rules
        or not all(isinstance(rule, Rule) for rule in rules)
    ):
        raise ValueError(
            f"GC rules script {script} must define a non-empty "
            "rules list of Rule objects"
        )
    return (
        {declared_rule_hash(rule) for rule in rules},
        {rule.__name__ for rule in rules},
    )


def _entry(call_dir: Path):
    """Return a current-layout GC record, or None when metadata is not current."""

    metadata_path = call_dir / ".rip" / "dependencies.toml"
    try:
        metadata = tomlkit.parse(metadata_path.read_text(encoding="utf-8"))
        identity = metadata["identity"]
        if identity["format"] != IDENTITY_FORMAT:
            return None
        recipe = metadata["recipe"]
        if recipe["rule"] != call_dir.parent.name:
            return None
        if identity["provenance_hash"] != call_dir.name:
            return None
        rule_hash = str(identity["rule_hash"])
        if hash_rule_identity(recipe) != rule_hash:
            return None
        outputs = metadata["outputs"]
        if not isinstance(outputs, list):
            return None
        mutable = metadata.get("mutable") is True
        parents = [str(parent["node_key"]) for parent in metadata["parents"]]
        for parent_key in parents:
            parts = Path(parent_key).parts
            if len(parts) != 3 or _HASH_COMPONENT.fullmatch(parts[1]) is None:
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
    return {
        "path": call_dir,
        "rule_hash": rule_hash,
        "mutable": mutable,
        "parents": parents,
    }


def _scan_entries(nodes_dir: Path):
    """Partition node-store folders into current entries and non-current paths."""

    entries = {}
    non_current = []
    if not nodes_dir.exists():
        return entries, non_current
    for rule_dir in nodes_dir.iterdir():
        if not rule_dir.is_dir() or rule_dir.name == ".rip":
            continue
        call_dirs = [path for path in rule_dir.iterdir() if path.is_dir()]
        if not call_dirs:
            non_current.append(rule_dir)
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
        if len(parts) != 2:
            return False
        if entries[key]["rule_hash"] not in valid_rule_hashes:
            memo[key] = True
            return True
        if key in visiting:
            return False

        next_visiting = visiting | {key}
        for parent_key in entries[key]["parents"]:
            parent_parts = Path(parent_key).parts
            if len(parent_parts) != 3:
                continue
            parent_call = Path(*parent_parts[:-1]).as_posix()
            if parent_call in entries and incompatible(parent_call, next_visiting):
                memo[key] = True
                return True
        memo[key] = False
        return False

    return {key for key in entries if incompatible(key, set())}


def _rule_name(key: str) -> str:
    """Return the rule-name component of a node-store entry key."""

    return Path(key).parts[0]


def collect(
    nodes_dir: Path,
    rules_script: Path,
    *,
    yes: bool = False,
    prune_unknown_rules: bool = False,
) -> None:
    """Report and optionally delete nodes outside the declared rule scope."""

    nodes_dir = nodes_dir.expanduser().resolve()
    try:
        valid_rule_hashes, declared_names = _load_declared_rules(rules_script)
    except Exception as exc:
        raise SystemExit(f"error: {exc}") from exc
    with _acquire_lock(nodes_dir):
        entries, non_current = _scan_entries(nodes_dir)

        # A rule name on disk that the script never names is more often a
        # forgotten import than a deleted rule. Treat those hashes as valid so
        # neither they nor their descendants are collected, unless the caller
        # asks for it explicitly.
        unknown_names = sorted({_rule_name(key) for key in entries} - declared_names)
        if unknown_names and not prune_unknown_rules:
            valid_rule_hashes |= {
                entry["rule_hash"]
                for key, entry in entries.items()
                if _rule_name(key) in unknown_names
            }

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
        if unknown_names:
            state = "pruning" if prune_unknown_rules else "preserved"
            print(f"Rules absent from {rules_script} ({state}):")
            for name in unknown_names:
                print(name)
            if not prune_unknown_rules:
                print("Pass --prune-unknown-rules to collect their nodes.")
            print()
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
