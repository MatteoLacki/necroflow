"""Invocation-local cache classification for DAG execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from necroflow.dag import (
    DAG,
    _content_hash,
    _has_changed_invalidation,
    _output_mtime,
)
from necroflow.nodes import Node, NodeState, _topo_sort

Reason = dict[str, object]


@dataclass
class ExecutionPlan:
    """One invocation's classified Nodes and their recorded reasons."""

    active: list[Node]
    orphans: list[Node]
    reasons: dict[Path, tuple[Reason, ...]]

    @property
    def active_keys(self) -> set[Path]:
        return {node.relative_path for node in self.active}


def _parent_content_changed(node: Node, parent: Node) -> bool:
    """Return whether a newer parent differs from its stored successful hash."""
    if node.path is None or parent.path is None or not parent.path.exists():
        return False
    if _output_mtime(parent.path) <= _output_mtime(node.path):
        return False
    hash_file = parent.path.parent / ".rip" / (parent.path.name + ".hash")
    return not (
        hash_file.exists()
        and _content_hash(parent.path) == hash_file.read_text().strip()
    )


def classify_nodes(
    nodes: list[Node], required_nodes: list[Node]
) -> tuple[dict[Path, Path], set[Path]]:
    """Classify base cache state and return evidence used for stale decisions."""
    required: dict[Path, Node] = {}
    frontier = list(required_nodes)
    while frontier:
        node = frontier.pop()
        if node.relative_path in required:
            continue
        required[node.relative_path] = node
        frontier.extend(
            parent for parent in node.parents if parent.relative_path not in required
        )

    for node in nodes:
        if node.relative_path not in required:
            node.state = (
                NodeState.ORPHAN
                if node.path is not None and node.path.exists()
                else None
            )

    changed_parents: dict[Path, Path] = {}
    changed_invalidators: set[Path] = set()
    for node in _topo_sort(list(required.values())):
        if node.path is None or not node.path.exists():
            node.state = NodeState.MISSING
            continue

        stale = any(
            parent.state in (NodeState.MISSING, NodeState.STALE)
            for parent in node.parents
            if parent.state is not None
        )
        if not stale:
            for parent in node.parents:
                if parent.mutable:
                    continue
                if _parent_content_changed(node, parent):
                    changed_parents[node.relative_path] = parent.relative_path
                    stale = True
                    break

        if _has_changed_invalidation(node):
            changed_invalidators.add(node.relative_path)
            stale = True

        node.state = NodeState.STALE if stale else NodeState.UP_TO_DATE

    return changed_parents, changed_invalidators


def _propagate_stale(active: list[Node], active_keys: set[Path]) -> None:
    """Propagate explicit STALE state into active cached descendants."""
    changed = True
    while changed:
        changed = False
        for node in active:
            if node.state != NodeState.UP_TO_DATE:
                continue
            if any(
                parent.relative_path in active_keys and parent.state == NodeState.STALE
                for parent in node.parents
            ):
                node.state = NodeState.STALE
                changed = True


def _parent_reason(node: Node, parent: Node, kind: str) -> Reason:
    reason: Reason = {
        "kind": kind,
        "parent_key": parent.relative_path.as_posix(),
        "parent_label": parent.rule_call.dag.label_for(parent),
    }
    if kind == "parent_not_up_to_date":
        reason["parent_state"] = parent.state.value if parent.state else None
    return reason


def _reasons_for(
    node: Node,
    *,
    forced: set[Path],
    compromised: set[Path],
    changed_parents: dict[Path, Path],
    changed_invalidators: set[Path],
    include_advisories: bool,
) -> tuple[Reason, ...]:
    if node.state == NodeState.MISSING:
        return ({"kind": "output_missing", "path": str(node.path)},)

    if node.state == NodeState.UP_TO_DATE:
        reasons: list[Reason] = [{"kind": "up_to_date"}]
        if include_advisories:
            for parent in node.parents:
                if not parent.mutable:
                    continue
                try:
                    if _parent_content_changed(node, parent):
                        reasons.append(
                            _parent_reason(
                                node, parent, "mutable_parent_content_ignored"
                            )
                        )
                except OSError as exc:
                    reasons.append(
                        {
                            "kind": "parent_check_error",
                            "parent_key": parent.relative_path.as_posix(),
                            "error": str(exc),
                        }
                    )
        return tuple(reasons)

    reasons = []
    if node.relative_path in forced:
        reasons.append({"kind": "forced_invalidation"})
    if node.relative_path in compromised:
        reasons.append({"kind": "compromised_prior_state"})
    if node.relative_path in changed_invalidators:
        reasons.append({"kind": "invalidator_changed"})

    changed_parent = changed_parents.get(node.relative_path)
    for parent in node.parents:
        if parent.state in (NodeState.MISSING, NodeState.STALE):
            reasons.append(_parent_reason(node, parent, "parent_not_up_to_date"))
        elif parent.relative_path == changed_parent:
            reasons.append(_parent_reason(node, parent, "parent_content_changed"))

    if node.state == NodeState.STALE and not reasons:
        reasons.append({"kind": "stale"})
    return tuple(reasons)


def plan_execution(
    dag: DAG,
    *,
    forced_stale_keys: set[Path] | None = None,
    include_advisories: bool = False,
) -> ExecutionPlan:
    """Classify one execution snapshot without deleting or executing outputs."""
    nodes = list(dag.nodes)
    changed_parents, changed_invalidators = classify_nodes(nodes, dag.required_nodes)

    active = [
        node
        for node in nodes
        if node.state is not None and node.state != NodeState.ORPHAN
    ]
    active_keys = {node.relative_path for node in active}

    forced: set[Path] = set()
    if forced_stale_keys:
        for node in active:
            if (
                node.relative_path in forced_stale_keys
                and node.state == NodeState.UP_TO_DATE
            ):
                node.state = NodeState.STALE
                forced.add(node.relative_path)
        _propagate_stale(active, active_keys)

    compromised: set[Path] = set()
    for node in active:
        if node.state == NodeState.UP_TO_DATE and node.is_compromised:
            node.state = NodeState.STALE
            compromised.add(node.relative_path)
    if compromised:
        _propagate_stale(active, active_keys)

    reasons = {
        node.relative_path: _reasons_for(
            node,
            forced=forced,
            compromised=compromised,
            changed_parents=changed_parents,
            changed_invalidators=changed_invalidators,
            include_advisories=include_advisories,
        )
        for node in active
    }
    return ExecutionPlan(
        active=active,
        orphans=[node for node in nodes if node.state == NodeState.ORPHAN],
        reasons=reasons,
    )
