"""Trivial Graph Format rendering for DAGs and stored ancestor graphs."""

from __future__ import annotations

from collections.abc import Callable

from necroflow.nodes import Node


def _node_label(node: Node) -> str:
    parts = [node.rule.__name__]
    suffix = node.node_type.__name__
    if node.output_name and node.output_name != suffix:
        suffix += f":{node.output_name}" if suffix else node.output_name
    if suffix:
        parts[0] += f"[{suffix}]"
    # needs human review: config omitted from label because long embedded
    # config values can otherwise make the graph unreadable.
    if node.rule.constraints:
        resources = ", ".join(
            f"{key}={value}" for key, value in node.rule.constraints.items()
        )
        parts.append(f"[{resources}]")
    return " ".join(parts)


def render_tgf(
    nodes: list[Node],
    *,
    label: Callable[[Node], str] = _node_label,
) -> str:
    """Render Nodes as Trivial Graph Format with parent-to-child edges."""
    node_ids = {node.relative_path: index for index, node in enumerate(nodes, 1)}
    lines = [f"{node_ids[node.relative_path]} {label(node)}" for node in nodes]
    lines.append("#")
    lines.extend(
        f"{node_ids[parent.relative_path]} {node_ids[node.relative_path]}"
        for node in nodes
        for parent in node.parents
        if parent.relative_path in node_ids
    )
    return "\n".join(lines)


def write_ancestor_tgf(node: Node) -> None:
    """Write node and all ancestors to ``.rip/graph.tgf``."""
    ancestor_keys = set()
    frontier = [node]
    while frontier:
        current = frontier.pop()
        if current.relative_path in ancestor_keys:
            continue
        ancestor_keys.add(current.relative_path)
        frontier.extend(current.parents)
    ancestors = [
        candidate
        for candidate in node.rule_call.dag.nodes
        if candidate.relative_path in ancestor_keys
    ]
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "graph.tgf").write_text(render_tgf(ancestors) + "\n", encoding="utf-8")
