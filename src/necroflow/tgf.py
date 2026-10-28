"""Trivial Graph Format and Graphviz DOT rendering for DAGs and stored
ancestor graphs."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from necroflow.nodes import Node

if TYPE_CHECKING:
    from necroflow.rule_call import RuleCall


def _node_label(node: Node) -> str:
    parts = [node.rule.__name__]
    suffix = node.node_type.__name__
    if node.output_name and node.output_name != suffix:
        suffix += f":{node.output_name}" if suffix else node.output_name
    if suffix:
        parts[0] += f"[{suffix}]"
    # needs human review: config omitted from label because long embedded
    # config values can otherwise make the graph unreadable.
    if node.rule_call.resource_requests:
        resources = ", ".join(
            f"{key}={value}" for key, value in node.rule_call.resource_requests.items()
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


def _dot_string(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def render_dot(
    nodes: list[Node],
    *,
    label: Callable[[Node], str] = _node_label,
) -> str:
    """Render Nodes as Graphviz DOT text with parent-to-child edges.

    Node ids are stable node-store keys. Co-outputs of one RuleCall share a
    dashed cluster, showing which Nodes one command produces together.
    Rendering is left to Graphviz, e.g. `necroflow graph --dot | dot -Tsvg`.
    """
    keys = {node.relative_path for node in nodes}
    calls: dict[Path, list[Node]] = {}
    for node in nodes:
        calls.setdefault(node.rule_call.relative_path, []).append(node)
    lines = ["digraph necroflow {", "  node [shape=box];"]
    for call_key, call_nodes in calls.items():
        declarations = [
            f"{_dot_string(node.relative_path.as_posix())} "
            f"[label={_dot_string(label(node))}];"
            for node in call_nodes
        ]
        if len(call_nodes) == 1:
            lines.append("  " + declarations[0])
            continue
        cluster = _dot_string("cluster_" + call_key.as_posix())
        lines.append(f"  subgraph {cluster} {{")
        lines.append("    style=dashed;")
        lines.extend("    " + declaration for declaration in declarations)
        lines.append("  }")
    lines.extend(
        f"  {_dot_string(parent.relative_path.as_posix())} -> "
        f"{_dot_string(node.relative_path.as_posix())};"
        for node in nodes
        for parent in node.parents
        if parent.relative_path in keys
    )
    lines.append("}")
    return "\n".join(lines)


def write_ancestor_tgf(call: RuleCall) -> None:
    """Write a call's outputs and all their ancestor Nodes to ``.rip/graph.tgf``.

    Ancestors are the parent Nodes actually consumed, transitively; sibling
    outputs of ancestor calls that nothing here consumes are left out.
    """
    seen: dict[Path, Node] = {}
    frontier = list(call.outputs)
    while frontier:
        node = frontier.pop()
        if node.relative_path not in seen:
            seen[node.relative_path] = node
            frontier.extend(node.parents)
    ancestors = call.dag.in_registration_order(seen.values())
    call.metadata_dir.mkdir(parents=True, exist_ok=True)
    graph = call.metadata_dir / "graph.tgf"
    graph.write_text(render_tgf(ancestors) + "\n", encoding="utf-8")
