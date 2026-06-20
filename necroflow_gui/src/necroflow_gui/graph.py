from __future__ import annotations

from dataclasses import dataclass
from html import escape
from urllib.parse import urlencode

from necroflow import Node, Pipeline


@dataclass(frozen=True)
class GraphNode:
    id: str
    label: str
    detail: str
    selected: bool


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str


@dataclass(frozen=True)
class PipelineGraph:
    nodes: list[GraphNode]
    edges: list[GraphEdge]


def build_node_lookup(pipeline: Pipeline) -> dict[str, Node]:
    """Return stable UI ids mapped to live Node objects for a freshly built pipeline."""
    assigned: dict[int, str] = {}
    lookup: dict[str, Node] = {}

    for name, value in vars(pipeline).items():
        if name.startswith("_"):
            continue
        if isinstance(value, Node):
            _assign_node(lookup, assigned, name, value)
        elif isinstance(value, tuple):
            output_names = [v.output_name for v in value if isinstance(v, Node)]
            unique_outputs = len(output_names) == len(set(output_names))
            for index, item in enumerate(value):
                if not isinstance(item, Node):
                    continue
                suffix = item.output_name if unique_outputs and item.output_name else str(index)
                _assign_node(lookup, assigned, f"{name}.{suffix}", item)

    for index, node in enumerate(pipeline.nodes):
        if id(node) not in assigned:
            _assign_node(lookup, assigned, f"node{index}", node)

    return lookup


def extract_graph(pipeline: Pipeline, selected: set[str]) -> PipelineGraph:
    lookup = build_node_lookup(pipeline)
    ids_by_node = {id(node): node_id for node_id, node in lookup.items()}
    nodes: list[GraphNode] = []
    edges: list[GraphEdge] = []

    for node_id, node in lookup.items():
        nodes.append(
            GraphNode(
                id=node_id,
                label=_node_label(node_id, node),
                detail=_node_detail(node),
                selected=node_id in selected,
            )
        )
        for parent in node.parents:
            source = ids_by_node.get(id(parent))
            if source is not None:
                edges.append(GraphEdge(source=source, target=node_id))

    return PipelineGraph(nodes=nodes, edges=edges)


def render_svg(
    graph: PipelineGraph,
    pipeline_id: str,
    config_id: str,
    width: int = 1120,
) -> str:
    positions = _layout(graph)
    node_w = 210
    node_h = 98
    layer_gap_x = 74
    layer_gap_y = 78
    margin = 36
    max_x = max((x for x, _ in positions.values()), default=0)
    max_y = max((y for _, y in positions.values()), default=0)
    svg_w = max(width, margin * 2 + max_x + node_w)
    svg_h = margin * 2 + max_y + node_h

    by_id = {node.id: node for node in graph.nodes}
    parts = [
        f'<svg class="pipeline-graph" viewBox="0 0 {svg_w} {svg_h}" '
        'xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Pipeline graph">',
        "<defs>",
        '<marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" '
        'orient="auto" markerUnits="strokeWidth">',
        '<path d="M0,0 L0,6 L9,3 z" fill="#65758b" />',
        "</marker>",
        "</defs>",
    ]

    for edge in graph.edges:
        if edge.source not in positions or edge.target not in positions:
            continue
        sx, sy = positions[edge.source]
        tx, ty = positions[edge.target]
        x1 = margin + sx + node_w
        y1 = margin + sy + node_h / 2
        x2 = margin + tx
        y2 = margin + ty + node_h / 2
        mid = (x1 + x2) / 2
        parts.append(
            '<path class="edge" '
            f'd="M{x1:.1f},{y1:.1f} C{mid:.1f},{y1:.1f} {mid:.1f},{y2:.1f} {x2:.1f},{y2:.1f}" '
            'marker-end="url(#arrow)" />'
        )

    for node_id, (x, y) in positions.items():
        node = by_id[node_id]
        href = "/toggle?" + urlencode(
            {"pipeline_id": pipeline_id, "config_id": config_id, "node_id": node.id}
        )
        cls = "node selected" if node.selected else "node"
        rx = margin + x
        ry = margin + y
        label_lines = _fit_lines(node.label, width=20, max_lines=2)
        detail_lines = _fit_lines(node.detail, width=25, max_lines=2)
        parts.append(f'<a href="{escape(href, quote=True)}" class="{cls}">')
        parts.append(f'<rect x="{rx}" y="{ry}" width="{node_w}" height="{node_h}" rx="8" />')
        text_y = ry + 25
        for line in label_lines:
            parts.append(
                f'<text class="node-label" x="{rx + 16}" y="{text_y}">{escape(line)}</text>'
            )
            text_y += 17
        text_y += 3
        for line in detail_lines:
            parts.append(
                f'<text class="node-detail" x="{rx + 16}" y="{text_y}">{escape(line)}</text>'
            )
            text_y += 15
        parts.append("</a>")

    parts.append("</svg>")
    return "\n".join(parts)


def _assign_node(
    lookup: dict[str, Node], assigned: dict[int, str], node_id: str, node: Node
) -> None:
    if id(node) in assigned:
        return
    unique_id = node_id
    counter = 2
    while unique_id in lookup:
        unique_id = f"{node_id}-{counter}"
        counter += 1
    lookup[unique_id] = node
    assigned[id(node)] = unique_id


def _node_label(node_id: str, node: Node) -> str:
    rule_name = node.rule.__name__ if node.rule else "unknown"
    output = node.output_name or "output"
    return f"{node_id} {rule_name}.{output}"


def _node_detail(node: Node) -> str:
    type_name = node.node_type.__name__ if node.node_type else "Node"
    if not node.config:
        return type_name
    config = ", ".join(f"{key}={value!r}" for key, value in node.config.items())
    return f"{type_name} {config}"


def _layout(graph: PipelineGraph) -> dict[str, tuple[int, int]]:
    parents: dict[str, list[str]] = {node.id: [] for node in graph.nodes}
    children: dict[str, list[str]] = {node.id: [] for node in graph.nodes}
    for edge in graph.edges:
        parents.setdefault(edge.target, []).append(edge.source)
        children.setdefault(edge.source, []).append(edge.target)

    depth: dict[str, int] = {}
    for node in graph.nodes:
        _depth(node.id, parents, depth)

    layers: dict[int, list[str]] = {}
    for node in graph.nodes:
        layers.setdefault(depth.get(node.id, 0), []).append(node.id)

    node_w = 210
    node_h = 98
    gap_x = 74
    gap_y = 78
    positions: dict[str, tuple[int, int]] = {}
    for layer, ids in sorted(layers.items()):
        for row, node_id in enumerate(ids):
            positions[node_id] = (layer * (node_w + gap_x), row * (node_h + gap_y))
    return positions


def _depth(node_id: str, parents: dict[str, list[str]], cache: dict[str, int]) -> int:
    if node_id in cache:
        return cache[node_id]
    ps = parents.get(node_id, [])
    cache[node_id] = 0 if not ps else max(_depth(parent, parents, cache) for parent in ps) + 1
    return cache[node_id]


def _fit_lines(text: str, width: int, max_lines: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        word = _ellipsize(word, width)
        candidate = word if not current else f"{current} {word}"
        if len(candidate) <= width:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = word
        if len(lines) == max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
    if len(lines) == max_lines and words:
        consumed = " ".join(lines).replace("...", "")
        if len(consumed) < len(text):
            lines[-1] = _ellipsize(lines[-1], width)
    return lines or [""]


def _ellipsize(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    if width <= 3:
        return "." * width
    return text[: width - 3] + "..."
