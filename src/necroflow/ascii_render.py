from __future__ import annotations

from collections.abc import Callable

from necroflow.nodes import Node

# Maps frozenset of active directions {U,D,L,R} to box-drawing char
_BOX = {
    frozenset("UD"): "│",
    frozenset("LR"): "─",
    frozenset("UR"): "└",
    frozenset("UL"): "┘",
    frozenset("DR"): "┌",
    frozenset("DL"): "┐",
    frozenset("ULR"): "┴",
    frozenset("DLR"): "┬",
    frozenset("UDR"): "├",
    frozenset("UDL"): "┤",
    frozenset("UDLR"): "┼",
}


def _render_connector(edges: list[tuple[int, int]]) -> list[str]:
    """Return three rows connecting src columns to dst columns with box-drawing chars."""
    max_col = max(max(s, d) for s, d in edges) + 1
    dirs: list[set[str]] = [set() for _ in range(max_col)]

    for s, d in edges:
        if s == d:
            dirs[s] |= {"U", "D"}
        else:
            lo, hi = min(s, d), max(s, d)
            dirs[s] |= {"U", "R" if d > s else "L"}
            dirs[d] |= {"D", "L" if d > s else "R"}
            for c in range(lo + 1, hi):
                dirs[c] |= {"L", "R"}

    row1 = [" "] * max_col
    row2 = [" "] * max_col
    row3 = [" "] * max_col

    for s, _ in edges:
        row1[s] = "│"
    for c, d in enumerate(dirs):
        row2[c] = _BOX.get(frozenset(d), " ")
    for _, d in edges:
        row3[d] = "▼"

    return ["".join(row1), "".join(row2), "".join(row3)]


def _node_label(node: Node) -> str:
    parts = [node.rule.__name__]
    suffix = node.node_type.__name__
    if node.output_name and node.output_name != suffix:
        suffix += f":{node.output_name}" if suffix else node.output_name
    if suffix:
        parts[0] += f"[{suffix}]"
    if node.rule_call.mutable:
        parts.append("[mutable]")
    # needs human review: config omitted from label because long embedded
    # config values can otherwise make the ASCII DAG unreadable.
    if node.rule.constraints:
        resources = ", ".join(
            f"{key}={value}" for key, value in node.rule.constraints.items()
        )
        parts.append(f"[{resources}]")
    return " ".join(parts)


def render_ascii(
    nodes: list[Node],
    header: str,
    *,
    label: Callable[[Node], str] = _node_label,
) -> str:
    """Render a topologically layered ASCII box-and-arrow diagram of nodes."""
    from collections import defaultdict, deque

    nodes_by_key = {n.relative_path: n for n in nodes}
    node_keys = set(nodes_by_key)

    # build forward edges and compute depth via Kahn's topo sort
    children: dict[object, list[object]] = {key: [] for key in node_keys}
    in_degree: dict[object, int] = {key: 0 for key in node_keys}
    for n in nodes:
        for p in n.parents:
            if p.relative_path in node_keys:
                children[p.relative_path].append(n.relative_path)
                in_degree[n.relative_path] += 1

    depth: dict[object, int] = {}
    queue: deque[object] = deque(key for key in node_keys if in_degree[key] == 0)
    for key in queue:
        depth[key] = 0
    while queue:
        key = queue.popleft()
        for ckey in children[key]:
            depth[ckey] = max(depth.get(ckey, -1), depth[key] + 1)
            in_degree[ckey] -= 1
            if in_degree[ckey] == 0:
                queue.append(ckey)

    layers: dict[int, list[object]] = defaultdict(list)
    for key, d in depth.items():
        layers[d].append(key)

    labels = {key: label(nodes_by_key[key]) for key in node_keys}
    raw_edges = [
        (p.relative_path, n.relative_path)
        for n in nodes
        for p in n.parents
        if p.relative_path in node_keys
    ]

    # Insert dummy pass-through nodes for long-range edges (span > 1 layer).
    # Each dummy key is a fresh sentinel object, distinct from every
    # relative_path by type, so it needs no reserved numbering scheme to
    # avoid colliding with a real node's key.
    dummy_keys: set[object] = set()
    routing_edges: list[tuple[object, object]] = []
    for u, v in raw_edges:
        if depth[v] - depth[u] <= 1:
            routing_edges.append((u, v))
        else:
            prev = u
            for d in range(depth[u] + 1, depth[v]):
                dkey = object()
                dummy_keys.add(dkey)
                layers[d].append(dkey)
                depth[dkey] = d
                routing_edges.append((prev, dkey))
                prev = dkey
            routing_edges.append((prev, v))

    GAP = 3
    lines: list[str] = [header + "\n"]
    centre_x: dict[object, int] = {}
    layer_rows: list[tuple[str, str, str]] = []

    for d in sorted(layers):
        keys = layers[d]
        tops, mids, bots = [], [], []
        x = 0
        for key in keys:
            if key in dummy_keys:
                tops.append(" ")
                mids.append("│")
                bots.append(" ")
                centre_x[key] = x
                x += 1 + GAP
            else:
                lbl = labels[key]
                w = len(lbl) + 2
                tops.append("┌" + "─" * w + "┐")
                mids.append("│ " + lbl + " │")
                bots.append("└" + "─" * w + "┘")
                centre_x[key] = x + (w + 2) // 2
                x += w + 2 + GAP
        layer_rows.append(("   ".join(tops), "   ".join(mids), "   ".join(bots)))

    for li, (top, mid, bot) in enumerate(layer_rows):
        lines.extend([top, mid, bot])
        d = li
        if d + 1 not in layers:
            continue
        cur_keys = set(layers[d])
        nxt_keys = set(layers[d + 1])
        col_edges = [
            (centre_x[u], centre_x[v])
            for u, v in routing_edges
            if u in cur_keys and v in nxt_keys
        ]
        if not col_edges:
            lines.append("")
            continue
        lines.extend(_render_connector(col_edges))

    return "\n".join(lines)


def write_ancestor_graph(node) -> None:
    """Write an ASCII subgraph of node + all ancestors to .rip/graph.txt."""
    seen: dict = {}
    frontier = [node]
    while frontier:
        n = frontier.pop()
        if n.relative_path in seen:
            continue
        seen[n.relative_path] = n
        frontier.extend(n.parents)
    ancestors = list(seen.values())
    n = len(ancestors)
    header = f"Provenance  {n} node{'s' if n != 1 else ''}"
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "graph.txt").write_text(
        render_ascii(ancestors, header) + "\n", encoding="utf-8"
    )
