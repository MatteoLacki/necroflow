from __future__ import annotations

from necroflow.nodes import Node
from necroflow.dag import resolve_paths as _resolve_paths

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


def _label(node: Node) -> str:
    parts = [node.rule.__name__]
    suffix = ""
    if node.node_type:
        suffix = node.node_type.__name__
    if node.output_name and node.output_name != suffix:
        suffix += f":{node.output_name}" if suffix else node.output_name
    if suffix:
        parts[0] += f"[{suffix}]"
    if node.config:
        cfg = ", ".join(f"{k}={v!r}" for k, v in node.config.items())
        parts.append(f"({cfg})")
    if node.rule and node.rule.constraints:
        res = ", ".join(f"{k}={v}" for k, v in node.rule.constraints.items())
        parts.append(f"[{res}]")
    return " ".join(parts)


def _sinks(pipeline: Pipeline) -> list:
    """Nodes with no dependents (children) in the pipeline — includes source nodes."""
    is_parent = {p.key for n in pipeline.nodes for p in n.parents}
    return [n for n in pipeline.nodes if n.key not in is_parent]


class _GraphBase:
    """Shared rendering logic for Pipeline and DAG."""

    @property
    def nodes(self) -> list:
        raise NotImplementedError

    def _header(self) -> str:
        raise NotImplementedError

    def _node_label(self, node: Node) -> str:
        return _label(node)

    def _node_color(self, node: Node) -> str:
        return "steelblue"

    def resolve_paths(self, outdir) -> None:
        _resolve_paths(self.nodes, outdir)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        from collections import defaultdict, deque

        nodes = list(self.nodes)
        id_to_node = {id(n): n for n in nodes}
        node_ids = set(id_to_node)

        # build forward edges and compute depth via Kahn's topo sort
        children: dict[int, list[int]] = {nid: [] for nid in node_ids}
        in_degree: dict[int, int] = {nid: 0 for nid in node_ids}
        for n in nodes:
            for p in n.parents:
                if id(p) in node_ids:
                    children[id(p)].append(id(n))
                    in_degree[id(n)] += 1

        depth: dict[int, int] = {}
        queue: deque[int] = deque(nid for nid in node_ids if in_degree[nid] == 0)
        for nid in queue:
            depth[nid] = 0
        while queue:
            nid = queue.popleft()
            for cid in children[nid]:
                depth[cid] = max(depth.get(cid, -1), depth[nid] + 1)
                in_degree[cid] -= 1
                if in_degree[cid] == 0:
                    queue.append(cid)

        layers: dict[int, list[int]] = defaultdict(list)
        for nid, d in depth.items():
            layers[d].append(nid)

        labels = {nid: self._node_label(id_to_node[nid]) for nid in node_ids}
        raw_edges = [(id(p), id(n)) for n in nodes for p in n.parents if id(p) in node_ids]

        # Insert dummy pass-through nodes for long-range edges (span > 1 layer)
        dummy_ids: set[int] = set()
        routing_edges: list[tuple[int, int]] = []
        dummy_counter = 0
        for u, v in raw_edges:
            if depth[v] - depth[u] <= 1:
                routing_edges.append((u, v))
            else:
                prev = u
                for d in range(depth[u] + 1, depth[v]):
                    did = -(dummy_counter + 1)
                    dummy_counter += 1
                    dummy_ids.add(did)
                    layers[d].append(did)
                    depth[did] = d
                    routing_edges.append((prev, did))
                    prev = did
                routing_edges.append((prev, v))

        GAP = 3
        lines: list[str] = [self._header() + "\n"]
        centre_x: dict[int, int] = {}
        layer_rows: list[tuple[str, str, str]] = []

        for d in sorted(layers):
            nids = layers[d]
            tops, mids, bots = [], [], []
            x = 0
            for nid in nids:
                if nid in dummy_ids:
                    tops.append(" ")
                    mids.append("│")
                    bots.append(" ")
                    centre_x[nid] = x
                    x += 1 + GAP
                else:
                    lbl = labels[nid]
                    w = len(lbl) + 2
                    tops.append("┌" + "─" * w + "┐")
                    mids.append("│ " + lbl + " │")
                    bots.append("└" + "─" * w + "┘")
                    centre_x[nid] = x + (w + 2) // 2
                    x += w + 2 + GAP
            layer_rows.append(("   ".join(tops), "   ".join(mids), "   ".join(bots)))

        for li, (top, mid, bot) in enumerate(layer_rows):
            lines.extend([top, mid, bot])
            d = li
            if d + 1 not in layers:
                continue
            cur_nids = set(layers[d])
            nxt_nids = set(layers[d + 1])
            col_edges = [
                (centre_x[u], centre_x[v])
                for u, v in routing_edges
                if u in cur_nids and v in nxt_nids
            ]
            if not col_edges:
                lines.append("")
                continue
            lines.extend(_render_connector(col_edges))

        return "\n".join(lines)

    def save(self, path) -> None:
        """Write the ASCII DAG render to a file."""
        from pathlib import Path
        Path(path).write_text(str(self) + "\n", encoding="utf-8")




class Pipeline(_GraphBase):
    def __init__(self):
        self._nodes_list = []
        self._node_names = {}

    @property
    def nodes(self) -> list:
        return self._nodes_list

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if name.startswith("."):
            raise ValueError(f"Pipeline attribute {name!r} must not start with '.'")
        if name in self._node_names:
            raise ValueError(f"Pipeline attribute {name!r} already assigned")
        if isinstance(value, Node):
            self._nodes_list.append(value)
            self._node_names[name] = value
            value.pipeline_label = name
        object.__setattr__(self, name, value)

    def _header(self) -> str:
        return f"Pipeline  {len(self.nodes)} nodes"


class DAG(_GraphBase):
    """Aggregator for multiple pipelines. Stores nodes by content-addressed hash;
    deduplicates shared upstream computations automatically."""

    def __init__(self, outdir=None):
        from pathlib import Path
        self._nodes: dict[str, object] = {}   # key -> canonical Node
        self._all_nodes: list = []            # all nodes including duplicates
        self._required: set[str] = set()
        self.outdir = Path(outdir) if outdir is not None else Path.cwd()

    def add(self, pipeline: Pipeline, request=None) -> None:
        """Add a pipeline's nodes. request defaults to pipeline sinks."""
        if request is None:
            request = _sinks(pipeline)
        for node in pipeline.nodes:
            self._nodes.setdefault(node.key, node)
            self._all_nodes.append(node)
        for node in request:
            self._required.add(node.key)

    @property
    def nodes(self) -> list:
        return list(self._nodes.values())

    @property
    def required_nodes(self) -> list:
        return [n for h, n in self._nodes.items() if h in self._required]

    def resolve_paths(self, outdir) -> None:
        _resolve_paths(self._all_nodes, outdir)

    def _header(self) -> str:
        return f"DAG  {len(self._nodes)} nodes  ({len(self._required)} required)"

    def _node_label(self, node: Node) -> str:
        required_keys = {n.key for n in self.required_nodes}
        return _label(node) + (" ★" if node.key in required_keys else "")

    def _node_color(self, node: Node) -> str:
        required_keys = {n.key for n in self.required_nodes}
        return "orange" if node.key in required_keys else "steelblue"

    def execute(self, **kwargs) -> None:
        from necroflow.executor import execute
        execute(self, self.outdir, **kwargs)
