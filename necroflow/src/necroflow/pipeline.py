from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from necroflow.dag import Node

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
    is_parent = {id(p) for n in pipeline.nodes for p in n.parents}
    return [n for n in pipeline.nodes if id(n) not in is_parent]


class _GraphBase:
    """Shared rendering logic for Pipeline and DAG."""

    @property
    def nodes(self) -> list:
        raise NotImplementedError

    def _header(self) -> str:
        raise NotImplementedError

    def _node_label(self, node: Node, nid: int) -> str:
        return _label(node)

    def _node_color(self, nid: int) -> str:
        return "steelblue"

    def resolve_paths(self, outdir) -> None:
        from necroflow.dag import resolve_paths
        resolve_paths(self.nodes, outdir)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        from collections import defaultdict
        import networkx as nx

        G = nx.DiGraph()
        for node in self.nodes:
            G.add_node(id(node), node=node)
            for parent in node.parents:
                G.add_edge(id(parent), id(node))

        depth: dict[int, int] = {}
        for nid in nx.topological_sort(G):
            preds = list(G.predecessors(nid))
            depth[nid] = max((depth[p] for p in preds), default=-1) + 1

        layers: dict[int, list[int]] = defaultdict(list)
        for nid, d in depth.items():
            layers[d].append(nid)

        labels = {nid: self._node_label(G.nodes[nid]["node"], nid) for nid in G.nodes}

        GAP = 3
        lines: list[str] = [self._header() + "\n"]
        centre_x: dict[int, int] = {}
        layer_rows: list[tuple[str, str, str]] = []

        for d in sorted(layers):
            nids = layers[d]
            tops, mids, bots = [], [], []
            x = 0
            for nid in nids:
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
                for u, v in G.edges()
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

    def plot(self, **fig_kw) -> None:
        import networkx as nx
        import matplotlib.pyplot as plt
        from collections import defaultdict

        G = nx.DiGraph()
        for node in self.nodes:
            G.add_node(id(node), node=node)
            for parent in node.parents:
                G.add_edge(id(parent), id(node))

        depth: dict[int, int] = {}
        for nid in nx.topological_sort(G):
            preds = list(G.predecessors(nid))
            depth[nid] = max((depth[p] for p in preds), default=-1) + 1

        layers: dict[int, list[int]] = defaultdict(list)
        for nid, d in depth.items():
            layers[d].append(nid)

        pos: dict[int, tuple[float, float]] = {}
        for d, nids in layers.items():
            for i, nid in enumerate(nids):
                pos[nid] = (i - (len(nids) - 1) / 2, -d)

        labels = {nid: self._node_label(G.nodes[nid]["node"], nid) for nid in G.nodes}
        colors = [self._node_color(nid) for nid in G.nodes]

        fig, ax = plt.subplots(**fig_kw)
        nx.draw(
            G,
            pos=pos,
            labels=labels,
            ax=ax,
            with_labels=True,
            node_color=colors,
            node_size=2000,
            font_color="white",
            font_size=9,
            arrows=True,
            arrowsize=20,
            edge_color="gray",
        )
        plt.tight_layout()
        plt.show()


class Pipeline(_GraphBase):
    def __init__(self):
        object.__setattr__(self, "_nodes_list", [])
        object.__setattr__(self, "_node_names", {})

    @property
    def nodes(self) -> list:
        return object.__getattribute__(self, "_nodes_list")

    def __setattr__(self, name, value):
        from necroflow.dag import Node

        node_names = object.__getattribute__(self, "_node_names")
        if name in node_names:
            raise ValueError(f"Pipeline attribute {name!r} already assigned")
        nodes = object.__getattribute__(self, "_nodes_list")
        if isinstance(value, Node):
            nodes.append(value)
            node_names[name] = value
            value.pipeline_label = name
        object.__setattr__(self, name, value)

    def _header(self) -> str:
        return f"Pipeline  {len(self.nodes)} nodes"


class DAG(_GraphBase):
    """Aggregator for multiple pipelines. Stores nodes by content-addressed hash;
    deduplicates shared upstream computations automatically."""

    def __init__(self, outdir=None):
        from pathlib import Path
        self._nodes: dict[str, object] = {}   # hash -> Node
        self._required: set[str] = set()
        self.outdir = Path(outdir) if outdir is not None else Path.cwd()

    def add(self, pipeline: Pipeline, request=None) -> None:
        """Add a pipeline's nodes. request defaults to pipeline sinks."""
        from necroflow.dag import _node_key

        if request is None:
            request = _sinks(pipeline)
        for node in pipeline.nodes:
            self._nodes[_node_key(node)] = node
        for node in request:
            self._required.add(_node_key(node))

    @property
    def nodes(self) -> list:
        return list(self._nodes.values())

    @property
    def required_nodes(self) -> list:
        return [n for h, n in self._nodes.items() if h in self._required]

    def _header(self) -> str:
        return f"DAG  {len(self._nodes)} nodes  ({len(self._required)} required)"

    def _node_label(self, node: Node, nid: int) -> str:
        return _label(node) + (" ★" if nid in {id(n) for n in self.required_nodes} else "")

    def _node_color(self, nid: int) -> str:
        return "orange" if nid in {id(n) for n in self.required_nodes} else "steelblue"

    def execute(self, total_threads=None, scheduler=None, keep_going=False, autoclean=False) -> None:
        from necroflow.executor import execute
        execute(self, self.outdir, total_threads, scheduler, keep_going, autoclean)
