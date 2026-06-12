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


class Pipeline:
    def __init__(self):
        object.__setattr__(self, "nodes", [])
        object.__setattr__(self, "_node_names", {})

    def __setattr__(self, name, value):
        from necroflow.dag import Node

        node_names = object.__getattribute__(self, "_node_names")
        if name in node_names:
            raise ValueError(f"Pipeline attribute {name!r} already assigned")
        nodes = object.__getattribute__(self, "nodes")
        if isinstance(value, Node):
            nodes.append(value)
            node_names[name] = value
        elif isinstance(value, tuple) and any(isinstance(v, Node) for v in value):
            for v in value:
                if isinstance(v, Node):
                    nodes.append(v)
            node_names[name] = value
        object.__setattr__(self, name, value)

    def _label(self, node: Node) -> str:
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

        labels = {nid: self._label(G.nodes[nid]["node"]) for nid in G.nodes}

        GAP = 3
        lines: list[str] = [f"Pipeline  {len(self.nodes)} nodes\n"]
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

    def __repr__(self) -> str:
        return str(self)

    def plot(self, **fig_kw):
        import networkx as nx
        import matplotlib.pyplot as plt

        G = nx.DiGraph()
        for node in self.nodes:
            G.add_node(id(node), node=node)
            for parent in node.parents:
                G.add_edge(id(parent), id(node))

        depth: dict[int, int] = {}
        for nid in nx.topological_sort(G):
            preds = list(G.predecessors(nid))
            depth[nid] = max((depth[p] for p in preds), default=-1) + 1

        from collections import defaultdict

        layers: dict[int, list[int]] = defaultdict(list)
        for nid, d in depth.items():
            layers[d].append(nid)

        pos: dict[int, tuple[float, float]] = {}
        for d, nids in layers.items():
            for i, nid in enumerate(nids):
                x = i - (len(nids) - 1) / 2
                pos[nid] = (x, -d)

        labels = {nid: self._label(G.nodes[nid]["node"]) for nid in G.nodes}

        fig, ax = plt.subplots(**fig_kw)
        nx.draw(
            G,
            pos=pos,
            labels=labels,
            ax=ax,
            with_labels=True,
            node_color="steelblue",
            node_size=2000,
            font_color="white",
            font_size=9,
            arrows=True,
            arrowsize=20,
            edge_color="gray",
        )
        plt.tight_layout()
        plt.show()
