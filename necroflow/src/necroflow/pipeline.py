from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from necroflow.dag import Node

_active_pipeline: ContextVar[Pipeline | None] = ContextVar(
    "_active_pipeline", default=None
)


class Pipeline:
    def __init__(self):
        self.nodes: list[Node] = []

    def __enter__(self) -> Pipeline:
        self._token = _active_pipeline.set(self)
        return self

    def __exit__(self, *_):
        _active_pipeline.reset(self._token)

    def _register(self, node: Node):
        self.nodes.append(node)

    def plot(self, **fig_kw):
        import networkx as nx
        import matplotlib.pyplot as plt

        G = nx.DiGraph()
        for node in self.nodes:
            G.add_node(id(node), node=node)
            for parent in node.parents:
                G.add_edge(id(parent), id(node))

        # Layered (topological) layout: depth from roots
        depth: dict[int, int] = {}
        for nid in nx.topological_sort(G):
            preds = list(G.predecessors(nid))
            depth[nid] = max((depth[p] for p in preds), default=-1) + 1

        # Group by depth, assign x within each layer
        from collections import defaultdict

        layers: dict[int, list[int]] = defaultdict(list)
        for nid, d in depth.items():
            layers[d].append(nid)

        pos: dict[int, tuple[float, float]] = {}
        for d, nids in layers.items():
            for i, nid in enumerate(nids):
                x = i - (len(nids) - 1) / 2
                pos[nid] = (x, -d)

        labels = {
            nid: (
                f"{G.nodes[nid]['node'].rule.__name__}\n"
                f"[{G.nodes[nid]['node'].output_name}]"
                if G.nodes[nid]["node"].output_name
                else G.nodes[nid]["node"].rule.__name__
            )
            for nid in G.nodes
        }

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
