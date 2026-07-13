"""Render a DAG as a graphviz PNG, laid out like a software architecture
diagram: orthogonal edges, nodes clustered by author-declared pipeline section when
available, otherwise into bands by dependency depth.

Optional feature — requires the 'dev' extra (`pip install necroflow[dev]`)
for networkx, and the system `dot` binary (graphviz) on PATH.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def _dot_id(nid: str) -> str:
    return '"' + nid.replace('"', '\\"') + '"'


def _dot_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _group_key(node_key: str) -> str:
    """Co-outputs of one rule call share 'rule/fingerprint/' — group on that."""
    return node_key.rsplit("/", 1)[0]


def render_png(
    dag, *, output_path: Path, title: str | None = None, dpi: int = 170
) -> None:
    """Render `dag` (a necroflow DAG, after resolve_paths) to a PNG at output_path.

    Raises SystemExit with an actionable message if networkx isn't installed
    or the `dot` binary isn't on PATH.
    """
    try:
        import networkx as nx
    except ImportError as exc:
        raise SystemExit(
            "error: PNG rendering requires the 'dev' extra "
            "(pip install -e 'git/necroflow[dev]') for networkx"
        ) from exc

    if shutil.which("dot") is None:
        raise SystemExit(
            "error: PNG rendering requires the graphviz 'dot' binary on PATH "
            "(e.g. apt install graphviz)"
        )

    requested_keys = {node.key for node in dag.required_nodes}

    groups: dict[str, dict] = {}
    order: list[str] = []
    for node in dag.nodes:
        gid = _group_key(node.key)
        if gid not in groups:
            groups[gid] = {
                "rule": node.rule.__name__ if node.rule else "unknown",
                "threads": 1,
                "requested": False,
                "outputs": [],
                "sections": set(),
            }
            order.append(gid)
        g = groups[gid]
        g["requested"] = g["requested"] or node.key in requested_keys
        threads = dict(getattr(node.rule, "constraints", {}) or {}).get("threads", 1)
        g["threads"] = max(g["threads"], threads)
        g["outputs"].append(node.output_name or "")
        g["sections"].add(dag.section_for(node))

    edges: set[tuple[str, str]] = set()
    for node in dag.nodes:
        t = _group_key(node.key)
        for parent in node.parents:
            s = _group_key(parent.key)
            if s != t:
                edges.add((s, t))

    G = nx.DiGraph()
    G.add_nodes_from(order)
    G.add_edges_from(edges)
    if not nx.is_directed_acyclic_graph(G):
        raise SystemExit("error: DAG is not acyclic (unexpected)")

    depth: dict[str, int] = {}
    for d, generation in enumerate(nx.topological_generations(G)):
        for gid in generation:
            depth[gid] = d

    lines: list[str] = ["digraph necroflow_dag {"]
    lines.append("  rankdir=TB;")
    lines.append('  bgcolor="white";')
    lines.append("  compound=true;")
    lines.append("  splines=ortho;")
    lines.append("  nodesep=0.32;")
    lines.append("  ranksep=0.55;")
    lines.append('  fontname="Helvetica";')
    if title:
        stats = f"{len(order)} rules &#183; {len(edges)} edges"
        lines.append(
            f'  label=<<b>{title}</b><br/><font point-size="11" color="#66707c">{stats}</font>>;'
        )
        lines.append("  labelloc=t;")
        lines.append("  fontsize=18;")
    lines.append('  node [fontname="Helvetica", fontsize=11];')
    lines.append('  edge [color="#9aa4b2", arrowsize=0.75, penwidth=1.1];')
    lines.append("")

    use_sections = all(
        len(g["sections"]) == 1 and None not in g["sections"] for g in groups.values()
    )
    if use_sections:
        by_section: dict[str, list[str]] = {}
        for gid in order:
            section = next(iter(groups[gid]["sections"]))
            by_section.setdefault(section, []).append(gid)
        layout_groups = list(by_section.items())
    else:
        by_depth: dict[int, list[str]] = {}
        for gid, d in depth.items():
            by_depth.setdefault(d, []).append(gid)
        layout_groups = [(None, by_depth[d]) for d in sorted(by_depth)]

    for index, (section, gids) in enumerate(layout_groups):
        if section is None:
            lines.append("  { rank=same;")
        else:
            lines.append(f"  subgraph cluster_section_{index} {{")
            lines.append(f'    label="{_dot_text(section)}";')
            lines.append('    style="rounded";')
            lines.append('    color="#c3cad2";')
            lines.append("    margin=12;")
        for gid in gids:
            g = groups[gid]
            is_source = G.in_degree(gid) == 0
            label_lines = [g["rule"]] + [o for o in g["outputs"] if o]
            if g["threads"] > 1:
                label_lines.append(f"threads={g['threads']}")
            label = "\\n".join(label_lines)
            if g["requested"]:
                shape, style, color, penwidth = (
                    "box",
                    "rounded,filled,bold",
                    "#c33d2c",
                    2.4,
                )
                fill = "#fff3ef"
            elif is_source:
                shape, style, color, penwidth = "cylinder", "filled", "#5b6672", 1.3
                fill = "#ffffff"
            else:
                shape, style, color, penwidth = "box", "rounded,filled", "#7c8797", 1.3
                fill = "#ffffff"
            lines.append(
                f'    {_dot_id(gid)} [label="{label}", shape={shape}, style="{style}", '
                f'fillcolor="{fill}", color="{color}", penwidth={penwidth}];'
            )
        lines.append("  }")
        lines.append("")

    for s, t in sorted(edges):
        lines.append(f"  {_dot_id(s)} -> {_dot_id(t)};")
    lines.append("}")

    dot_src = "\n".join(lines)
    subprocess.run(
        ["dot", "-Tpng", f"-Gdpi={dpi}", "-o", str(output_path)],
        input=dot_src,
        text=True,
        check=True,
    )
