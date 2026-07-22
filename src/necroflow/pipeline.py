from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path

from necroflow.nodes import Node
from necroflow.fingerprints import (
    DEFAULT_FINGERPRINT_PROVIDER,
    default_fingerprint,
    validate_fingerprint_function,
)

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
    suffix = node.node_type.__name__
    if node.output_name and node.output_name != suffix:
        suffix += f":{node.output_name}" if suffix else node.output_name
    if suffix:
        parts[0] += f"[{suffix}]"
    # needs human review: config omitted from label — a single long config value
    # (e.g. write_*_config rules embedding a full TOML file as `text=`) used to
    # blow up box width to ~1400 chars and make the ASCII DAG unreadable.
    if node.rule.constraints:
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

    @property
    def nodes_dir(self) -> Path:
        raise NotImplementedError

    def _header(self) -> str:
        raise NotImplementedError

    def _node_label(self, node: Node) -> str:
        return _label(node)

    def _node_color(self, node: Node) -> str:
        return "steelblue"

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
        raw_edges = [
            (id(p), id(n)) for n in nodes for p in n.parents if id(p) in node_ids
        ]

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


class _AncestorView(_GraphBase):
    """Read-only view of a node and all its ancestors, for provenance rendering."""

    def __init__(self, nodes: list) -> None:
        self._nodes = nodes

    @property
    def nodes(self) -> list:
        return self._nodes

    def _header(self) -> str:
        n = len(self._nodes)
        return f"Provenance  {n} node{'s' if n != 1 else ''}"


def write_ancestor_graph(node) -> None:
    """Write an ASCII subgraph of node + all ancestors to .rip/graph.txt."""
    seen: dict = {}
    frontier = [node]
    while frontier:
        n = frontier.pop()
        if n.key in seen:
            continue
        seen[n.key] = n
        frontier.extend(n.parents)
    view = _AncestorView(list(seen.values()))
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "graph.txt").write_text(str(view) + "\n", encoding="utf-8")


class Pipeline(_GraphBase):
    def __init__(
        self,
        nodes_dir: str | Path,
        *,
        fingerprint_function: Callable = default_fingerprint,
        fingerprint_provider: str = DEFAULT_FINGERPRINT_PROVIDER,
        shellpath: str | Path | None = None,
    ):
        self._nodes_list = []
        self._node_names = {}
        self._sections = []
        self._active_section = None
        self._section_by_node_id = {}
        self._nodes_dir = Path(nodes_dir).expanduser().resolve()
        self._fingerprint_function = fingerprint_function
        self._fingerprint_provider = fingerprint_provider
        self._shellpath = _normalize_shellpath(shellpath)
        validate_fingerprint_function(
            fingerprint_function, provider=fingerprint_provider
        )

    @property
    def nodes_dir(self) -> Path:
        return self._nodes_dir

    @property
    def fingerprint_function(self) -> Callable:
        return self._fingerprint_function

    @property
    def fingerprint_provider(self) -> str:
        return self._fingerprint_provider

    @property
    def shellpath(self) -> str | None:
        return self._shellpath

    @property
    def execution_context(self) -> dict[str, str]:
        return {"shellpath": self._shellpath} if self._shellpath is not None else {}

    def section(self, name: str) -> None:
        """Start a named presentation section for subsequently assigned nodes."""
        if not isinstance(name, str):
            raise TypeError("section name must be a string")
        name = name.strip()
        if not name:
            raise ValueError("section name must not be empty")
        if name in self._sections:
            raise ValueError(f"pipeline section {name!r} already exists")
        self._sections.append(name)
        self._active_section = name

    @property
    def sections(self) -> tuple[str, ...]:
        """Declared presentation sections, in author-defined order."""
        return tuple(self._sections)

    def section_for(self, node: Node) -> str | None:
        """Return this pipeline section for node, if assigned."""
        return self._section_by_node_id.get(id(node))

    @property
    def nodes(self) -> list[Node]:
        return self._nodes_list

    def __getattr__(self, name: str) -> Node:
        try:
            return self._node_names[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, name: str) -> Node:
        if not isinstance(name, str):
            raise TypeError("Pipeline label must be a string")
        return self._node_names[name]

    def _assign_node(self, name: str, value: Node) -> None:
        if not isinstance(name, str):
            raise TypeError("Pipeline label must be a string")
        if not name:
            raise ValueError("Pipeline label must not be empty")
        if name.startswith("."):
            raise ValueError(f"Pipeline label {name!r} must not start with '.'")
        if name in self._node_names:
            raise ValueError(f"Pipeline label {name!r} already assigned")
        if value.rule_call.pipeline is not self:
            raise ValueError(
                f"Node assigned as {name!r} was compiled for a different Pipeline"
            )
        self._nodes_list.append(value)
        self._node_names[name] = value
        self._section_by_node_id[id(value)] = self._active_section
        value.pipeline_label = name

    def __setitem__(self, name: str, value: Node) -> None:
        if not isinstance(value, Node):
            raise TypeError(
                f"Pipeline labels require Node values, got {type(value).__name__}"
            )
        self._assign_node(name, value)

    def __setattr__(self, name: str, value: object) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if isinstance(value, Node):
            if any(name in cls.__dict__ for cls in type(self).__mro__):
                raise ValueError(
                    f"Pipeline attribute {name!r} is reserved; use item syntax "
                    f"P[{name!r}] if this label is intentional"
                )
            self._assign_node(name, value)
        object.__setattr__(self, name, value)

    def _header(self) -> str:
        return f"Pipeline  {len(self.nodes)} nodes"


class DAG(_GraphBase):
    """Aggregator for multiple pipelines. Stores nodes by content-addressed hash;
    deduplicates shared upstream computations automatically."""

    def __init__(self, outdir):
        self._nodes: dict[str, object] = {}  # key -> canonical Node
        self._all_nodes: list = []  # all nodes including duplicates
        self._required: set[str] = set()
        self._sections_by_key: dict[str, set[str | None]] = {}
        self._section_by_node_id: dict[int, str | None] = {}
        self.outdir = Path(outdir).expanduser().resolve()
        self.last_execution_report = None

    @property
    def nodes_dir(self) -> Path:
        return self.outdir

    def add(self, pipeline: Pipeline, request=None) -> None:
        """Add a pipeline's nodes. request defaults to pipeline sinks."""
        if pipeline.nodes_dir != self.outdir:
            raise ValueError(
                f"Pipeline node store {pipeline.nodes_dir} does not match "
                f"DAG node store {self.outdir}"
            )
        if request is None:
            request = _sinks(pipeline)
        for node in pipeline.nodes:
            self._nodes.setdefault(node.key, node)
            self._all_nodes.append(node)
            section = pipeline.section_for(node)
            self._section_by_node_id[id(node)] = section
            self._sections_by_key.setdefault(node.key, set()).add(section)
        for node in request:
            self._required.add(node.key)

    @property
    def nodes(self) -> list:
        return list(self._nodes.values())

    @property
    def required_nodes(self) -> list:
        return [n for h, n in self._nodes.items() if h in self._required]

    def section_for(self, node: Node) -> str | None:
        """Return the node section when all contributing pipelines agree."""
        sections = self._sections_by_key.get(node.key, set())
        if len(sections) == 1:
            return next(iter(sections))
        return None

    def _header(self) -> str:
        return f"DAG  {len(self._nodes)} nodes  ({len(self._required)} required)"

    def _node_label(self, node: Node) -> str:
        required_keys = {n.key for n in self.required_nodes}
        return _label(node) + (" ★" if node.key in required_keys else "")

    def _node_color(self, node: Node) -> str:
        required_keys = {n.key for n in self.required_nodes}
        return "orange" if node.key in required_keys else "steelblue"

    def execute(self, **kwargs):
        from necroflow.executor import execute

        self.last_execution_report = execute(self, **kwargs)
        return self.last_execution_report


def _normalize_shellpath(shellpath: str | Path | None) -> str | None:
    if shellpath is None:
        return None
    path = Path(shellpath).expanduser()
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError(f"shellpath does not exist: {path}") from exc
    if not resolved.is_file():
        raise ValueError(f"shellpath is not a file: {resolved}")
    if not os.access(resolved, os.X_OK):
        raise ValueError(f"shellpath is not executable: {resolved}")
    return str(resolved)
