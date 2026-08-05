from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path, PurePosixPath

from necroflow.nodes import Node
from necroflow.rule_call import RuleCall

_LINUX_NAME_MAX = 255
_LINUX_PATH_MAX = 4096


def _validate_request_path(name: str, *, kind: str) -> PurePosixPath:
    """Return a canonical, portable relative request path."""
    if not isinstance(name, str):
        raise TypeError(f"{kind} must be a string")
    if not name:
        raise ValueError(f"{kind} must not be empty")
    if name.startswith("."):
        raise ValueError(f"{kind} {name!r} must not start with '.'")

    label_path = PurePosixPath(name)
    if label_path.is_absolute() or label_path.as_posix() != name:
        raise ValueError(f"{kind} {name!r} must be a canonical relative POSIX path")
    for component in label_path.parts:
        if "\0" in component:
            raise ValueError(f"{kind} {name!r} contains a null byte")
        if component in {".", ".."} or component.startswith("."):
            raise ValueError(
                f"{kind} {name!r} contains forbidden component {component!r}"
            )
        length = len(os.fsencode(component))
        if length > _LINUX_NAME_MAX:
            raise ValueError(
                f"{kind} component too long "
                f"({length} > NAME_MAX {_LINUX_NAME_MAX} bytes): {component!r}"
            )
    return label_path


def _validate_pipeline_label(name: str, output_filename: str) -> PurePosixPath:
    """Return a canonical, portable relative result path for a label."""
    label_path = _validate_request_path(name, kind="Pipeline label")

    result_path = label_path / output_filename
    length = len(os.fsencode(result_path.as_posix()))
    if length > _LINUX_PATH_MAX:
        raise ValueError(
            f"Pipeline result path too long "
            f"({length} > PATH_MAX {_LINUX_PATH_MAX} bytes): {result_path}"
        )
    return label_path


def _result_paths_conflict(left: PurePosixPath, right: PurePosixPath) -> bool:
    """Return whether either result path must be a directory for the other."""
    return left == right or left in right.parents or right in left.parents


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


class _GraphBase:
    """Shared rendering logic for Pipeline and DAG."""

    @property
    def nodes(self) -> list:
        raise NotImplementedError

    def _header(self) -> str:
        raise NotImplementedError

    def _node_label(self, node: Node) -> str:
        parts = [node.rule.__name__]
        suffix = node.node_type.__name__
        if node.output_name and node.output_name != suffix:
            suffix += f":{node.output_name}" if suffix else node.output_name
        if suffix:
            parts[0] += f"[{suffix}]"
        if node.mutable:
            parts.append("[mutable]")
        # needs human review: config omitted from label because long embedded
        # config values can otherwise make the ASCII DAG unreadable.
        if node.rule.constraints:
            resources = ", ".join(
                f"{key}={value}" for key, value in node.rule.constraints.items()
            )
            parts.append(f"[{resources}]")
        return " ".join(parts)

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
        if n.relative_path in seen:
            continue
        seen[n.relative_path] = n
        frontier.extend(n.parents)
    view = _AncestorView(list(seen.values()))
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "graph.txt").write_text(str(view) + "\n", encoding="utf-8")


class _PipelineState:
    """Mutable construction state shared by one root Pipeline and all its views."""

    def __init__(self, dag: DAG, shellpath: str | Path | None) -> None:
        self.dag = dag
        self.shellpath = _normalize_shellpath(shellpath)
        self.nodes_list: list[Node] = []
        self.node_paths: set[Path] = set()
        self.node_names: dict[str, Node] = {}
        self.finished = False


class Pipeline(_GraphBase):
    """One compiled request namespace over a shared canonical DAG."""

    def __init__(
        self,
        dag: DAG,
        *,
        shellpath: str | Path | None = None,
    ):
        if not isinstance(dag, DAG):
            raise TypeError(
                f"Pipeline requires an owning DAG, got {type(dag).__name__}"
            )
        self._state = _PipelineState(dag, shellpath)
        self._request_prefix = ""

    @classmethod
    def _view(cls, state: _PipelineState, request_prefix: str) -> Pipeline:
        """Return a prefixed view over existing root construction state."""
        view = cls.__new__(cls)
        view._state = state
        view._request_prefix = request_prefix
        return view

    @property
    def nodes_dir(self) -> Path:
        return self._state.dag.nodes_dir

    @property
    def dag(self) -> DAG:
        return self._state.dag

    @property
    def shellpath(self) -> str | None:
        return self._state.shellpath

    @property
    def request_prefix(self) -> str | None:
        """Return this view's qualified request prefix, or None for the root."""
        return self._request_prefix or None

    @property
    def finished(self) -> bool:
        """Return whether construction of this shared Pipeline has finished."""
        return self._state.finished

    def _assert_open(self) -> None:
        """Reject mutation and rule compilation after the root is finished."""
        if self._state.finished:
            raise RuntimeError("Pipeline construction has finished")

    def _assert_finished(self) -> None:
        """Reject operations whose meaning requires the complete Pipeline."""
        if not self._state.finished:
            raise RuntimeError("Pipeline construction is not finished")

    def finish(self) -> None:
        """Freeze this root Pipeline and every prefixed view over it."""
        if self._request_prefix:
            raise RuntimeError("only the root Pipeline can finish construction")
        self._state.finished = True

    def subpipeline(self, request_prefix: str) -> Pipeline:
        """Return a view that qualifies assignments with a request prefix."""
        self._assert_open()
        prefix = _validate_request_path(
            request_prefix, kind="Subpipeline request prefix"
        ).as_posix()
        if self._request_prefix:
            prefix = f"{self._request_prefix}/{prefix}"
        return type(self)._view(self._state, prefix)

    def _qualified_label(self, name: str) -> str:
        """Return a view-local label qualified for the root namespace."""
        if self._request_prefix:
            return f"{self._request_prefix}/{name}"
        return name

    def labels_for(self, node: Node) -> tuple[str, ...]:
        """Return labels assigned to a canonical Node in this Pipeline.

        These are qualified request names owned by the shared root and returned
        in assignment order. A Node may have several labels in one Pipeline,
        and the same canonical Node may have different labels in other
        Pipelines sharing the DAG. This is the authoritative lookup when
        producing output for a particular Pipeline or job.
        """
        return tuple(
            name
            for name, candidate in self._state.node_names.items()
            if candidate is node
        )

    @property
    def labels(self) -> tuple[str, ...]:
        """Return all qualified root labels in assignment order."""
        return tuple(self._state.node_names)

    def sinks(self) -> list[Node]:
        """Return labeled Nodes with no labeled dependents after construction."""
        self._assert_finished()
        parent_paths = {
            parent.relative_path for node in self.nodes for parent in node.parents
        }
        return [node for node in self.nodes if node.relative_path not in parent_paths]

    @property
    def nodes(self) -> list[Node]:
        return self._state.nodes_list

    def __getattr__(self, name: str) -> Node:
        try:
            return self._state.node_names[self._qualified_label(name)]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, name: str) -> Node:
        if not isinstance(name, str):
            raise TypeError("Pipeline label must be a string")
        return self._state.node_names[self._qualified_label(name)]

    def _assign_node(self, name: str, value: Node) -> None:
        self._assert_open()
        qualified_name = self._qualified_label(name)
        label_path = _validate_pipeline_label(qualified_name, value.path.name)
        if qualified_name in self._state.node_names:
            raise ValueError(f"Pipeline label {qualified_name!r} already assigned")
        if value.rule_call.dag is not self._state.dag:
            raise ValueError(
                f"Node assigned as {qualified_name!r} belongs to a different DAG"
            )
        result_path = label_path / value.path.name
        for existing_name, existing_node in self._state.node_names.items():
            existing_path = PurePosixPath(existing_name) / existing_node.path.name
            if _result_paths_conflict(result_path, existing_path):
                raise ValueError(
                    f"Pipeline result path {result_path!s} for label "
                    f"{qualified_name!r} "
                    f"conflicts with {existing_path!s} for label "
                    f"{existing_name!r}"
                )
        if value.relative_path not in self._state.node_paths:
            self._state.nodes_list.append(value)
            self._state.node_paths.add(value.relative_path)
        self._state.node_names[qualified_name] = value
        self._state.dag._record_binding(value, qualified_name)

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
    """Shared registry and executor for canonical content-addressed rule calls."""

    def __init__(self, outdir):
        self._calls: dict[Path, RuleCall] = {}
        self._nodes: dict[Path, Node] = {}
        self._required: set[Path] = set()
        self._labels_by_path: dict[Path, set[str]] = {}
        self.outdir = Path(outdir).expanduser().resolve()
        self.last_execution_report = None

    @property
    def nodes_dir(self) -> Path:
        return self.outdir

    @property
    def calls(self) -> dict[Path, RuleCall]:
        return self._calls

    def intern(self, call: RuleCall) -> RuleCall:
        """Return the canonical RuleCall for this relative call path."""
        existing = self._calls.get(call.relative_path)
        if existing is not None:
            expected = {
                name: (node.node_type, node.relative_path)
                for name, node in existing.output_nodes.items()
            }
            candidate = {
                name: (node.node_type, node.relative_path)
                for name, node in call.output_nodes.items()
            }
            if candidate != expected:
                raise ValueError(
                    f"fingerprint collision for {call.relative_path}: "
                    "declared outputs do not match the canonical RuleCall"
                )
            return existing
        self._calls[call.relative_path] = call
        for node in call.output_nodes.values():
            if node.relative_path in self._nodes:
                raise ValueError(f"duplicate output path: {node.relative_path}")
            self._nodes[node.relative_path] = node
        return call

    def require(self, nodes) -> None:
        """Add canonical Nodes to the set requested for execution."""
        for node in nodes:
            if not isinstance(node, Node):
                raise TypeError(
                    f"DAG requirements must be Nodes, got {type(node).__name__}"
                )
            if node.rule_call.dag is not self:
                raise ValueError("required Node belongs to a different DAG")
            self._required.add(node.relative_path)

    def _record_binding(self, node: Node, label: str) -> None:
        """Record one qualified Pipeline label as DAG-wide diagnostic metadata."""
        self._labels_by_path.setdefault(node.relative_path, set()).add(label)

    def labels_for(self, node: Node) -> tuple[str, ...]:
        """Return distinct labels recorded across all Pipelines for a Node.

        These labels are DAG-wide diagnostic metadata, not authoritative
        names for any particular Pipeline. They are sorted for deterministic
        display and may contain several aliases when Pipelines bind the same
        canonical Node under different names.
        """
        return tuple(sorted(self._labels_by_path.get(node.relative_path, set())))

    def label_for(self, node: Node) -> str | None:
        """Return the Node's sole DAG-wide label, or ``None`` if ambiguous.

        This is a best-effort display helper for execution events and
        diagnostics that lack Pipeline context. It deliberately refuses to
        choose arbitrarily when :meth:`labels_for` finds zero or several
        distinct labels.
        """
        labels = self.labels_for(node)
        return labels[0] if len(labels) == 1 else None

    @property
    def nodes(self) -> list:
        return list(self._nodes.values())

    @property
    def required_nodes(self) -> list:
        return [n for path, n in self._nodes.items() if path in self._required]

    def _header(self) -> str:
        return f"DAG  {len(self._nodes)} nodes  ({len(self._required)} required)"

    def _node_label(self, node: Node) -> str:
        required_paths = {n.relative_path for n in self.required_nodes}
        return super()._node_label(node) + (
            " ★" if node.relative_path in required_paths else ""
        )

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
