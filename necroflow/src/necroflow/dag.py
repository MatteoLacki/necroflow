from __future__ import annotations

import hashlib
import inspect
from collections import namedtuple
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import tomlkit


class NodeState(Enum):
    MISSING = "missing"        # in required subgraph, no output — must run
    STALE = "stale"            # in required subgraph, output exists but a parent is newer — must run
    UP_TO_DATE = "up_to_date"  # in required subgraph, output valid — skip
    ORPHAN = "orphan"          # NOT in required subgraph, output exists
    READY = "ready"            # Missing/Stale with all parents UpToDate — submit now
    RUNNING = "running"        # submitted to thread pool
    FAILED = "failed"          # command returned non-zero exit code
    INTERRUPTED = "interrupted"  # killed by signal, or left RUNNING when necroflow crashed


class NodeTypeMeta(type):
    """Metaclass so that Fastq("output_name") returns a Node, not a Fastq instance."""

    def __call__(cls, output_name: str | None = None) -> Node:
        return Node(output_name=output_name, node_type=cls)

    def __repr__(cls) -> str:
        return cls.__name__


class NodeType(metaclass=NodeTypeMeta):
    """Base class for node types. Subclass to define types.

    class Fastq(NodeType): ...
    class SortedBam(Bam): name = "sorted.bam"  # filename within the rule output dir

    Fastq("label")  # creates Node(node_type=Fastq, output_name="label")
    """

    name: str | None = None



def _is_nodetype(ann) -> bool:
    return inspect.isclass(ann) and issubclass(ann, NodeType)


@dataclass
class Node:
    output_name: str | None = None
    node_type: type[NodeType] | None = None
    parents: list[Node] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    rule: Any | None = None
    command: str | list[str] | None = None
    path: Path | None = None
    output_nodes: dict[str, Node] = field(default_factory=dict)
    state: NodeState | None = None
    info: str | None = None
    pipeline_label: str | None = None

    def __post_init__(self):
        if self.info is None and self.node_type is not None:
            doc = self.node_type.__doc__
            if doc:
                self.info = doc.strip()


def _call_fingerprint(node: Node) -> tuple:
    """Fingerprint of the rule call that produced this node.

    Shared by all co-outputs of the same call — output_name excluded from the
    node's own hash but included in parent references so downstream nodes that
    consume different co-outputs still get distinct hashes.
    """
    return (
        node.rule.__name__ if node.rule else None,
        node.command,
        tuple(sorted(node.config.items())),
        tuple((_call_fingerprint(p), p.output_name) for p in node.parents),
    )


def _folder_hash(node: Node) -> str:
    """8-char hash of the rule call — shared by all co-outputs. Names the output directory."""
    return hashlib.sha256(repr(_call_fingerprint(node)).encode()).hexdigest()[:16]


def _node_key(node: Node) -> str:
    """Unique key for a node: relative path rule_name/folder_hash/filename.
    Distinct for co-outputs because filename differs."""
    rule_name = node.rule.__name__ if node.rule else "unknown"
    filename = (
        node.node_type.name
        if node.node_type and node.node_type.name
        else node.output_name or "output"
    )
    return f"{rule_name}/{_folder_hash(node)}/{filename}"


def _accumulated_config(node: Node) -> dict:
    config = {}
    for parent in node.parents:
        config.update(_accumulated_config(parent))
    config.update(node.config)
    return config


def write_dependencies(node: Node) -> None:
    """Write dependencies.toml into node.path.parent. Call after the job succeeds.

    Co-outputs share a directory, so calling this for any one of them is sufficient.
    """
    data = {
        "rule": node.rule.__name__ if node.rule else "unknown",
        "hash": node.path.parent.name,
        "config": _accumulated_config(node),
    }
    node.path.parent.mkdir(parents=True, exist_ok=True)
    (node.path.parent / "dependencies.toml").write_text(tomlkit.dumps(data))


def check_cache(node: Node) -> bool:
    """True if node.path and dependencies.toml both exist (cache hit)."""
    return (
        node.path is not None
        and node.path.exists()
        and (node.path.parent / "dependencies.toml").exists()
    )


def _output_mtime(path: Path) -> float:
    """Mtime of a node output. For directories, returns the max mtime of all files inside."""
    if path.is_dir():
        mtimes = [f.stat().st_mtime for f in path.rglob("*") if f.is_file()]
        return max(mtimes) if mtimes else path.stat().st_mtime
    return path.stat().st_mtime


def classify_nodes(nodes: list[Node], required_nodes: list[Node]) -> None:
    """Set node.state for each node. Requires resolve_paths() to have been called first.

    Nodes in the required subgraph (required_nodes + all ancestors) get Missing/Stale/UpToDate.
    Nodes outside the subgraph with existing output get Orphan.
    Nodes outside the subgraph with no output get state=None (excluded from execution).
    """
    # build required subgraph via BFS over parents using object identity
    required_subgraph: set[int] = set()
    frontier = list(required_nodes)
    while frontier:
        n = frontier.pop()
        if id(n) in required_subgraph:
            continue
        required_subgraph.add(id(n))
        frontier.extend(n.parents)

    for node in nodes:
        if id(node) not in required_subgraph:
            if node.path is not None and node.path.exists():
                node.state = NodeState.ORPHAN
            else:
                node.state = None
            continue

        if node.path is None or not node.path.exists():
            node.state = NodeState.MISSING
            continue

        node_mtime = _output_mtime(node.path)
        stale = any(
            p.path is not None and p.path.exists() and _output_mtime(p.path) > node_mtime
            for p in node.parents
        )
        node.state = NodeState.STALE if stale else NodeState.UP_TO_DATE

    # propagate STALE transitively: if any parent is MISSING or STALE, this node is also STALE
    changed = True
    while changed:
        changed = False
        for node in nodes:
            if node.state == NodeState.UP_TO_DATE:
                if any(
                    p.state in (NodeState.MISSING, NodeState.STALE)
                    for p in node.parents
                    if p.state is not None
                ):
                    node.state = NodeState.STALE
                    changed = True


def resolve_command(node: Node) -> str | list[str] | None:
    """Format node.command with input/output paths and config values.

    Requires resolve_paths() to have been called first.
    Substitutions: {input_name} -> parent.path, {output_name} -> node.path, {config_key} -> value.
    """
    if node.command is None:
        return None
    pos_input_names = [n for n, t in node.rule.inputs.specs.items() if _is_nodetype(t)]
    subs: dict[str, Any] = {}
    for iname, parent in zip(pos_input_names, node.parents):
        subs[iname] = parent.path
    subs.update(node.config)
    for oname, onode in node.output_nodes.items():
        subs[oname] = onode.path
    if isinstance(node.command, list):
        return [c.format(**subs) for c in node.command]
    return node.command.format(**subs)


def resolve_paths(nodes: list[Node], outdir: Path | str) -> None:
    """Set node.path for each node: outdir / rule_name / hash8 / filename.

    Filename is node_type.name if set, else output_name (no extension).
    Co-outputs of the same rule call share the same hash directory.
    """
    outdir = Path(outdir)
    for node in nodes:
        rule_name = node.rule.__name__ if node.rule else "unknown"
        filename = (
            node.node_type.name
            if node.node_type and node.node_type.name
            else node.output_name or "output"
        )
        node.path = outdir / rule_name / _folder_hash(node) / filename


class Inputs:
    """Declare rule inputs: NodeType values = positional Node args; plain types = config kwargs."""

    def __init__(self, **specs):
        self.specs = specs


class Outputs:
    """Declare rule outputs by name: Outputs(bam=Bam, log=Log)."""

    def __init__(self, **specs):
        self.specs = specs


class Constraints:
    """Declare scheduler constraints: Constraints(threads=4, memory="8G")."""

    def __init__(self, **kwargs):
        self.specs = kwargs


class Rules:
    """Container for registered rules. Names must be unique."""

    def __init__(self):
        object.__setattr__(self, "_registry", {})

    def register(
        self,
        name: str,
        inputs: Inputs,
        outputs: Outputs,
        command: str | list[str],
        constraints: Constraints | None = None,
        info: str | None = None,
    ) -> None:
        registry = object.__getattribute__(self, "_registry")
        if name in registry:
            raise ValueError(f"Rule {name!r} already registered")

        pos_inputs = [(n, t) for n, t in inputs.specs.items() if _is_nodetype(t)]
        kw_inputs = {n: t for n, t in inputs.specs.items() if not _is_nodetype(t)}

        output_names = list(outputs.specs.keys())
        multi = len(output_names) > 1
        ReturnType = namedtuple(f"{name}_outputs", output_names) if multi else None

        constraints_dict = constraints.specs if constraints else {}

        def wrapper(*args, **kwargs):
            for (pname, ptype), val in zip(pos_inputs, args):
                if not isinstance(val, Node):
                    raise TypeError(
                        f"{name}: {pname!r} expected Node, got {type(val).__name__!r}"
                    )
                if val.node_type is None or not issubclass(val.node_type, ptype):
                    got = val.node_type.__name__ if val.node_type else "None"
                    raise TypeError(
                        f"{name}: {pname!r} expected {ptype.__name__}, got {got}"
                    )

            for kname, val in kwargs.items():
                if kname not in kw_inputs:
                    continue
                ktype = kw_inputs[kname]
                try:
                    ok = isinstance(val, ktype)
                except TypeError:
                    ok = True
                if not ok:
                    raise TypeError(
                        f"{name}: {kname!r} expected {ktype}, got {type(val).__name__!r}"
                    )

            parents = [a for a in args if isinstance(a, Node)]
            nodes = [
                Node(
                    output_name=oname,
                    node_type=otype,
                    parents=parents,
                    config=kwargs,
                    rule=wrapper,
                    command=command,
                )
                for oname, otype in outputs.specs.items()
            ]

            all_outputs = {oname: n for oname, n in zip(output_names, nodes)}
            for n in nodes:
                n.output_nodes = all_outputs

            return ReturnType(*nodes) if multi else nodes[0]

        wrapper.__name__ = name
        wrapper.constraints = constraints_dict
        wrapper.inputs = inputs
        wrapper.outputs = outputs
        wrapper.command = command
        wrapper.info = info

        registry[name] = wrapper
        object.__setattr__(self, name, wrapper)
