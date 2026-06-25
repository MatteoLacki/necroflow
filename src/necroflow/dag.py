from __future__ import annotations

import hashlib
from collections import namedtuple
from pathlib import Path
from typing import Any

import tomlkit

from necroflow.nodes import Node, NodeState, NodeType, NodeTypeMeta, _is_nodetype



def _content_hash(path: Path) -> str:
    """SHA-256 of a file's bytes, or of all non-.rip files in a directory."""
    h = hashlib.sha256()
    if path.is_file():
        h.update(path.read_bytes())
    else:
        for f in sorted(path.rglob("*")):
            if f.is_file() and ".rip" not in f.parts:
                h.update(str(f.relative_to(path)).encode())
                h.update(f.read_bytes())
    return h.hexdigest()


def _accumulated_config(node: Node) -> dict:
    config = {}
    for parent in node.parents:
        config.update(_accumulated_config(parent))
    config.update(node.config)
    return config


def write_dependencies(node: Node) -> None:
    """Write dependencies.toml and per-output content hashes into node.path.parent.

    Call after the job succeeds. Co-outputs share a directory, so calling this for
    any one of them writes metadata for all siblings via node.output_nodes.
    """
    data = {
        "rule": node.rule.__name__ if node.rule else "unknown",
        "hash": node.path.parent.name,
        "config": _accumulated_config(node),
    }
    rip = node.path.parent / ".rip"
    rip.mkdir(parents=True, exist_ok=True)
    (rip / "dependencies.toml").write_text(tomlkit.dumps(data))
    for onode in node.output_nodes.values():
        if onode.path is not None and onode.path.exists():
            (rip / (onode.path.name + ".hash")).write_text(_content_hash(onode.path))


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
        stale = False
        for p in node.parents:
            if p.path is None or not p.path.exists():
                continue
            if _output_mtime(p.path) <= node_mtime:
                continue  # fast path: parent not newer
            hash_file = p.path.parent / ".rip" / (p.path.name + ".hash")
            if hash_file.exists() and _content_hash(p.path) == hash_file.read_text().strip():
                continue  # parent re-ran but content unchanged
            stale = True
            break
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
        node.path = outdir / node.key


class Inputs:
    """Declare rule inputs: NodeType values = positional Node args; plain types = config kwargs."""

    def __init__(self, **specs):
        self.specs = specs


class Outputs:
    """Declare rule outputs by name: Outputs(bam=Bam, log=Log)."""

    def __init__(self, **specs):
        self.specs = specs


class Constraints:
    """Declare scheduler constraints: Constraints(threads=4, ram="250Mi")."""

    def __init__(self, **kwargs):
        self.specs = kwargs


_SI_SUFFIXES = {"K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15}
_BIN_SUFFIXES = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50}


def parse_resource(s: str | int) -> int:
    """Parse a resource value with optional unit suffix.

    SI (1000-based):     K  M  G  T  P
    Binary (1024-based): Ki Mi Gi Ti Pi
    Plain integer string or int passed through as-is.
    """
    if isinstance(s, int):
        return s
    s = s.strip()
    for suffix, mult in _BIN_SUFFIXES.items():  # binary first — longer suffixes
        if s.endswith(suffix):
            return int(s[: -len(suffix)]) * mult
    for suffix, mult in _SI_SUFFIXES.items():
        if s.endswith(suffix):
            return int(s[: -len(suffix)]) * mult
    return int(s)


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
            if len(args) < len(pos_inputs):
                missing = [pname for pname, _ in pos_inputs[len(args):]]
                raise TypeError(f"{name}: missing required inputs: {missing!r}")
            if len(args) > len(pos_inputs):
                raise TypeError(
                    f"{name}: too many positional inputs: expected {len(pos_inputs)}, got {len(args)}"
                )
            missing_kw = [kname for kname in kw_inputs if kname not in kwargs]
            if missing_kw:
                raise TypeError(f"{name}: missing required inputs: {missing_kw!r}")
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
