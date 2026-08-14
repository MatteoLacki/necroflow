from __future__ import annotations

from pathlib import Path

from necroflow.tgf import _node_label, render_tgf
from necroflow.nodes import (
    Node,
    NodeType,
    NodeTypeMeta,
)
from necroflow.rule_call import RuleCall
from necroflow.rules import parse_resource


class DAG:
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

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        required_paths = {n.relative_path for n in self.required_nodes}

        def label(node: Node) -> str:
            return _node_label(node) + (
                " [required]" if node.relative_path in required_paths else ""
            )

        return render_tgf(self.nodes, label=label)

    def save(self, path) -> None:
        """Write the DAG in Trivial Graph Format."""
        Path(path).write_text(str(self) + "\n", encoding="utf-8")

    def run(self, **kwargs):
        from necroflow.executor import run

        self.last_execution_report = run(self, **kwargs)
        return self.last_execution_report
