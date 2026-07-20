"""Tests for Pipeline, DAG, and _sinks."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule

import pytest
from pathlib import Path
from necroflow import NodeType, Pipeline, DAG, command
from necroflow.pipeline import _sinks


class A(NodeType):
    filename = "a.txt"


class B(NodeType):
    filename = "b.txt"


class C(NodeType):
    filename = "c.txt"


class D(NodeType):
    filename = "d.txt"


class E(NodeType):
    filename = "e.txt"


R_make_a = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R_make_b = Rule("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
R_make_c = Rule("make_c", Inputs(a=A), Outputs(c=C), "touch {c}")
R_make_d = Rule("make_d", Inputs(b=B, c=C), Outputs(d=D), "touch {d}")
R_make_c_from_b = Rule("make_c_from_b", Inputs(b=B), Outputs(c=C), "touch {c}")
R_make_e_from_ac = Rule("make_e_from_ac", Inputs(a=A, c=C), Outputs(e=E), "touch {e}")


def diamond():
    """A → B, A → C, (B,C) → D"""
    P = Pipeline()
    P.a = R_make_a(x="x")
    P.b = R_make_b(P.a)
    P.c = R_make_c(P.a)
    P.d = R_make_d(P.b, P.c)
    return P


# ── _sinks ────────────────────────────────────────────────────────────────────


def test_sinks_source_node():
    # single node with no parents and no children — must be a sink
    P = Pipeline()
    P.a = R_make_a(x="x")
    assert _sinks(P) == [P.a]


def test_sinks_linear():
    P = Pipeline()
    P.a = R_make_a(x="x")
    P.b = R_make_b(P.a)
    assert _sinks(P) == [P.b]


def test_sinks_diamond():
    P = diamond()
    assert _sinks(P) == [P.d]


def test_sinks_multiple():
    P = Pipeline()
    P.a = R_make_a(x="x")
    P.b = R_make_b(P.a)
    P.c = R_make_c(P.a)
    # b and c are both sinks (nothing depends on them)
    assert set(id(n) for n in _sinks(P)) == {id(P.b), id(P.c)}


def test_sinks_excludes_intermediate():
    P = diamond()
    sinks = _sinks(P)
    assert P.a not in sinks
    assert P.b not in sinks
    assert P.c not in sinks


# ── Pipeline attribute assignment ─────────────────────────────────────────────


def test_pipeline_nodes_accumulate():
    P = Pipeline()
    P.a = R_make_a(x="x")
    P.b = R_make_b(P.a)
    assert len(P.nodes) == 2


def test_pipeline_dot_prefix_raises():
    """Label starting with '.' must raise — reserved for .rip internal folder."""
    P = Pipeline()
    with pytest.raises(ValueError, match=r"must not start with '\.'"):
        setattr(P, ".hidden", R_make_a(x="x"))


def test_pipeline_duplicate_raises():
    P = Pipeline()
    P.a = R_make_a(x="x")
    with pytest.raises(ValueError):
        P.a = R_make_a(x="y")


def test_pipeline_label_single():
    P = Pipeline()
    P.a = R_make_a(x="x")
    assert P.a.pipeline_label == "a"


def test_pipeline_save(tmp_path):
    P = diamond()
    out = tmp_path / "out.txt"
    P.save(out)
    assert out.exists()
    assert "Pipeline" in out.read_text()


def test_workdir_is_reserved_input_output_name():
    with pytest.raises(ValueError, match="reserved command placeholder"):

        @command("touch {a}")
        def bad_input(workdir: str):
            return A[a]

    with pytest.raises(ValueError, match="reserved command placeholder"):

        @command("touch {workdir}")
        def bad_output(x: str):
            return A[workdir]


def test_pipeline_sections_tag_subsequent_nodes_only():
    P = Pipeline()
    P.a = R_make_a(x="x")
    P.section("Preparation")
    P.b = R_make_b(P.a)
    P.section("Analysis")
    P.c = R_make_c(P.a)

    assert P.sections == ("Preparation", "Analysis")
    assert P.section_for(P.a) is None
    assert P.section_for(P.b) == "Preparation"
    assert P.section_for(P.c) == "Analysis"


def test_pipeline_section_rejects_invalid_or_duplicate_names():
    P = Pipeline()
    with pytest.raises(TypeError, match="must be a string"):
        P.section(1)
    with pytest.raises(ValueError, match="must not be empty"):
        P.section("  ")
    P.section("Preparation")
    with pytest.raises(ValueError, match="already exists"):
        P.section("Preparation")


def test_png_renderer_clusters_unambiguous_pipeline_sections(tmp_path, monkeypatch):
    import sys
    from necroflow import graphviz_render

    class FakeGraph:
        def __init__(self):
            self.nodes = []
            self.edges = []

        def add_nodes_from(self, nodes):
            self.nodes.extend(nodes)

        def add_edges_from(self, edges):
            self.edges.extend(edges)

        def in_degree(self, node):
            return sum(target == node for _source, target in self.edges)

    class FakeNetworkX:
        DiGraph = FakeGraph

        def is_directed_acyclic_graph(graph):
            return True

        def topological_generations(graph):
            yield graph.nodes

    P = Pipeline()
    P.section("Preparation")
    P.a = R_make_a(x="x")
    P.section("Analysis")
    P.b = R_make_b(P.a)
    dag = DAG()
    dag.add(P)

    captured = {}
    monkeypatch.setitem(sys.modules, "networkx", FakeNetworkX)
    monkeypatch.setattr(graphviz_render.shutil, "which", lambda _name: "dot")
    monkeypatch.setattr(
        graphviz_render.subprocess,
        "run",
        lambda _args, **kwargs: captured.setdefault("dot", kwargs["input"]),
    )

    graphviz_render.render_png(dag, output_path=tmp_path / "dag.png")

    assert "subgraph cluster_section_0" in captured["dot"]
    assert 'label="Preparation";' in captured["dot"]
    assert 'label="Analysis";' in captured["dot"]


# ── DAG deduplication ─────────────────────────────────────────────────────────


def test_dag_deduplicates_shared_nodes():
    P1 = Pipeline()
    P1.a = R_make_a(x="shared")
    P1.b = R_make_b(P1.a)

    P2 = Pipeline()
    P2.a = R_make_a(x="shared")
    P2.b = R_make_b(P2.a)

    dag = DAG()
    dag.add(P1)
    dag.add(P2)
    # same config → same hash → 2 unique nodes, not 4
    assert len(dag.nodes) == 2


def test_dag_section_is_none_when_shared_nodes_have_conflicting_sections():
    P1 = Pipeline()
    P1.section("Preparation")
    P1.a = R_make_a(x="shared")

    P2 = Pipeline()
    P2.section("Alternative preparation")
    P2.a = R_make_a(x="shared")

    dag = DAG()
    dag.add(P1)
    dag.add(P2)

    assert dag.section_for(dag.nodes[0]) is None


def test_dag_keeps_distinct_nodes():
    P1 = Pipeline()
    P1.a = R_make_a(x="x1")
    P1.b = R_make_b(P1.a)

    P2 = Pipeline()
    P2.a = R_make_a(x="x2")
    P2.b = R_make_b(P2.a)

    dag = DAG()
    dag.add(P1)
    dag.add(P2)
    assert len(dag.nodes) == 4


def test_dag_required_defaults_to_sinks():
    P = diamond()
    dag = DAG()
    dag.add(P)
    assert len(dag.required_nodes) == 1
    assert dag.required_nodes[0].rule.__name__ == "make_d"


def test_dag_explicit_request():
    P = diamond()
    dag = DAG()
    dag.add(P, request=[P.b, P.c])
    req_rules = {n.rule.__name__ for n in dag.required_nodes}
    assert req_rules == {"make_b", "make_c"}


def test_str_long_range_edge():
    """Long-range edges (spanning >1 layer) render as │ pass-throughs, not silently dropped.

    Chain: a(0)→b(1)→c(2), plus direct a→e(3). The a→e edge skips two layers; dummy
    pass-through nodes are inserted so the connector is drawn through all intermediate layers.
    """
    P = Pipeline()
    P.a = R_make_a(x="x")
    P.b = R_make_b(P.a)
    P.c = R_make_c_from_b(P.b)
    P.e = R_make_e_from_ac(P.a, P.c)
    rendered = str(P)
    # all node labels present
    for label in ("make_a", "make_b", "make_c_from_b", "make_e_from_ac"):
        assert label in rendered
    # dummy pass-throughs add an extra │ to the mid row of intermediate layers,
    # e.g. "│ make_b[B:b] │   │" has 3 pipe chars vs 2 for a plain box row
    rows_with_dummy = [
        l for l in rendered.splitlines() if "make_" in l and l.count("│") >= 3
    ]
    assert (
        len(rows_with_dummy) > 0
    ), "expected dummy │ pass-through in intermediate layer rows"


def test_dag_save(tmp_path):
    P = diamond()
    dag = DAG(tmp_path)
    dag.add(P)
    out = tmp_path / "dag.txt"
    dag.save(out)
    assert out.exists()
    assert "DAG" in out.read_text()
