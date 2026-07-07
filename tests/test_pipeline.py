"""Tests for Pipeline, DAG, and _sinks."""
import pytest
from pathlib import Path
from necroflow import NodeType, Inputs, Outputs, Rules, Pipeline, DAG
from necroflow.pipeline import _sinks


class A(NodeType): filename = "a.txt"
class B(NodeType): filename = "b.txt"
class C(NodeType): filename = "c.txt"
class D(NodeType): filename = "d.txt"
class E(NodeType): filename = "e.txt"


R = Rules()
R.register("make_a",        Inputs(x=str),      Outputs(a=A), "touch {a}")
R.register("make_b",        Inputs(a=A),         Outputs(b=B), "touch {b}")
R.register("make_c",        Inputs(a=A),         Outputs(c=C), "touch {c}")
R.register("make_d",        Inputs(b=B, c=C),    Outputs(d=D), "touch {d}")
R.register("make_c_from_b", Inputs(b=B),         Outputs(c=C), "touch {c}")
R.register("make_e_from_ac",Inputs(a=A, c=C),    Outputs(e=E), "touch {e}")


def diamond():
    """A → B, A → C, (B,C) → D"""
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    P.c = R.make_c(P.a)
    P.d = R.make_d(P.b, P.c)
    return P


# ── _sinks ────────────────────────────────────────────────────────────────────

def test_sinks_source_node():
    # single node with no parents and no children — must be a sink
    P = Pipeline()
    P.a = R.make_a(x="x")
    assert _sinks(P) == [P.a]


def test_sinks_linear():
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    assert _sinks(P) == [P.b]


def test_sinks_diamond():
    P = diamond()
    assert _sinks(P) == [P.d]


def test_sinks_multiple():
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    P.c = R.make_c(P.a)
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
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    assert len(P.nodes) == 2


def test_pipeline_dot_prefix_raises():
    """Label starting with '.' must raise — reserved for .rip internal folder."""
    P = Pipeline()
    with pytest.raises(ValueError, match=r"must not start with '\.'"):
        setattr(P, ".hidden", R.make_a(x="x"))



def test_pipeline_duplicate_raises():
    P = Pipeline()
    P.a = R.make_a(x="x")
    with pytest.raises(ValueError):
        P.a = R.make_a(x="y")


def test_pipeline_label_single():
    P = Pipeline()
    P.a = R.make_a(x="x")
    assert P.a.pipeline_label == "a"


def test_pipeline_save(tmp_path):
    P = diamond()
    out = tmp_path / "out.txt"
    P.save(out)
    assert out.exists()
    assert "Pipeline" in out.read_text()


def test_workdir_is_reserved_input_output_name():
    with pytest.raises(ValueError, match="reserved command placeholder"):
        Rules().register("bad_input", Inputs(workdir=str), Outputs(a=A), "touch {a}")
    with pytest.raises(ValueError, match="reserved command placeholder"):
        Rules().register("bad_output", Inputs(x=str), Outputs(workdir=A), "touch {workdir}")


# ── DAG deduplication ─────────────────────────────────────────────────────────

def test_dag_deduplicates_shared_nodes():
    P1 = Pipeline()
    P1.a = R.make_a(x="shared")
    P1.b = R.make_b(P1.a)

    P2 = Pipeline()
    P2.a = R.make_a(x="shared")
    P2.b = R.make_b(P2.a)

    dag = DAG()
    dag.add(P1)
    dag.add(P2)
    # same config → same hash → 2 unique nodes, not 4
    assert len(dag.nodes) == 2


def test_dag_keeps_distinct_nodes():
    P1 = Pipeline()
    P1.a = R.make_a(x="x1")
    P1.b = R.make_b(P1.a)

    P2 = Pipeline()
    P2.a = R.make_a(x="x2")
    P2.b = R.make_b(P2.a)

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
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    P.c = R.make_c_from_b(P.b)
    P.e = R.make_e_from_ac(P.a, P.c)
    rendered = str(P)
    # all node labels present
    for label in ("make_a", "make_b", "make_c_from_b", "make_e_from_ac"):
        assert label in rendered
    # dummy pass-throughs add an extra │ to the mid row of intermediate layers,
    # e.g. "│ make_b[B:b] │   │" has 3 pipe chars vs 2 for a plain box row
    rows_with_dummy = [l for l in rendered.splitlines()
                       if "make_" in l and l.count("│") >= 3]
    assert len(rows_with_dummy) > 0, "expected dummy │ pass-through in intermediate layer rows"


def test_dag_save(tmp_path):
    P = diamond()
    dag = DAG(tmp_path)
    dag.add(P)
    out = tmp_path / "dag.txt"
    dag.save(out)
    assert out.exists()
    assert "DAG" in out.read_text()
