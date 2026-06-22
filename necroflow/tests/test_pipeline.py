"""Tests for Pipeline, DAG, and _sinks."""
import pytest
from pathlib import Path
from necroflow import NodeType, Inputs, Outputs, Rules, Pipeline, DAG
from necroflow.pipeline import _sinks


class A(NodeType): name = "a.txt"
class B(NodeType): name = "b.txt"
class C(NodeType): name = "c.txt"
class D(NodeType): name = "d.txt"


R = Rules()
R.register("make_a", Inputs(x=str),  Outputs(a=A), "touch {a}")
R.register("make_b", Inputs(a=A),    Outputs(b=B), "touch {b}")
R.register("make_c", Inputs(a=A),    Outputs(c=C), "touch {c}")
R.register("make_d", Inputs(b=B, c=C), Outputs(d=D), "touch {d}")


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


def test_dag_save(tmp_path):
    P = diamond()
    dag = DAG(tmp_path)
    dag.add(P)
    out = tmp_path / "dag.txt"
    dag.save(out)
    assert out.exists()
    assert "DAG" in out.read_text()
