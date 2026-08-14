"""Tests for dependency graph serialization."""

from necroflow import DAG, NodeType, Pipeline
from necroflow.rules import Inputs, Outputs, Rule


class A(NodeType):
    filename = "a.txt"


class B(NodeType):
    filename = "b.txt"


class C(NodeType):
    filename = "c.txt"


class D(NodeType):
    filename = "d.txt"


MAKE_A = Rule("make_a", Inputs(value=str), Outputs(a=A), "touch {a}")
MAKE_B = Rule("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
MAKE_C = Rule("make_c", Inputs(a=A), Outputs(c=C), "touch {c}")
MAKE_D = Rule("make_d", Inputs(b=B, c=C), Outputs(d=D), "touch {d}")


def _diamond(nodes_dir):
    pipeline = Pipeline(DAG(nodes_dir))
    pipeline.a = MAKE_A(pipeline, value="x")
    pipeline.b = MAKE_B(pipeline, pipeline.a)
    pipeline.c = MAKE_C(pipeline, pipeline.a)
    pipeline.d = MAKE_D(pipeline, pipeline.b, pipeline.c)
    return pipeline


EXPECTED = """1 make_a[A:a]
2 make_b[B:b]
3 make_c[C:c]
4 make_d[D:d]
#
1 2
1 3
2 4
3 4"""


def test_pipeline_renders_standard_tgf(tmp_path):
    """Text rendering must use TGF node records and parent-to-child edges."""
    pipeline = _diamond(tmp_path)

    assert str(pipeline) == EXPECTED


def test_success_writes_ancestor_tgf_only(tmp_path):
    """Successful calls must store their ancestor graph as graph.tgf, not graph.txt."""
    pipeline = _diamond(tmp_path)
    pipeline.finish()
    pipeline.dag.require([pipeline.d])

    pipeline.dag.run()

    rip = pipeline.d.path.parent / ".rip"
    assert (rip / "graph.tgf").read_text() == EXPECTED + "\n"
    assert not (rip / "graph.txt").exists()
