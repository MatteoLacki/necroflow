"""Tests for dependency graph serialization."""

from necroflow import DAG, NodeType, Pipeline
from necroflow.rules import Inputs, Outputs, Command


class A(NodeType):
    filename = "a.txt"


class B(NodeType):
    filename = "b.txt"


class C(NodeType):
    filename = "c.txt"


class D(NodeType):
    filename = "d.txt"


MAKE_A = Command("make_a", Inputs(value=str), Outputs(a=A), "touch {a}")
MAKE_B = Command("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
MAKE_C = Command("make_c", Inputs(a=A), Outputs(c=C), "touch {c}")
MAKE_D = Command("make_d", Inputs(b=B, c=C), Outputs(d=D), "touch {d}")


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


MAKE_PAIR = Command("make_pair", Inputs(a=A), Outputs(b=B, c=C), "touch {b} {c}")


def test_ancestor_tgf_lists_every_co_output(tmp_path):
    """A multi-output call's graph.tgf must show all of its outputs.

    The graph is per RuleCall, the unit that ran; starting it from the first
    output alone silently dropped the sibling outputs.
    """
    pipeline = Pipeline(DAG(tmp_path))
    pipeline.a = MAKE_A(pipeline, value="x")
    pipeline.b, pipeline.c = MAKE_PAIR(pipeline, pipeline.a)
    pipeline.finish()
    pipeline.dag.require([pipeline.b])

    pipeline.dag.run()

    graph = pipeline.b.rule_call.metadata_dir / "graph.tgf"
    assert graph.read_text() == (
        "1 make_a[A:a]\n2 make_pair[B:b]\n3 make_pair[C:c]\n#\n1 2\n1 3\n"
    )


def test_graph_labels_show_resource_overrides(tmp_path):
    """Text and DOT graphs must show requests selected by the resource file."""
    from necroflow import Constraints
    from necroflow.tgf import render_dot

    rule = Command(
        "override", Inputs(), Outputs(a=A), "touch {a}", Constraints(threads=1)
    )
    pipeline = Pipeline(DAG(tmp_path, resources={"override": {"threads": 7}}))
    pipeline.a = rule(pipeline)
    assert "threads=7" in str(pipeline)
    assert "threads=1" not in str(pipeline)
    assert "threads=7" in render_dot(pipeline.nodes)
