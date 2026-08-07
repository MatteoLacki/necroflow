"""Tests for invocation-local execution planning."""

from necroflow import DAG, NodeState, NodeType, Pipeline
from necroflow.planning import plan_execution
from necroflow.rules import Inputs, Outputs, Rule


class A(NodeType):
    filename = "a.txt"


class B(NodeType):
    filename = "b.txt"


MAKE_A = Rule("planning_make_a", Inputs(value=str), Outputs(a=A), "touch {a}")
MAKE_B = Rule("planning_make_b", Inputs(a=A), Outputs(b=B), "touch {b}")


def _pipeline(nodes_dir):
    dag = DAG(nodes_dir)
    pipeline = Pipeline(dag)
    pipeline.a = MAKE_A(pipeline, value="x")
    pipeline.b = MAKE_B(pipeline, pipeline.a)
    return pipeline


def test_plan_partitions_nodes_without_deleting_orphans(tmp_path):
    """Planning identifies old inactive outputs but never deletes filesystem state."""

    first = _pipeline(tmp_path)
    first.dag.require([first.b])
    first.dag.run()
    orphan_path = first.b.path

    second = _pipeline(tmp_path)
    second.dag.require([second.a])
    plan = plan_execution(second.dag)

    assert plan.active == [second.a]
    assert plan.orphans == [second.b]
    assert orphan_path.exists()
    assert plan.reasons[second.a.relative_path][0]["kind"] == "up_to_date"


def test_forced_staleness_and_reasons_propagate_in_one_plan(tmp_path):
    """One plan must contain both forced invalidation and its descendant cause."""

    pipeline = _pipeline(tmp_path)
    pipeline.dag.require([pipeline.b])
    pipeline.dag.run()

    plan = plan_execution(
        pipeline.dag,
        forced_stale_keys={pipeline.a.relative_path},
    )

    assert pipeline.a.state == NodeState.STALE
    assert pipeline.b.state == NodeState.STALE
    assert {reason["kind"] for reason in plan.reasons[pipeline.a.relative_path]} == {
        "forced_invalidation"
    }
    assert {reason["kind"] for reason in plan.reasons[pipeline.b.relative_path]} == {
        "parent_not_up_to_date"
    }


def test_plan_records_reasons_without_reinvoking_invalidator(tmp_path):
    """Planning and explanation must share one invalidator observation."""

    calls = 0

    def generation(node):
        nonlocal calls
        calls += 1
        return "one"

    class Tracked(NodeType):
        filename = "tracked.txt"
        invalidator = staticmethod(generation)

    make_tracked = Rule(
        "planning_make_tracked",
        Inputs(value=str),
        Outputs(output=Tracked),
        "touch {output}",
    )
    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)
    pipeline.output = make_tracked(pipeline, value="x")
    dag.require([pipeline.output])
    dag.run()
    calls = 0

    plan = plan_execution(dag)

    assert calls == 1
    assert pipeline.output.state == NodeState.UP_TO_DATE
    assert plan.reasons[pipeline.output.relative_path] == ({"kind": "up_to_date"},)
