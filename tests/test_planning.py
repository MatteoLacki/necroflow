"""Tests for invocation-local RuleCall planning."""

from necroflow import DAG, NodeType, Pipeline, RuleCallState
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
    pipeline.finish()
    return pipeline


def test_plan_partitions_calls_without_deleting_orphans(tmp_path):
    """Planning identifies inactive call dirs but never deletes them."""
    first = _pipeline(tmp_path)
    first.dag.require([first.b])
    first.dag.run()
    orphan_path = first.b.rule_call.workdir

    second = _pipeline(tmp_path)
    second.dag.require([second.a])
    plan = plan_execution(second.dag)

    assert plan.active == [second.a.rule_call]
    assert plan.orphans == [second.b.rule_call]
    assert orphan_path.exists()
    assert plan.reasons[second.a.rule_call.relative_path][0]["kind"] == "up_to_date"


def test_forced_parent_leaves_child_unclassified_until_parent_settles(tmp_path):
    """Planner does not assume rebuilt parent bytes before execution."""
    pipeline = _pipeline(tmp_path)
    pipeline.dag.require([pipeline.b])
    pipeline.dag.run()

    plan = plan_execution(
        pipeline.dag,
        forced_stale_call_keys={pipeline.a.rule_call.relative_path},
    )

    assert pipeline.a.rule_call.state == RuleCallState.STALE
    assert pipeline.b.rule_call.state is None
    assert plan.reasons[pipeline.b.rule_call.relative_path] == (
        {"kind": "parent_will_run"},
    )


def test_plan_records_invalidator_reason_from_one_observation(tmp_path):
    """Planning and explanation share one invalidator observation."""
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
    pipeline.finish()
    dag.require([pipeline.output])
    dag.run()
    calls = 0

    plan = plan_execution(dag)

    assert calls == 1
    assert pipeline.output.rule_call.state == RuleCallState.UP_TO_DATE
    assert plan.reasons[pipeline.output.rule_call.relative_path] == (
        {"kind": "up_to_date"},
    )
