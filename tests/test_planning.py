"""Tests for invocation-local RuleCall planning."""

import pytest

from necroflow import DAG, NodeType, Pipeline, Verdict
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


@pytest.mark.parametrize("reuse_unchanged", [False, True])
def test_forced_parent_propagates_unless_reuse_is_enabled(tmp_path, reuse_unchanged):
    """Only explicit content reuse defers descendants of a parent that will run."""
    pipeline = _pipeline(tmp_path)
    pipeline.dag.require([pipeline.b])
    pipeline.dag.run()

    plan = plan_execution(
        pipeline.dag,
        forced_stale_call_keys={pipeline.a.rule_call.relative_path},
        reuse_unchanged=reuse_unchanged,
    )

    assert plan.verdicts[pipeline.a.rule_call.relative_path] is Verdict.STALE
    if reuse_unchanged:
        assert pipeline.b.rule_call.relative_path not in plan.verdicts
    else:
        assert plan.verdicts[pipeline.b.rule_call.relative_path] is Verdict.STALE
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
    assert plan.verdicts[pipeline.output.rule_call.relative_path] is Verdict.UP_TO_DATE
    assert plan.reasons[pipeline.output.rule_call.relative_path] == (
        {"kind": "up_to_date"},
    )


def test_plan_settles_a_cached_chain_in_one_pass(tmp_path):
    """Registration order is topological, so one pass decides a whole cached chain.

    Every call is interned after the calls producing its inputs; planning
    relies on that instead of repeating passes until nothing changes.
    """

    class C(NodeType):
        filename = "c.txt"

    make_c = Rule("planning_make_c", Inputs(b=B), Outputs(c=C), "touch {c}")

    def chain(nodes_dir):
        pipeline = Pipeline(DAG(nodes_dir))
        pipeline.a = MAKE_A(pipeline, value="x")
        pipeline.b = MAKE_B(pipeline, pipeline.a)
        pipeline.c = make_c(pipeline, pipeline.b)
        pipeline.finish()
        pipeline.dag.require([pipeline.c])
        return pipeline

    chain(tmp_path).dag.run()
    second = chain(tmp_path)
    plan = plan_execution(second.dag)

    assert [plan.verdicts[call.relative_path] for call in plan.active] == [
        Verdict.UP_TO_DATE
    ] * 3
