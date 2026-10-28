import logging

from necroflow.rules import Constraints, Inputs, Outputs, Command
import pytest

from necroflow import Pipeline, DAG, NodeType


class A(NodeType):
    filename = "a"


class B(NodeType):
    filename = "b"


class C(NodeType):
    filename = "c"


class D(NodeType):
    filename = "d"


R_make_a = Command("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R_make_b = Command("make_b", Inputs(x=str), Outputs(b=B), "touch {b}")
R_fail_c = Command(
    "fail_c", Inputs(a=A), Outputs(c=C), "{{ : {c}; exit 1; }}"
)  # always fails
R_make_d = Command("make_d", Inputs(c=C), Outputs(d=D), "touch {d}")


def two_branch_dag(tmp_path):
    """Two independent branches: make_a→fail_c→make_d  and  make_b (independent)."""
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.a = R_make_a(P, x="input")
    P.b = R_make_b(P, x="input")
    P.c = R_fail_c(P, P.a)
    P.d = R_make_d(P, P.c)
    dag.require([P.d, P.b])
    return dag, P


def test_default_raises_immediately(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(Exception):
        dag.run()


def test_keep_going_raises_exception_group(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(ExceptionGroup):
        dag.run(keep_going=True)


def test_keep_going_runs_independent_branch(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(ExceptionGroup):
        dag.run(keep_going=True)

    # make_b is independent — its output should exist
    b_node = next(n for n in dag.nodes if n.rule.__name__ == "make_b")
    assert b_node.path.exists()


def test_keep_going_downstream_of_failure_is_blocked(tmp_path, caplog):
    """A call below a failure is blocked: never run, never reported, and counted.

    It did not fail itself, so it gets no report event and no `.rip/state`;
    the run summary counts it separately from the failure that blocked it.
    """
    dag, P = two_branch_dag(tmp_path)
    with caplog.at_level(logging.INFO, logger="necroflow"):
        with pytest.raises(ExceptionGroup) as caught:
            dag.run(keep_going=True)

    report = caught.value.execution_report
    assert report[P.c.rule_call.relative_path.as_posix()].state == "failed"
    assert P.d.rule_call.relative_path.as_posix() not in report
    assert not P.d.path.exists()
    assert not P.d.rule_call.state_file.exists()
    assert "1 failed, 1 blocked" in caplog.text


def test_keep_going_no_error_when_all_succeed(tmp_path):
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.a = R_make_a(P, x="x1")
    P.b = R_make_b(P, x="x2")
    dag.require([P.a, P.b])
    dag.run(keep_going=True)  # should not raise
