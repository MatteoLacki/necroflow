import pytest
from necroflow import Rules, Inputs, Outputs, Pipeline, DAG, NodeType, NodeState

class A(NodeType): pass
class B(NodeType): pass
class C(NodeType): pass
class D(NodeType): pass

R = Rules()
R.register("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R.register("make_b", Inputs(x=str), Outputs(b=B), "touch {b}")
R.register("fail_c", Inputs(a=A), Outputs(c=C), "{{ : {c}; exit 1; }}")  # always fails
R.register("make_d", Inputs(c=C), Outputs(d=D), "touch {d}")


def two_branch_dag(tmp_path):
    """Two independent branches: make_a→fail_c→make_d  and  make_b (independent)."""
    P = Pipeline()
    P.a = R.make_a(x="input")
    P.b = R.make_b(x="input")
    P.c = R.fail_c(P.a)
    P.d = R.make_d(P.c)
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.d, P.b])
    return dag, P


def test_default_raises_immediately(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(Exception):
        dag.execute()


def test_keep_going_raises_exception_group(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(ExceptionGroup):
        dag.execute(keep_going=True)


def test_keep_going_runs_independent_branch(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(ExceptionGroup):
        dag.execute(keep_going=True)

    # make_b is independent — its output should exist
    b_node = next(n for n in dag.nodes if n.rule.__name__ == "make_b")
    assert b_node.path.exists()


def test_keep_going_downstream_of_failure_is_failed(tmp_path):
    dag, P = two_branch_dag(tmp_path)
    with pytest.raises(ExceptionGroup):
        dag.execute(keep_going=True)

    d_node = next(n for n in dag.nodes if n.rule.__name__ == "make_d")
    assert d_node.state == NodeState.FAILED
    assert not d_node.path.exists()


def test_keep_going_no_error_when_all_succeed(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x1")
    P.b = R.make_b(x="x2")
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.a, P.b])
    dag.execute(keep_going=True)  # should not raise
