import time

import pytest

from necroflow import Rules, Inputs, Outputs, Pipeline, DAG, NodeType, NodeState
from necroflow.nodes import Node


class A(NodeType): pass
class B(NodeType): pass
class C(NodeType): pass

R = Rules()
R.register("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R.register("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
R.register("fail_c", Inputs(x=str), Outputs(c=C), "exit 1")
R.register("signal_c", Inputs(x=str), Outputs(c=C), "kill -TERM $$")


def _node(tmp_path, key="rule/fp/out.txt"):
    n = Node()
    n.path = tmp_path / key
    return n


def simple_dag(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.b])
    return dag, P


# --- unit tests for state file helpers ---

def test_fresh_state_not_compromised(tmp_path):
    assert not _node(tmp_path).is_compromised


def test_mark_running_is_compromised(tmp_path):
    n = _node(tmp_path)
    n.mark_running()
    assert n.is_compromised


def test_mark_done_up_to_date_not_compromised(tmp_path):
    n = _node(tmp_path)
    n.mark_running()
    n.mark_done("up_to_date")
    assert not n.is_compromised


def test_mark_done_failed_is_compromised(tmp_path):
    n = _node(tmp_path)
    n.mark_running()
    n.mark_done("failed")
    assert n.is_compromised


def test_mark_done_interrupted_is_compromised(tmp_path):
    n = _node(tmp_path)
    n.mark_running()
    n.mark_done("interrupted")
    assert n.is_compromised


# --- integration: successful run writes up_to_date ---

def test_successful_run_not_compromised(tmp_path):
    dag, P = simple_dag(tmp_path)
    dag.execute()

    dag.resolve_paths(tmp_path)
    for n in dag.nodes:
        assert not n.is_compromised


# --- integration: simulated crash → nodes re-run ---

def test_simulated_crash_reruns_node(tmp_path):
    dag, P = simple_dag(tmp_path)
    dag.execute()

    dag.resolve_paths(tmp_path)
    b_node = next(n for n in dag.nodes if n.rule.__name__ == "make_b")

    # simulate crash: overwrite state file directly
    b_node.state_file.write_text("running")

    mtime_before = b_node.path.stat().st_mtime
    time.sleep(0.05)
    dag.execute()
    assert b_node.path.stat().st_mtime > mtime_before


# --- integration: failed node → FAILED state + re-run next time ---

def test_failed_node_state(tmp_path):
    P = Pipeline()
    P.c = R.fail_c(x="x")
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.c])

    with pytest.raises(Exception):
        dag.execute()

    c_node = next(n for n in dag.nodes if n.rule.__name__ == "fail_c")
    assert c_node.state == NodeState.FAILED
    assert c_node.is_compromised


# --- integration: interrupted node (signal) → INTERRUPTED state ---

def test_interrupted_node_state(tmp_path):
    P = Pipeline()
    P.c = R.signal_c(x="x")
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.c])

    with pytest.raises(Exception):
        dag.execute()

    c_node = next(n for n in dag.nodes if n.rule.__name__ == "signal_c")
    assert c_node.state == NodeState.INTERRUPTED
    assert c_node.is_compromised


# --- integration: retry after failure / interruption ---

class X(NodeType): pass
class Y(NodeType): pass

R2 = Rules()
R2.register("make_x",        Inputs(v=str), Outputs(x=X), "touch {x}")
R2.register("make_y_fail",   Inputs(x=X),   Outputs(y=Y), "touch {y} && exit 1")
R2.register("make_y_signal", Inputs(x=X),   Outputs(y=Y), "touch {y}; kill -TERM $$")


def _xy_dag(tmp_path, y_rule="make_y_fail"):
    P = Pipeline()
    P.x = R2.make_x(v="v")
    P.y = getattr(R2, y_rule)(P.x)
    dag = DAG(outdir=tmp_path)
    dag.add(P)
    return dag, P


def test_failed_node_reruns_on_retry(tmp_path):
    dag, P = _xy_dag(tmp_path, "make_y_fail")

    with pytest.raises(Exception):
        dag.execute()

    dag.resolve_paths(tmp_path)
    x = next(n for n in dag.nodes if n.rule.__name__ == "make_x")
    y = next(n for n in dag.nodes if n.rule.__name__ == "make_y_fail")
    assert x.path.exists() and y.path.exists()

    x_mtime = x.path.stat().st_mtime
    y_mtime = y.path.stat().st_mtime
    time.sleep(0.05)

    with pytest.raises(Exception):
        dag.execute()

    assert x.path.stat().st_mtime == x_mtime
    assert y.path.stat().st_mtime > y_mtime


def test_interrupted_node_reruns_on_retry(tmp_path):
    dag, P = _xy_dag(tmp_path, "make_y_signal")

    with pytest.raises(Exception):
        dag.execute()

    dag.resolve_paths(tmp_path)
    x = next(n for n in dag.nodes if n.rule.__name__ == "make_x")
    y = next(n for n in dag.nodes if n.rule.__name__ == "make_y_signal")
    assert x.path.exists() and y.path.exists()

    x_mtime = x.path.stat().st_mtime
    y_mtime = y.path.stat().st_mtime
    time.sleep(0.05)

    with pytest.raises(Exception):
        dag.execute()

    assert x.path.stat().st_mtime == x_mtime
    assert y.path.stat().st_mtime > y_mtime
