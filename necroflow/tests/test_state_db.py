import sqlite3
import pytest
from necroflow import Rules, Inputs, Outputs, Pipeline, DAG, NodeType, NodeState, StateDB
from necroflow.dag import _node_key

class A(NodeType): pass
class B(NodeType): pass
class C(NodeType): pass

R = Rules()
R.register("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R.register("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
R.register("fail_c", Inputs(x=str), Outputs(c=C), "exit 1")
R.register("signal_c", Inputs(x=str), Outputs(c=C), "kill -TERM $$")


def simple_dag(tmp_path, rule_b="make_b"):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.b])
    return dag, P


# --- StateDB unit tests ---

def test_fresh_db_no_compromised(tmp_path):
    db = StateDB(tmp_path)
    assert db.compromised_keys() == set()
    db.close()


def test_mark_running_is_compromised(tmp_path):
    db = StateDB(tmp_path)
    db.mark_running("align/abc/bam")
    assert "align/abc/bam" in db.compromised_keys()
    db.close()


def test_mark_done_up_to_date_not_compromised(tmp_path):
    db = StateDB(tmp_path)
    db.mark_running("align/abc/bam")
    db.mark_done("align/abc/bam", "up_to_date")
    assert db.compromised_keys() == set()
    db.close()


def test_mark_done_failed_is_compromised(tmp_path):
    db = StateDB(tmp_path)
    db.mark_done("align/abc/bam", "failed")
    assert "align/abc/bam" in db.compromised_keys()
    db.close()


def test_mark_done_interrupted_is_compromised(tmp_path):
    db = StateDB(tmp_path)
    db.mark_done("align/abc/bam", "interrupted")
    assert "align/abc/bam" in db.compromised_keys()
    db.close()


# --- Integration: successful run writes up_to_date ---

def test_successful_run_persists_up_to_date(tmp_path):
    dag, P = simple_dag(tmp_path)
    dag.execute()

    dag.resolve_paths(tmp_path)
    db = StateDB(tmp_path)
    assert db.compromised_keys() == set()
    db.close()


# --- Integration: simulated crash → nodes re-run ---

def test_simulated_crash_reruns_node(tmp_path):
    dag, P = simple_dag(tmp_path)
    dag.execute()

    # simulate crash: manually set make_b's key to 'running' in DB
    dag.resolve_paths(tmp_path)
    b_node = next(n for n in dag.nodes if n.rule.__name__ == "make_b")
    key = _node_key(b_node)

    db = StateDB(tmp_path)
    db.mark_running(key)
    db.close()

    # re-run: make_b output exists but is compromised → should re-run
    import time
    mtime_before = b_node.path.stat().st_mtime
    time.sleep(0.05)
    dag.execute()
    mtime_after = b_node.path.stat().st_mtime
    assert mtime_after > mtime_before


# --- Integration: failed node → FAILED state + re-run next time ---

def test_failed_node_state(tmp_path):
    P = Pipeline()
    P.c = R.fail_c(x="x")
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.c])

    with pytest.raises(Exception):
        dag.execute()

    c_node = next(n for n in dag.nodes if n.rule.__name__ == "fail_c")
    assert c_node.state == NodeState.FAILED

    db = StateDB(tmp_path)
    assert _node_key(c_node) in db.compromised_keys()
    db.close()


# --- Integration: interrupted node (signal) → INTERRUPTED state ---

def test_interrupted_node_state(tmp_path):
    P = Pipeline()
    P.c = R.signal_c(x="x")
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.c])

    with pytest.raises(Exception):
        dag.execute()

    c_node = next(n for n in dag.nodes if n.rule.__name__ == "signal_c")
    assert c_node.state == NodeState.INTERRUPTED

    db = StateDB(tmp_path)
    assert _node_key(c_node) in db.compromised_keys()
    db.close()
