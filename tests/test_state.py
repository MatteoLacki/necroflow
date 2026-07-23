from necroflow.rules import Constraints, Inputs, Outputs, Rule
import hashlib
import time
from pathlib import Path

import pytest

from necroflow import Pipeline, DAG, NodeType, NodeState
from necroflow.nodes import Node


class A(NodeType):
    pass


class B(NodeType):
    pass


class C(NodeType):
    pass


R_make_a = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R_make_b = Rule("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
R_fail_c = Rule("fail_c", Inputs(x=str), Outputs(c=C), "{{ : {c}; exit 1; }}")
R_signal_c = Rule(
    "signal_c", Inputs(x=str), Outputs(c=C), "{{ : {c}; kill -TERM $$; }}"
)


def _node(tmp_path, key="rule/fp/out.txt"):
    n = object.__new__(Node)
    n.path = tmp_path / key
    return n


def simple_dag(tmp_path):
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    dag.require([P.b])
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

    for n in dag.nodes:
        assert not n.is_compromised


# --- integration: simulated crash → nodes re-run ---


def test_simulated_crash_reruns_node(tmp_path):
    dag, P = simple_dag(tmp_path)
    dag.execute()

    b_node = next(n for n in dag.nodes if n.rule.__name__ == "make_b")

    # simulate crash: overwrite state file directly
    b_node.state_file.write_text("running")

    mtime_before = b_node.path.stat().st_mtime
    time.sleep(0.05)
    dag.execute()
    assert b_node.path.stat().st_mtime > mtime_before


# --- integration: failed node → FAILED state + re-run next time ---


def test_failed_node_state(tmp_path):
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.c = R_fail_c(P, x="x")
    dag.require([P.c])

    with pytest.raises(Exception):
        dag.execute()

    c_node = next(n for n in dag.nodes if n.rule.__name__ == "fail_c")
    assert c_node.state == NodeState.FAILED
    assert c_node.is_compromised


# --- integration: interrupted node (signal) → INTERRUPTED state ---


def test_interrupted_node_state(tmp_path):
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.c = R_signal_c(P, x="x")
    dag.require([P.c])

    with pytest.raises(Exception):
        dag.execute()

    c_node = next(n for n in dag.nodes if n.rule.__name__ == "signal_c")
    assert c_node.state == NodeState.INTERRUPTED
    assert c_node.is_compromised


# --- integration: retry after failure / interruption ---


class X(NodeType):
    pass


class Y(NodeType):
    pass


R2_make_x = Rule("make_x", Inputs(v=str), Outputs(x=X), "touch {x}")
R2_make_y_fail = Rule("make_y_fail", Inputs(x=X), Outputs(y=Y), "touch {y} && exit 1")
R2_make_y_signal = Rule(
    "make_y_signal", Inputs(x=X), Outputs(y=Y), "touch {y}; kill -TERM $$"
)


def _xy_dag(tmp_path, y_rule="make_y_fail"):
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    rules = {
        "make_y_fail": R2_make_y_fail,
        "make_y_signal": R2_make_y_signal,
    }
    P.x = R2_make_x(P, v="v")
    P.y = rules[y_rule](P, P.x)
    dag.require(P.sinks())
    return dag, P


def test_failed_node_reruns_on_retry(tmp_path):
    dag, P = _xy_dag(tmp_path, "make_y_fail")

    with pytest.raises(Exception):
        dag.execute()

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


# --- integration: NodeType invalidators ---


def _sha256_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_nodetype_invalidator_external_file_change_reruns_node(tmp_path):
    dependency = tmp_path / "tool.bin"
    dependency.write_text("v1")

    def external_hash(node):
        return _sha256_file(Path(node.config["dependency"]))

    class ExternalInvalidated(NodeType):
        filename = "external.txt"
        invalidator = external_hash

    r_make_external = Rule(
        "make_external",
        Inputs(text=str, dependency=str),
        Outputs(out=ExternalInvalidated),
        "echo {text} > {out}",
    )
    execute = DAG(outdir=tmp_path)
    P = Pipeline(execute)
    P.out = r_make_external(P, text="payload", dependency=str(dependency))

    execute.require(P.sinks())
    execute.execute()
    mtime_before = P.out.path.stat().st_mtime

    time.sleep(0.05)
    dependency.write_text("v2")
    execute.execute()

    assert P.out.path.stat().st_mtime > mtime_before


def test_nodetype_invalidator_output_file_change_reruns_node(tmp_path):
    def output_hash(node):
        return _sha256_file(node.path)

    class OutputHashInvalidated(NodeType):
        filename = "output-hash.txt"
        invalidator = output_hash

    r_make_output_hash = Rule(
        "make_output_hash",
        Inputs(text=str),
        Outputs(out=OutputHashInvalidated),
        "echo {text} > {out}",
    )
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.out = r_make_output_hash(P, text="payload")
    dag.require(P.sinks())
    dag.execute()

    P.out.path.write_text("manual edit\n")
    assert P.out.path.read_text().strip() == "manual edit"

    dag.execute()

    assert P.out.path.read_text().strip() == "payload"


def test_nodetype_invalidator_missing_metadata_reruns_node(tmp_path):
    def output_hash(node):
        return _sha256_file(node.path)

    class OutputInvalidated(NodeType):
        filename = "output.txt"
        invalidator = output_hash

    r_make_output = Rule(
        "make_output",
        Inputs(text=str),
        Outputs(out=OutputInvalidated),
        "echo {text} > {out}",
    )
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.out = r_make_output(P, text="payload")
    dag.require(P.sinks())
    dag.execute()
    token_file = P.out.path.parent / ".rip" / (P.out.path.name + ".invalidation")
    assert token_file.exists()
    token_file.unlink()
    mtime_before = P.out.path.stat().st_mtime

    time.sleep(0.05)
    dag.execute()

    assert P.out.path.stat().st_mtime > mtime_before


def test_nodetype_invalidator_exception_fails_fast(tmp_path):
    should_raise = {"value": False}

    def maybe_raise(node):
        if should_raise["value"]:
            raise RuntimeError("invalidator failed")
        return "ok"

    class RaisingInvalidator(NodeType):
        filename = "raising.txt"
        invalidator = maybe_raise

    r_make_raising = Rule(
        "make_raising",
        Inputs(text=str),
        Outputs(out=RaisingInvalidator),
        "echo {text} > {out}",
    )
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.out = r_make_raising(P, text="payload")
    dag.require(P.sinks())
    dag.execute()

    should_raise["value"] = True
    with pytest.raises(RuntimeError, match="invalidator failed"):
        dag.execute()


def test_multi_output_invalidator_reruns_shared_command_once(tmp_path):
    dependency = tmp_path / "dep.txt"
    dependency.write_text("v1")
    count = tmp_path / "count.txt"
    count.write_text("0")

    def external_hash(node):
        return _sha256_file(Path(node.config["dependency"]))

    class InvalidatedA(NodeType):
        filename = "a.txt"
        invalidator = external_hash

    class PlainB(NodeType):
        filename = "b.txt"

    r_make_pair = Rule(
        "make_pair",
        Inputs(dependency=str, count=str),
        Outputs(a=InvalidatedA, b=PlainB),
        "n=$(cat {count}); n=$((n + 1)); echo $n > {count}; echo a > {a}; echo b > {b}",
    )
    dag = DAG(outdir=tmp_path)
    P = Pipeline(dag)
    P.a, P.b = r_make_pair(P, dependency=str(dependency), count=str(count))
    dag.require(P.sinks())

    dag.execute()
    assert count.read_text().strip() == "1"

    dependency.write_text("v2")
    dag.execute()

    assert count.read_text().strip() == "2"
    assert P.a.path.exists()
    assert P.b.path.exists()
