"""Tests for execute(), schedulers, and thread budget."""
import pytest
from necroflow import NodeType, Inputs, Outputs, Constraints, Rules, Pipeline, DAG, execute
from necroflow import fifo_scheduler, connected_component_scheduler


class A(NodeType): name = "a.txt"
class B(NodeType): name = "b.txt"
class C(NodeType): name = "c.txt"
class D(NodeType): name = "d.txt"


R = Rules()
R.register("make_a",       Inputs(x=str), Outputs(a=A),        "touch {a}")
R.register("make_ab",      Inputs(x=str), Outputs(a=A, b=B),   "touch {a} {b}")
R.register("make_only_a",  Inputs(x=str), Outputs(a=A, b=B),   "touch {a}")
R.register("make_b",       Inputs(a=A),   Outputs(b=B),        "touch {b}")
R.register("make_c",       Inputs(a=A),   Outputs(c=C),        "touch {c}")
R.register("fail_a",       Inputs(x=str), Outputs(a=A),        "exit 1")
R.register("no_output_a",  Inputs(x=str), Outputs(a=A),        "true")   # exits 0, creates nothing
R.register("make_a_heavy", Inputs(x=str), Outputs(a=A),        "touch {a}", Constraints(threads=4))


# ── basic execution ───────────────────────────────────────────────────────────

def test_execute_creates_outputs(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path)
    assert P.a.path.exists()
    assert P.b.path.exists()


def test_execute_via_dag(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    dag = DAG(tmp_path)
    dag.add(P)
    dag.execute()
    assert P.b.path is not None
    assert P.b.path.exists()


def test_execute_idempotent(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path)
    mtime_a = P.a.path.stat().st_mtime
    mtime_b = P.b.path.stat().st_mtime
    execute(P, tmp_path)
    assert P.a.path.stat().st_mtime == mtime_a
    assert P.b.path.stat().st_mtime == mtime_b


def test_execute_failure_raises(tmp_path):
    P = Pipeline()
    P.a = R.fail_a(x="x")
    with pytest.raises(Exception):
        execute(P, tmp_path)


def test_missing_output_raises(tmp_path):
    P = Pipeline()
    P.a = R.no_output_a(x="x")
    with pytest.raises(RuntimeError, match="output missing"):
        execute(P, tmp_path)


def test_missing_cooutput_raises(tmp_path):
    """A successful multi-output command must fail if any declared co-output is absent.

    This guards against marking skipped sibling outputs UP_TO_DATE merely because
    the representative co-output was created.
    """
    P = Pipeline()
    P.a, P.b = R.make_only_a(x="x")
    with pytest.raises(RuntimeError, match="output missing"):
        execute(P, tmp_path)


def test_cooutputs_run_once(tmp_path):
    # make_ab writes "touch {a} {b}" — if run twice, both files would be touched twice
    # We verify the command only ran once by checking a single job.log exists
    P = Pipeline()
    P.a, P.b = R.make_ab(x="x")
    execute(P, tmp_path)
    assert P.a.path.exists() and P.b.path.exists()
    # both co-outputs share a directory; only one job.log should exist
    assert (P.a.path.parent / "job.log").exists()
    assert P.a.path.parent == P.b.path.parent


def test_single_node_pipeline_executes(tmp_path):
    # source node (no parents) must be treated as a sink
    P = Pipeline()
    P.a = R.make_a(x="x")
    dag = DAG(tmp_path)
    dag.add(P)
    dag.execute()
    assert P.a.path is not None and P.a.path.exists()


# ── schedulers ────────────────────────────────────────────────────────────────

def test_fifo_scheduler(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    P.c = R.make_c(P.a)
    execute(P, tmp_path, scheduler=fifo_scheduler)
    assert P.b.path.exists() and P.c.path.exists()


def test_connected_component_scheduler(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path, scheduler=connected_component_scheduler)
    assert P.b.path.exists()


def test_custom_scheduler_invoked(tmp_path):
    calls = []

    def recording_scheduler(ready, remaining):
        calls.append(len(ready))
        return ready

    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path, scheduler=recording_scheduler)
    assert len(calls) > 0


# ── thread budget ─────────────────────────────────────────────────────────────

def test_single_thread_budget(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path, total_threads=1)
    assert P.a.path.exists() and P.b.path.exists()


def test_autoclean_deletes_orphan(tmp_path):
    # first run: produce a and b
    P1 = Pipeline()
    P1.a = R.make_a(x="x")
    P1.b = R.make_b(P1.a)
    execute(P1, tmp_path)
    b_path = P1.b.path
    assert b_path.exists()

    # second run: only request a — b becomes ORPHAN
    P2 = Pipeline()
    P2.a = R.make_a(x="x")
    P2.b = R.make_b(P2.a)
    dag = DAG(tmp_path)
    dag.add(P2, request=[P2.a])
    dag.execute(autoclean=True)

    assert not b_path.exists()


def test_autoclean_false_leaves_orphan(tmp_path):
    P1 = Pipeline()
    P1.a = R.make_a(x="x")
    P1.b = R.make_b(P1.a)
    execute(P1, tmp_path)
    b_path = P1.b.path

    P2 = Pipeline()
    P2.a = R.make_a(x="x")
    P2.b = R.make_b(P2.a)
    dag = DAG(tmp_path)
    dag.add(P2, request=[P2.a])
    dag.execute(autoclean=False)

    assert b_path.exists()


def test_heavy_job_runs_solo(tmp_path):
    # job needing 4 threads runs even with total_threads=2 (solo when nothing else is running)
    P = Pipeline()
    P.a = R.make_a_heavy(x="x")
    execute(P, tmp_path, total_threads=2)
    assert P.a.path.exists()
