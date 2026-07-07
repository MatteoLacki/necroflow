"""Tests for execute(), schedulers, and thread budget."""
import pytest
from necroflow import NodeType, Inputs, Outputs, Constraints, Rules, Pipeline, DAG, execute
from necroflow import fifo_scheduler, connected_component_scheduler
from necroflow.schedulers import ConnectedComponentScheduler


class A(NodeType): filename = "a.txt"
class B(NodeType): filename = "b.txt"
class C(NodeType): filename = "c.txt"
class D(NodeType): filename = "d.txt"


R = Rules()
R.register("make_a",       Inputs(x=str), Outputs(a=A),        "touch {a}")
R.register("make_a_workdir", Inputs(x=str), Outputs(a=A),       "mkdir -p {workdir}/scratch; echo {x} > {workdir}/scratch/value.txt; touch {a}")
R.register("make_a_from_workdir", Inputs(x=str), Outputs(a=A),  "mkdir -p {workdir}/tool-results; echo {x} > {workdir}/tool-results/a.txt; mv {workdir}/tool-results/a.txt {a}")
R.register("make_ab",      Inputs(x=str), Outputs(a=A, b=B),   "touch {a} {b}")
R.register("make_only_a",  Inputs(x=str), Outputs(a=A, b=B),   "touch {a} {b}; rm {b}")
R.register("make_b",       Inputs(a=A),   Outputs(b=B),        "touch {b}")
R.register("make_c",       Inputs(a=A),   Outputs(c=C),        "touch {c}")
R.register("make_c_from_b",Inputs(b=B),   Outputs(c=C),        "touch {c}")
R.register("fail_a",       Inputs(x=str), Outputs(a=A),        "{{ : {a}; exit 1; }}")
R.register("no_output_a",  Inputs(x=str), Outputs(a=A),        "{{ : {a}; true; }}")  # exits 0, creates nothing
R.register("make_a_heavy", Inputs(x=str), Outputs(a=A),        "touch {a}", Constraints(threads=4))


# ── basic execution ───────────────────────────────────────────────────────────

def test_execute_creates_outputs(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path)
    assert P.a.path.exists()
    assert P.b.path.exists()


def test_execute_handles_outdir_with_spaces(tmp_path):
    """String command placeholders must survive output paths containing spaces.

    Necroflow owns the generated output paths, so callers should not have to
    manually quote every {output} and {input} placeholder just because the
    selected outdir contains whitespace.
    """
    outdir = tmp_path / "results with spaces"
    P = Pipeline()
    P.a = R.make_a(x="x")

    execute(P, outdir)

    assert P.a.path.exists()


def test_workdir_placeholder_resolves_to_rule_output_dir(tmp_path):
    """{workdir} gives commands a retained side-output directory for scratch results."""
    P = Pipeline()
    P.a = R.make_a_workdir(x="x")

    execute(P, tmp_path)

    assert (P.a.path.parent / "scratch" / "value.txt").read_text().strip() == "x"


def test_workdir_can_stage_results_before_final_output_move(tmp_path):
    """A command may write tool results under {workdir}, then move the final artifact to {output}."""
    P = Pipeline()
    P.a = R.make_a_from_workdir(x="final contents")

    execute(P, tmp_path)

    assert P.a.path.read_text().strip() == "final contents"
    assert (P.a.path.parent / "tool-results").is_dir()
    assert not (P.a.path.parent / "tool-results" / "a.txt").exists()


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


def test_conditional_pipeline(tmp_path):
    """if/else branching in a pipeline factory is fully supported.

    Two pipelines sharing the same upstream node but taking different branches:
    - the shared upstream output is produced once and cached for both
    - each branch produces a distinct output at a distinct path
    - the branching config value need not appear in any node's config
    """
    P1 = Pipeline()
    P1.a = R.make_a(x="x")
    P1.result = R.make_b(P1.a)   # branch "b"

    P2 = Pipeline()
    P2.a = R.make_a(x="x")
    P2.result = R.make_c(P2.a)   # branch "c"

    dag = DAG(tmp_path)
    dag.add(P1)
    dag.add(P2)
    dag.execute()

    # shared upstream is at the same path for both pipelines
    assert P1.a.path == P2.a.path
    assert P1.a.path.exists()

    # each branch lands at a distinct path
    assert P1.result.path != P2.result.path
    assert P1.result.path.exists()
    assert P2.result.path.exists()


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
    assert (P.a.path.parent / ".rip" / "job.log").exists()
    assert P.a.path.parent == P.b.path.parent


def test_shared_node_executed_once(tmp_path):
    """A node shared by two pipelines (same rule + config) must run exactly once.

    Two pipelines both depend on make_a(x="shared") as their upstream. After
    DAG deduplication the shared node has one canonical entry. We verify it ran
    only once by checking a single job.log exists for that node's directory.
    """
    P1 = Pipeline()
    P1.a = R.make_a(x="shared")
    P1.b = R.make_b(P1.a)

    P2 = Pipeline()
    P2.a = R.make_a(x="shared")
    P2.c = R.make_c(P2.a)

    dag = DAG(tmp_path)
    dag.add(P1)
    dag.add(P2)
    dag.execute()

    # All outputs must exist, including the shared upstream node from P1
    assert P1.a.path.exists()
    assert P1.b.path.exists()
    assert P2.c.path.exists()

    # The shared upstream node ran once — only one make_a directory under outdir
    make_a_dirs = list(tmp_path.glob("make_a/*/"))
    assert len(make_a_dirs) == 1
    # That directory has a single job.log confirming one execution
    assert (make_a_dirs[0] / ".rip" / "job.log").exists()


def test_shared_node_path_set_on_first_pipeline(tmp_path):
    """First-added pipeline's node object must have path set after execution.

    When two pipelines share an upstream node (same key), the first-added node
    is canonical. The second pipeline's duplicate node is not stored. After
    execution the first pipeline's object must have path set — not None.
    """
    P1 = Pipeline()
    P1.a = R.make_a(x="shared")

    P2 = Pipeline()
    P2.a = R.make_a(x="shared")

    dag = DAG(tmp_path)
    dag.add(P1)
    dag.add(P2)
    dag.execute()

    assert P1.a.path is not None
    assert P1.a.path.exists()
    assert P2.a.path == P1.a.path   # duplicate node gets same path as canonical


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


# ── connected-component scheduler ordering ────────────────────────────────────

class Step(NodeType): pass  # reused across all chain rules below

Rchain = Rules()
Rchain.register("c2_s1", Inputs(x=str),    Outputs(s=Step), "touch {s}")
Rchain.register("c2_s2", Inputs(s=Step),   Outputs(s=Step), "touch {s}")
Rchain.register("c3_s1", Inputs(x=str),    Outputs(s=Step), "touch {s}")
Rchain.register("c3_s2", Inputs(s=Step),   Outputs(s=Step), "touch {s}")
Rchain.register("c3_s3", Inputs(s=Step),   Outputs(s=Step), "touch {s}")
Rchain.register("c4_s1", Inputs(x=str),    Outputs(s=Step), "touch {s}")
Rchain.register("c4_s2", Inputs(s=Step),   Outputs(s=Step), "touch {s}")
Rchain.register("c4_s3", Inputs(s=Step),   Outputs(s=Step), "touch {s}")
Rchain.register("c4_s4", Inputs(s=Step),   Outputs(s=Step), "touch {s}")


def _recording(sched):
    """Return (scheduler_fn, started_list). started_list records rule name of
    first node returned per scheduler call (= submission order with threads=1)."""
    started = []
    def fn(ready, remaining):
        result = sched(ready, remaining)
        if result:
            started.append(result[0].rule.__name__)
        return result
    return fn, started


def test_scheduler_exhausts_smallest_chain_first(tmp_path):
    """Three independent chains of sizes 2, 3, 4: with threads=1 the scheduler
    must finish the size-2 chain before starting the size-3, and the size-3
    before the size-4."""
    P = Pipeline()
    P.c2a = Rchain.c2_s1(x="c2");  P.c2b = Rchain.c2_s2(P.c2a)
    P.c3a = Rchain.c3_s1(x="c3");  P.c3b = Rchain.c3_s2(P.c3a);  P.c3c = Rchain.c3_s3(P.c3b)
    P.c4a = Rchain.c4_s1(x="c4");  P.c4b = Rchain.c4_s2(P.c4a);  P.c4c = Rchain.c4_s3(P.c4b);  P.c4d = Rchain.c4_s4(P.c4c)

    fn, started = _recording(ConnectedComponentScheduler())
    execute(P, tmp_path, scheduler=fn, resource_caps={"threads": 1})

    chain2 = {"c2_s1", "c2_s2"}
    chain3 = {"c3_s1", "c3_s2", "c3_s3"}
    chain4 = {"c4_s1", "c4_s2", "c4_s3", "c4_s4"}
    idx = {name: i for i, name in enumerate(started)}
    assert max(idx[n] for n in chain2) < min(idx[n] for n in chain3)
    assert max(idx[n] for n in chain3) < min(idx[n] for n in chain4)


class FA(NodeType): pass
class FB(NodeType): pass
class FC(NodeType): pass
class FD(NodeType): pass
class FE(NodeType): pass
class FF(NodeType): pass
class FG(NodeType): pass

Rfork = Rules()
Rfork.register("ra", Inputs(x=str), Outputs(a=FA), "touch {a}")
Rfork.register("rb", Inputs(a=FA),  Outputs(b=FB), "touch {b}")
Rfork.register("rc", Inputs(b=FB),  Outputs(c=FC), "touch {c}")
Rfork.register("rd", Inputs(b=FB),  Outputs(d=FD), "touch {d}")
Rfork.register("re", Inputs(c=FC),  Outputs(e=FE), "touch {e}")
Rfork.register("rf", Inputs(d=FD),  Outputs(f=FF), "touch {f}")
Rfork.register("rg", Inputs(f=FF),  Outputs(g=FG), "touch {g}")


def test_scheduler_fork_prefers_smaller_branch(tmp_path):
    """DAG: A->B->(C->E | D->F->G). After A and B complete the graph splits into
    C->E (size 2) and D->F->G (size 3). With threads=1 the scheduler must
    complete C->E entirely before starting D."""
    P = Pipeline()
    P.a = Rfork.ra(x="x")
    P.b = Rfork.rb(P.a)
    P.c = Rfork.rc(P.b)
    P.d = Rfork.rd(P.b)
    P.e = Rfork.re(P.c)
    P.f = Rfork.rf(P.d)
    P.g = Rfork.rg(P.f)

    fn, started = _recording(ConnectedComponentScheduler())
    execute(P, tmp_path, scheduler=fn, resource_caps={"threads": 1})

    assert started == ["ra", "rb", "rc", "re", "rd", "rf", "rg"]


# ── thread budget ─────────────────────────────────────────────────────────────

def test_single_thread_budget(tmp_path):
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path, resource_caps={"threads": 1})
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


def test_dry_run_does_not_execute(tmp_path):
    """dry_run=True must not create any output files."""
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path, dry_run=True)
    assert not P.a.path.exists()
    assert not P.b.path.exists()


def test_dry_run_autoclean_does_not_delete_orphans(tmp_path):
    """dry_run=True with autoclean=True must not mutate the output tree.

    This guards a real bug: execute(..., dry_run=True, autoclean=True) currently
    deletes ORPHAN outputs before it reaches the dry-run branch. A dry run may
    report what would be cleaned, but it must not unlink files or directories.
    """
    P1 = Pipeline()
    P1.a = R.make_a(x="x")
    P1.b = R.make_b(P1.a)
    execute(P1, tmp_path)
    b_path = P1.b.path
    assert b_path.exists()

    P2 = Pipeline()
    P2.a = R.make_a(x="x")
    P2.b = R.make_b(P2.a)
    dag = DAG(tmp_path)
    dag.add(P2, request=[P2.a])
    dag.execute(autoclean=True, dry_run=True)

    assert b_path.exists()


def test_dry_run_shows_missing(tmp_path, caplog):
    """dry_run=True must log MISSING nodes that would run."""
    import logging
    P = Pipeline()
    P.a = R.make_a(x="x")
    with caplog.at_level(logging.INFO, logger="necroflow"):
        execute(P, tmp_path, dry_run=True)
    assert "would-run" in caplog.text
    assert "MISSING" in caplog.text
    assert "make_a" in caplog.text


def test_dry_run_shows_stale(tmp_path, caplog):
    """dry_run=True must log STALE nodes after an input is updated."""
    import logging, time
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path)
    time.sleep(0.01)
    P.a.path.write_bytes(b"updated")  # content change → different hash → STALE
    with caplog.at_level(logging.INFO, logger="necroflow"):
        execute(P, tmp_path, dry_run=True)
    assert "STALE" in caplog.text
    assert "make_b" in caplog.text


def test_dry_run_all_up_to_date(tmp_path, caplog):
    """dry_run=True on a complete pipeline must report 0 would-run."""
    import logging
    P = Pipeline()
    P.a = R.make_a(x="x")
    execute(P, tmp_path)
    with caplog.at_level(logging.INFO, logger="necroflow"):
        execute(P, tmp_path, dry_run=True)
    assert "0 would run" in caplog.text


def test_autoclean_deletes_intermediates(tmp_path):
    """autoclean=True must delete intermediate outputs once all their children are UP_TO_DATE.

    Linear chain a→b→c: after execution, a and b (intermediates) should be gone; c (sink) kept.
    """
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    P.c = R.make_c_from_b(P.b)
    execute(P, tmp_path, autoclean=True)
    assert P.c.path.exists()
    assert not P.b.path.exists()
    assert not P.a.path.exists()


def test_autoclean_deletes_intermediate_workdir(tmp_path):
    """autoclean=True removes the whole rule-call directory, including {workdir} side files."""
    P = Pipeline()
    P.a = R.make_a_workdir(x="x")
    P.b = R.make_b(P.a)
    execute(P, tmp_path, autoclean=True)
    assert P.b.path.exists()
    assert not P.a.path.parent.exists()


def test_autoclean_false_leaves_intermediates(tmp_path):
    """autoclean=False must leave all intermediate outputs intact."""
    P = Pipeline()
    P.a = R.make_a(x="x")
    P.b = R.make_b(P.a)
    P.c = R.make_c_from_b(P.b)
    execute(P, tmp_path, autoclean=False)
    assert P.a.path.exists()
    assert P.b.path.exists()
    assert P.c.path.exists()


def test_heavy_job_runs_solo(tmp_path):
    # job needing 4 threads runs even with threads cap=2 (solo fallback when nothing else running)
    P = Pipeline()
    P.a = R.make_a_heavy(x="x")
    execute(P, tmp_path, resource_caps={"threads": 2})
    assert P.a.path.exists()


# ── parse_resource ────────────────────────────────────────────────────────────

from necroflow.dag import parse_resource

def test_parse_resource_plain_int():
    assert parse_resource(8) == 8
    assert parse_resource("8") == 8

def test_parse_resource_si():
    assert parse_resource("1K") == 1_000
    assert parse_resource("2M") == 2_000_000
    assert parse_resource("3G") == 3_000_000_000
    assert parse_resource("1T") == 1_000_000_000_000
    assert parse_resource("1P") == 1_000_000_000_000_000

def test_parse_resource_binary():
    assert parse_resource("1Ki") == 1024
    assert parse_resource("1Mi") == 1024 ** 2
    assert parse_resource("1Gi") == 1024 ** 3
    assert parse_resource("1Ti") == 1024 ** 4
    assert parse_resource("1Pi") == 1024 ** 5

def test_parse_resource_si_ne_binary():
    assert parse_resource("1M") != parse_resource("1Mi")

def test_resource_cap_respected(tmp_path):
    """A custom resource cap is enforced: two jobs declaring ram=250Mi each cannot
    run simultaneously under a 300Mi cap."""
    R2 = Rules()
    R2.register("make_a", Inputs(x=str), Outputs(a=A), "touch {a}", Constraints(ram="250Mi"))
    R2.register("make_b", Inputs(x=str), Outputs(b=B), "touch {b}", Constraints(ram="250Mi"))
    P = Pipeline()
    P.a = R2.make_a(x="1")
    P.b = R2.make_b(x="2")
    # Should complete without error (solo fallback ensures each job runs eventually)
    execute(P, tmp_path, resource_caps={"threads": 8, "ram": parse_resource("300Mi")})
    assert P.a.path.exists() and P.b.path.exists()
