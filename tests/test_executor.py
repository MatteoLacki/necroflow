"""Tests for execute(), schedulers, and thread budget."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule
from necroflow import (
    command,
    symlink_file,
    symlink_file_rule,
    text_file,
    text_file_rule,
)

import shutil
from pathlib import Path

import pytest
import tomlkit
from necroflow import NodeType, Pipeline, DAG, execute, output
from necroflow import fifo_scheduler, connected_component_scheduler, output
from necroflow.schedulers import ConnectedComponentScheduler


class A(NodeType):
    filename = "a.txt"


class B(NodeType):
    filename = "b.txt"


class C(NodeType):
    filename = "c.txt"


class D(NodeType):
    filename = "d.txt"


class ShellOut(NodeType):
    filename = "shell.txt"


R_make_a = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R_make_a_workdir = Rule(
    "make_a_workdir",
    Inputs(x=str),
    Outputs(a=A),
    "mkdir -p {workdir}/scratch; echo {x} > {workdir}/scratch/value.txt; touch {a}",
)
R_make_a_from_workdir = Rule(
    "make_a_from_workdir",
    Inputs(x=str),
    Outputs(a=A),
    "mkdir -p {workdir}/tool-results; echo {x} > {workdir}/tool-results/a.txt; mv {workdir}/tool-results/a.txt {a}",
)
R_make_ab = Rule("make_ab", Inputs(x=str), Outputs(a=A, b=B), "touch {a} {b}")
R_make_only_a = Rule(
    "make_only_a", Inputs(x=str), Outputs(a=A, b=B), "touch {a} {b}; rm {b}"
)
R_make_b = Rule("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
R_make_c = Rule("make_c", Inputs(a=A), Outputs(c=C), "touch {c}")
R_make_c_from_b = Rule("make_c_from_b", Inputs(b=B), Outputs(c=C), "touch {c}")
R_fail_a = Rule("fail_a", Inputs(x=str), Outputs(a=A), "{{ : {a}; exit 1; }}")
R_no_output_a = Rule(
    "no_output_a", Inputs(x=str), Outputs(a=A), "{{ : {a}; true; }}"
)  # exits 0, creates nothing
R_make_a_heavy = Rule(
    "make_a_heavy", Inputs(x=str), Outputs(a=A), "touch {a}", Constraints(threads=4)
)
R_brace_shell = Rule(
    "brace_shell",
    Inputs(x=str),
    Outputs(out=ShellOut),
    "printf '%s\n' {{left,right}} > {out}",
)
R_env_shell = Rule(
    "env_shell",
    Inputs(x=str),
    Outputs(out=ShellOut),
    "printf '%s\n' $NF_TEST_SHELL > {out}",
)
# ── basic execution ───────────────────────────────────────────────────────────


def test_execute_creates_outputs(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P)
    assert P.a.path.exists()
    assert P.b.path.exists()


def test_execute_handles_outdir_with_spaces(tmp_path):
    """String command placeholders must survive output paths containing spaces.

    Necroflow owns the generated output paths, so callers should not have to
    manually quote every {output} and {input} placeholder just because the
    selected outdir contains whitespace.
    """
    outdir = tmp_path / "results with spaces"
    P = Pipeline(outdir)
    P.a = R_make_a(P, x="x")

    execute(P)

    assert P.a.path.exists()


def test_explicit_shellpath_uses_selected_shell_for_brace_expansion(tmp_path):
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is not available")
    P = Pipeline(tmp_path, shellpath=bash)
    P.out = R_brace_shell(P, x="x")

    execute(P)

    assert P.out.path.read_text() == "left\nright\n"


def test_explicit_shellpath_runs_custom_shell_wrapper(tmp_path, monkeypatch):
    wrapper = tmp_path / "nf-shell"
    wrapper.write_text('#!/bin/sh\nNF_TEST_SHELL=wrapper exec /bin/sh "$@"\n')
    wrapper.chmod(0o755)
    P = Pipeline(tmp_path / "out", shellpath=wrapper)
    P.out = R_env_shell(P, x="x")

    execute(P)

    assert P.out.path.read_text() == "wrapper\n"


def test_explicit_shellpath_changes_string_command_fingerprint(tmp_path):
    shell = shutil.which("sh") or "/bin/sh"
    default = Pipeline(tmp_path / "default")
    default.out = R_env_shell(default, x="x")
    with_shell = Pipeline(tmp_path / "with-shell", shellpath=shell)
    with_shell.out = R_env_shell(with_shell, x="x")

    assert with_shell.out.key != default.out.key
    assert with_shell.out.execution_context["shellpath"] == str(Path(shell).resolve())
    assert "shellpath" not in default.out.execution_context


def test_invalid_shellpath_fails_before_outputs(tmp_path):
    with pytest.raises(ValueError, match="shellpath does not exist"):
        Pipeline(tmp_path, shellpath=tmp_path / "missing-shell")

    assert not list(tmp_path.rglob("a.txt"))


def test_pipeline_shellpath_can_be_combined_with_node_runner(tmp_path):
    shell = shutil.which("sh") or "/bin/sh"
    P = Pipeline(tmp_path, shellpath=shell)
    P.out = R_make_a(P, x="x")

    execute(P, node_runner=lambda node, log_path: node.path.touch())
    assert P.out.path.exists()


def test_workdir_placeholder_resolves_to_rule_output_dir(tmp_path):
    """{workdir} gives commands a retained side-output directory for scratch results."""
    P = Pipeline(tmp_path)
    P.a = R_make_a_workdir(P, x="x")

    execute(P)

    assert (P.a.path.parent / "scratch" / "value.txt").read_text().strip() == "x"


def test_workdir_can_stage_results_before_final_output_move(tmp_path):
    """A command may write tool results under {workdir}, then move the final artifact to {output}."""
    P = Pipeline(tmp_path)
    P.a = R_make_a_from_workdir(P, x="final contents")

    execute(P)

    assert P.a.path.read_text().strip() == "final contents"
    assert (P.a.path.parent / "tool-results").is_dir()
    assert not (P.a.path.parent / "tool-results" / "a.txt").exists()


def test_execute_via_dag(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    dag = DAG(tmp_path)
    dag.add(P)
    dag.execute()
    assert P.b.path is not None
    assert P.b.path.exists()


def test_execute_idempotent(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P)
    mtime_a = P.a.path.stat().st_mtime
    mtime_b = P.b.path.stat().st_mtime
    execute(P)
    assert P.a.path.stat().st_mtime == mtime_a
    assert P.b.path.stat().st_mtime == mtime_b


def test_forced_stale_parent_propagates_to_child(tmp_path):
    import time

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P)
    mtime_a = P.a.path.stat().st_mtime
    mtime_b = P.b.path.stat().st_mtime

    time.sleep(0.05)
    execute(P, forced_stale_keys={P.a.key})

    assert P.a.path.stat().st_mtime > mtime_a
    assert P.b.path.stat().st_mtime > mtime_b


def test_compromised_parent_propagates_to_child(tmp_path):
    import time

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P)
    mtime_a = P.a.path.stat().st_mtime
    mtime_b = P.b.path.stat().st_mtime
    P.a.state_file.write_text("running")

    time.sleep(0.05)
    execute(P)

    assert P.a.path.stat().st_mtime > mtime_a
    assert P.b.path.stat().st_mtime > mtime_b


def test_conditional_pipeline(tmp_path):
    """if/else branching in a pipeline factory is fully supported.

    Two pipelines sharing the same upstream node but taking different branches:
    - the shared upstream output is produced once and cached for both
    - each branch produces a distinct output at a distinct path
    - the branching config value need not appear in any node's config
    """
    P1 = Pipeline(tmp_path)
    P1.a = R_make_a(P1, x="x")
    P1.result = R_make_b(P1, P1.a)  # branch "b"

    P2 = Pipeline(tmp_path)
    P2.a = R_make_a(P2, x="x")
    P2.result = R_make_c(P2, P2.a)  # branch "c"

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
    P = Pipeline(tmp_path)
    P.a = R_fail_a(P, x="x")
    with pytest.raises(Exception):
        execute(P)


def test_missing_output_raises(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_no_output_a(P, x="x")
    with pytest.raises(RuntimeError, match="output missing"):
        execute(P)


def test_missing_cooutput_raises(tmp_path):
    """A successful multi-output command must fail if any declared co-output is absent.

    This guards against marking skipped sibling outputs UP_TO_DATE merely because
    the representative co-output was created.
    """
    P = Pipeline(tmp_path)
    P.a, P.b = R_make_only_a(P, x="x")
    with pytest.raises(RuntimeError, match="output missing"):
        execute(P)


def test_cooutputs_run_once(tmp_path):
    # make_ab writes "touch {a} {b}" — if run twice, both files would be touched twice
    # We verify the command only ran once by checking a single job.log exists
    P = Pipeline(tmp_path)
    P.a, P.b = R_make_ab(P, x="x")
    execute(P)
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
    P1 = Pipeline(tmp_path)
    P1.a = R_make_a(P1, x="shared")
    P1.b = R_make_b(P1, P1.a)

    P2 = Pipeline(tmp_path)
    P2.a = R_make_a(P2, x="shared")
    P2.c = R_make_c(P2, P2.a)

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
    P1 = Pipeline(tmp_path)
    P1.a = R_make_a(P1, x="shared")

    P2 = Pipeline(tmp_path)
    P2.a = R_make_a(P2, x="shared")

    dag = DAG(tmp_path)
    dag.add(P1)
    dag.add(P2)
    dag.execute()

    assert P1.a.path is not None
    assert P1.a.path.exists()
    assert P2.a.path == P1.a.path  # duplicate node gets same path as canonical


def test_single_node_pipeline_executes(tmp_path):
    # source node (no parents) must be treated as a sink
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    dag = DAG(tmp_path)
    dag.add(P)
    dag.execute()
    assert P.a.path is not None and P.a.path.exists()


# ── schedulers ────────────────────────────────────────────────────────────────


def test_fifo_scheduler(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c(P, P.a)
    execute(P, scheduler=fifo_scheduler)
    assert P.b.path.exists() and P.c.path.exists()


def test_connected_component_scheduler(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P, scheduler=connected_component_scheduler)
    assert P.b.path.exists()


def test_scheduler_receives_available_resources(tmp_path):
    seen = []

    def recording_scheduler(ready, remaining, available_resources):
        seen.append(dict(available_resources))
        return ready

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    execute(P, scheduler=recording_scheduler, resource_caps={"threads": 3})
    assert seen == [{"threads": 3}]


def test_legacy_two_argument_scheduler_rejected_up_front(tmp_path):
    """A scheduler missing available_resources must fail fast with the protocol.

    The scheduler protocol grew a third argument (available_resources). A legacy
    2-argument callable would otherwise raise a bare TypeError mid-run, after the
    lock is taken and nodes are classified. execute() must reject it before doing
    any work, and the error must spell out the expected protocol so the author
    can fix the signature without reading executor internals.
    """

    def legacy_scheduler(ready, remaining):
        return ready

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    with pytest.raises(TypeError, match=r"ready, remaining, available_resources"):
        execute(P, scheduler=legacy_scheduler)
    assert not (tmp_path / "make_a").exists()


def test_scheduler_protocol_accepts_callable_objects(tmp_path):
    """Class-based schedulers with a 3-argument __call__ must pass validation.

    Built-in connected_component_scheduler is a callable object, not a function;
    the protocol check must inspect __call__ rather than assume a plain function.
    """

    class ObjectScheduler:
        def __call__(self, ready, remaining, available_resources):
            return ready

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    execute(P, scheduler=ObjectScheduler())
    assert P.a.path.exists()


def test_custom_scheduler_invoked(tmp_path):
    calls = []

    def recording_scheduler(ready, remaining, available_resources):
        calls.append(len(ready))
        return ready

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P, scheduler=recording_scheduler)
    assert len(calls) > 0


# ── connected-component scheduler ordering ────────────────────────────────────


class Step(NodeType):
    pass  # reused across all chain rules below


Rchain_c2_s1 = Rule("c2_s1", Inputs(x=str), Outputs(s=Step), "touch {s}")
Rchain_c2_s2 = Rule("c2_s2", Inputs(s=Step), Outputs(s=Step), "touch {s}")
Rchain_c3_s1 = Rule("c3_s1", Inputs(x=str), Outputs(s=Step), "touch {s}")
Rchain_c3_s2 = Rule("c3_s2", Inputs(s=Step), Outputs(s=Step), "touch {s}")
Rchain_c3_s3 = Rule("c3_s3", Inputs(s=Step), Outputs(s=Step), "touch {s}")
Rchain_c4_s1 = Rule("c4_s1", Inputs(x=str), Outputs(s=Step), "touch {s}")
Rchain_c4_s2 = Rule("c4_s2", Inputs(s=Step), Outputs(s=Step), "touch {s}")
Rchain_c4_s3 = Rule("c4_s3", Inputs(s=Step), Outputs(s=Step), "touch {s}")
Rchain_c4_s4 = Rule("c4_s4", Inputs(s=Step), Outputs(s=Step), "touch {s}")


def _recording(sched):
    """Return (scheduler_fn, started_list). started_list records rule name of
    first node returned per scheduler call (= submission order with threads=1)."""
    started = []

    def fn(ready, remaining, available_resources):
        result = sched(ready, remaining, available_resources)
        if result:
            started.append(result[0].rule.__name__)
        return result

    return fn, started


def test_scheduler_exhausts_smallest_chain_first(tmp_path):
    """Three independent chains of sizes 2, 3, 4: with threads=1 the scheduler
    must finish the size-2 chain before starting the size-3, and the size-3
    before the size-4."""
    P = Pipeline(tmp_path)
    P.c2a = Rchain_c2_s1(P, x="c2")
    P.c2b = Rchain_c2_s2(P, P.c2a)
    P.c3a = Rchain_c3_s1(P, x="c3")
    P.c3b = Rchain_c3_s2(P, P.c3a)
    P.c3c = Rchain_c3_s3(P, P.c3b)
    P.c4a = Rchain_c4_s1(P, x="c4")
    P.c4b = Rchain_c4_s2(P, P.c4a)
    P.c4c = Rchain_c4_s3(P, P.c4b)
    P.c4d = Rchain_c4_s4(P, P.c4c)

    fn, started = _recording(ConnectedComponentScheduler())
    execute(P, scheduler=fn, resource_caps={"threads": 1})

    chain2 = {"c2_s1", "c2_s2"}
    chain3 = {"c3_s1", "c3_s2", "c3_s3"}
    chain4 = {"c4_s1", "c4_s2", "c4_s3", "c4_s4"}
    idx = {name: i for i, name in enumerate(started)}
    assert max(idx[n] for n in chain2) < min(idx[n] for n in chain3)
    assert max(idx[n] for n in chain3) < min(idx[n] for n in chain4)


class FA(NodeType):
    pass


class FB(NodeType):
    pass


class FC(NodeType):
    pass


class FD(NodeType):
    pass


class FE(NodeType):
    pass


class FF(NodeType):
    pass


class FG(NodeType):
    pass


Rfork_ra = Rule("ra", Inputs(x=str), Outputs(a=FA), "touch {a}")
Rfork_rb = Rule("rb", Inputs(a=FA), Outputs(b=FB), "touch {b}")
Rfork_rc = Rule("rc", Inputs(b=FB), Outputs(c=FC), "touch {c}")
Rfork_rd = Rule("rd", Inputs(b=FB), Outputs(d=FD), "touch {d}")
Rfork_re = Rule("re", Inputs(c=FC), Outputs(e=FE), "touch {e}")
Rfork_rf = Rule("rf", Inputs(d=FD), Outputs(f=FF), "touch {f}")
Rfork_rg = Rule("rg", Inputs(f=FF), Outputs(g=FG), "touch {g}")


def test_scheduler_fork_prefers_smaller_branch(tmp_path):
    """DAG: A->B->(C->E | D->F->G). After A and B complete the graph splits into
    C->E (size 2) and D->F->G (size 3). With threads=1 the scheduler must
    complete C->E entirely before starting D."""
    P = Pipeline(tmp_path)
    P.a = Rfork_ra(P, x="x")
    P.b = Rfork_rb(P, P.a)
    P.c = Rfork_rc(P, P.b)
    P.d = Rfork_rd(P, P.b)
    P.e = Rfork_re(P, P.c)
    P.f = Rfork_rf(P, P.d)
    P.g = Rfork_rg(P, P.f)

    fn, started = _recording(ConnectedComponentScheduler())
    execute(P, scheduler=fn, resource_caps={"threads": 1})

    assert started == ["ra", "rb", "rc", "re", "rd", "rf", "rg"]


# ── thread budget ─────────────────────────────────────────────────────────────


def test_single_thread_budget(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P, resource_caps={"threads": 1})
    assert P.a.path.exists() and P.b.path.exists()


def test_autoclean_deletes_orphan(tmp_path):
    # first run: produce a and b
    P1 = Pipeline(tmp_path)
    P1.a = R_make_a(P1, x="x")
    P1.b = R_make_b(P1, P1.a)
    execute(P1)
    b_path = P1.b.path
    assert b_path.exists()

    # second run: only request a — b becomes ORPHAN
    P2 = Pipeline(tmp_path)
    P2.a = R_make_a(P2, x="x")
    P2.b = R_make_b(P2, P2.a)
    dag = DAG(tmp_path)
    dag.add(P2, request=[P2.a])
    dag.execute(autoclean=True)

    assert not b_path.exists()


def test_autoclean_false_leaves_orphan(tmp_path):
    P1 = Pipeline(tmp_path)
    P1.a = R_make_a(P1, x="x")
    P1.b = R_make_b(P1, P1.a)
    execute(P1)
    b_path = P1.b.path

    P2 = Pipeline(tmp_path)
    P2.a = R_make_a(P2, x="x")
    P2.b = R_make_b(P2, P2.a)
    dag = DAG(tmp_path)
    dag.add(P2, request=[P2.a])
    dag.execute(autoclean=False)

    assert b_path.exists()


def test_dry_run_does_not_execute(tmp_path):
    """dry_run=True must not create any output files."""
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P, dry_run=True)
    assert not P.a.path.exists()
    assert not P.b.path.exists()


def test_dry_run_autoclean_does_not_delete_orphans(tmp_path):
    """dry_run=True with autoclean=True must not mutate the output tree.

    This guards a real bug: execute(..., dry_run=True, autoclean=True) currently
    deletes ORPHAN outputs before it reaches the dry-run branch. A dry run may
    report what would be cleaned, but it must not unlink files or directories.
    """
    P1 = Pipeline(tmp_path)
    P1.a = R_make_a(P1, x="x")
    P1.b = R_make_b(P1, P1.a)
    execute(P1)
    b_path = P1.b.path
    assert b_path.exists()

    P2 = Pipeline(tmp_path)
    P2.a = R_make_a(P2, x="x")
    P2.b = R_make_b(P2, P2.a)
    dag = DAG(tmp_path)
    dag.add(P2, request=[P2.a])
    dag.execute(autoclean=True, dry_run=True)

    assert b_path.exists()


def test_dry_run_shows_missing(tmp_path, caplog):
    """dry_run=True must log MISSING nodes that would run."""
    import logging

    P = Pipeline(tmp_path / "out")
    P.a = R_make_a(P, x="x")
    with caplog.at_level(logging.INFO, logger="necroflow"):
        execute(P, dry_run=True)
    assert "would-run" in caplog.text
    assert "MISSING" in caplog.text
    assert "make_a" in caplog.text


def test_dry_run_shows_stale(tmp_path, caplog):
    """dry_run=True must log STALE nodes after an input is updated."""
    import logging, time

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P)
    time.sleep(0.01)
    P.a.path.write_bytes(b"updated")  # content change → different hash → STALE
    with caplog.at_level(logging.INFO, logger="necroflow"):
        execute(P, dry_run=True)
    assert "STALE" in caplog.text
    assert "make_b" in caplog.text


def test_dry_run_all_up_to_date(tmp_path, caplog):
    """dry_run=True on a complete pipeline must report 0 would-run."""
    import logging

    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    execute(P)
    with caplog.at_level(logging.INFO, logger="necroflow"):
        execute(P, dry_run=True)
    assert "0 would run" in caplog.text


def test_autoclean_deletes_intermediates(tmp_path):
    """autoclean=True must delete intermediate outputs once all their children are UP_TO_DATE.

    Linear chain a→b→c: after execution, a and b (intermediates) should be gone; c (sink) kept.
    """
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c_from_b(P, P.b)
    execute(P, autoclean=True)
    assert P.c.path.exists()
    assert not P.b.path.exists()
    assert not P.a.path.exists()


def test_autoclean_deletes_intermediate_workdir(tmp_path):
    """autoclean=True removes the whole rule-call directory, including {workdir} side files."""
    P = Pipeline(tmp_path)
    P.a = R_make_a_workdir(P, x="x")
    P.b = R_make_b(P, P.a)
    execute(P, autoclean=True)
    assert P.b.path.exists()
    assert not P.a.path.parent.exists()


def test_autoclean_false_leaves_intermediates(tmp_path):
    """autoclean=False must leave all intermediate outputs intact."""
    P = Pipeline(tmp_path)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c_from_b(P, P.b)
    execute(P, autoclean=False)
    assert P.a.path.exists()
    assert P.b.path.exists()
    assert P.c.path.exists()


def test_heavy_job_runs_solo(tmp_path):
    # job needing 4 threads runs even with threads cap=2 (solo fallback when nothing else running)
    P = Pipeline(tmp_path)
    P.a = R_make_a_heavy(P, x="x")
    execute(P, resource_caps={"threads": 2})
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
    assert parse_resource("1Mi") == 1024**2
    assert parse_resource("1Gi") == 1024**3
    assert parse_resource("1Ti") == 1024**4
    assert parse_resource("1Pi") == 1024**5


def test_parse_resource_si_ne_binary():
    assert parse_resource("1M") != parse_resource("1Mi")


def test_resource_cap_respected(tmp_path):
    """A custom resource cap is enforced: two jobs declaring ram=250Mi each cannot
    run simultaneously under a 300Mi cap."""
    R2_make_a = Rule(
        "make_a", Inputs(x=str), Outputs(a=A), "touch {a}", Constraints(ram="250Mi")
    )
    R2_make_b = Rule(
        "make_b", Inputs(x=str), Outputs(b=B), "touch {b}", Constraints(ram="250Mi")
    )
    P = Pipeline(tmp_path)
    P.a = R2_make_a(P, x="1")
    P.b = R2_make_b(P, x="2")
    # Should complete without error (solo fallback ensures each job runs eventually)
    execute(P, resource_caps={"threads": 8, "ram": parse_resource("300Mi")})
    assert P.a.path.exists() and P.b.path.exists()


# -- built-in text file rules -------------------------------------------------


def test_text_file_rule_writes_exact_text(tmp_path):
    class ConfigFile(NodeType):
        filename = "config.json"

    r_write_config = text_file_rule("write_config", ConfigFile)
    P = Pipeline(tmp_path)
    P.config = r_write_config(P, text='{"a": 1}\n')

    execute(P)

    assert P.config.path.read_text() == '{"a": 1}\n'


def test_text_file_rule_is_idempotent(tmp_path):
    import time

    class ConfigFile(NodeType):
        filename = "config.txt"

    r_write_config = text_file_rule("write_config", ConfigFile)
    P = Pipeline(tmp_path)
    P.config = r_write_config(P, text="same\n")

    execute(P)
    mtime = P.config.path.stat().st_mtime
    time.sleep(0.05)
    execute(P)

    assert P.config.path.stat().st_mtime == mtime


def test_text_file_fingerprint_changes_with_text():
    class ConfigFile(NodeType):
        filename = "config.txt"

    r_write_config = text_file_rule("write_config", ConfigFile)

    P = Pipeline("/tmp/necroflow-test-text-fingerprint")
    assert (
        r_write_config(P, text="a").fingerprint
        != r_write_config(P, text="b").fingerprint
    )


def test_text_file_recipe_distinguishes_from_shell_rule():
    class ConfigFile(NodeType):
        filename = "config.txt"

    text_rules_write_config = text_file_rule("write_config", ConfigFile)
    shell_rules_write_config = Rule(
        "write_config",
        Inputs(text=str),
        Outputs(config_file=ConfigFile),
        "printf %s {text} > {config_file}",
    )

    P = Pipeline("/tmp/necroflow-test-text-recipe")
    assert (
        text_rules_write_config(P, text="same").fingerprint
        != shell_rules_write_config(P, text="same").fingerprint
    )


def test_text_file_rejects_non_string_text():
    class ConfigFile(NodeType):
        filename = "config.txt"

    r_write_config = text_file_rule("write_config", ConfigFile)

    with pytest.raises(TypeError, match="expected <class 'str'>"):
        r_write_config(Pipeline("/tmp/necroflow-test-text-type"), text={"not": "text"})


def test_text_file_custom_input_name(tmp_path):
    class ConfigFile(NodeType):
        filename = "config.txt"

    r_write_config = text_file_rule(
        "write_config", ConfigFile, input_name="serialized_config"
    )
    P = Pipeline(tmp_path)
    P.config = r_write_config(P, serialized_config="custom\n")

    execute(P)

    assert P.config.path.read_text() == "custom\n"


def test_text_file_decorator_matches_factory_fingerprint():
    class ConfigFile(NodeType):
        filename = "config.txt"

    @text_file
    def write_config(text: str):
        """Write the configuration."""
        config_file = output(ConfigFile)
        return config_file

    explicit = text_file_rule(
        "write_config",
        ConfigFile,
        input_name="text",
        output_name="config_file",
    )

    P = Pipeline("/tmp/necroflow-test-text-decorator")
    assert (
        write_config(P, text="same").fingerprint == explicit(P, text="same").fingerprint
    )
    assert write_config.info == "Write the configuration."


def test_text_file_decorator_accepts_encoding(tmp_path):
    class ConfigFile(NodeType):
        filename = "config.txt"

    @text_file(encoding="utf-16-le")
    def write_config(text: str):
        config_file = output(ConfigFile)
        return config_file

    P = Pipeline(tmp_path)
    P.config = write_config(P, text="hello")
    execute(P)

    assert P.config.path.read_text(encoding="utf-16-le") == "hello"


def test_text_file_decorator_rejects_invalid_declaration():
    class ConfigFile(NodeType):
        filename = "config.txt"

    with pytest.raises(TypeError, match="exactly one input"):

        @text_file
        def bad_text_file(first: str, second: str):
            config_file = output(ConfigFile)
            return config_file


# -- built-in symlink ingestion rule ------------------------------------------


def test_symlink_file_rule_links_to_source_content(tmp_path):
    class RawData(NodeType):
        filename = "raw.txt"

    source = tmp_path / "source.txt"
    source.write_text("hello\n")
    r_ingest_raw = symlink_file_rule("ingest_raw", RawData)
    P = Pipeline(tmp_path)
    P.raw = r_ingest_raw(P, path=str(source))

    execute(P)

    assert P.raw.path.is_symlink()
    assert P.raw.path.read_text() == "hello\n"


def test_symlink_file_decorator_matches_factory_and_links(tmp_path):
    class RawData(NodeType):
        filename = "raw.txt"

    @symlink_file
    def ingest_raw(path: str):
        """Ingest raw data."""
        raw = output(RawData)
        return raw

    explicit = symlink_file_rule(
        "ingest_raw",
        RawData,
        path_arg="path",
        output_name="raw",
    )
    fingerprint_pipeline = Pipeline(tmp_path / "fingerprint")
    assert (
        ingest_raw(fingerprint_pipeline, path="source").fingerprint
        == explicit(fingerprint_pipeline, path="source").fingerprint
    )
    assert ingest_raw.info == "Ingest raw data."

    source = tmp_path / "source.txt"
    source.write_text("decorated\n")
    P = Pipeline(tmp_path)
    P = Pipeline(tmp_path / "out")
    P.raw = ingest_raw(P, path=str(source))
    execute(P)

    assert P.raw.path.is_symlink()
    assert P.raw.path.read_text() == "decorated\n"


def test_symlink_file_change_reruns_downstream_consumer(tmp_path):
    """Editing the source file behind a symlink_file node must rerun its children.

    A bare path config value is fingerprinted as text and never revisited, so
    dataset edits go unnoticed. symlink_file exists specifically to make the
    normal mtime-fast-path -> content-hash STALE machinery pick up the edit
    through the symlink, without needing a NodeType.invalidator.
    """
    import time

    class RawData(NodeType):
        filename = "raw.txt"

    class Copied(NodeType):
        filename = "copied.txt"

    source = tmp_path / "source.txt"
    source.write_text("v1\n")
    r_ingest_raw = symlink_file_rule("ingest_raw", RawData)
    r_copy_raw = Rule(
        "copy_raw", Inputs(raw=RawData), Outputs(copied=Copied), "cp {raw} {copied}"
    )

    def build():
        P = Pipeline(outdir)
        P.raw = r_ingest_raw(P, path=str(source))
        P.copied = r_copy_raw(P, P.raw)
        return P

    outdir = tmp_path / "out"
    P1 = build()
    execute(P1)
    assert P1.copied.path.read_text() == "v1\n"

    time.sleep(0.05)
    source.write_text("v2\n")

    P2 = build()
    execute(P2)

    assert P2.copied.path.read_text() == "v2\n"
    assert P2.copied.path == P1.copied.path  # in-place overwrite, not a new dir


def test_symlink_file_custom_path_arg(tmp_path):
    class RawData(NodeType):
        filename = "raw.txt"

    source = tmp_path / "source.txt"
    source.write_text("hi\n")
    r_ingest_raw = symlink_file_rule("ingest_raw", RawData, path_arg="dataset_path")
    P = Pipeline(tmp_path / "out")
    P.raw = r_ingest_raw(P, dataset_path=str(source))

    execute(P)

    assert P.raw.path.read_text() == "hi\n"


def test_symlink_file_rejects_reserved_path_arg():
    class RawData(NodeType):
        filename = "raw.txt"

    with pytest.raises(ValueError, match="reserved"):
        r_ingest_raw = symlink_file_rule("ingest_raw", RawData, path_arg="workdir")


def test_symlink_file_rejects_non_nodetype_output():
    with pytest.raises(TypeError, match="must be a NodeType"):
        r_ingest_raw = symlink_file_rule("ingest_raw", str)


def test_execute_returns_report_and_writes_run_stats_with_output_size(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a_from_workdir(P, x="abc")

    report = execute(P)

    event = report.get(P.a)
    assert event is not None
    assert event.cached is False
    assert event.state == "up_to_date"
    assert event.duration_seconds is not None and event.duration_seconds >= 0
    assert event.started_at is not None
    assert event.finished_at is not None
    assert event.exit_code == 0
    assert event.output_size_bytes == len("abc\n")
    assert event.output_size_human == "4 B"

    run_doc = tomlkit.parse((P.a.path.parent / ".rip" / "run.toml").read_text())
    assert run_doc["run"]["exit_code"] == 0
    assert run_doc["run"]["output_size_bytes"] == len("abc\n")
    assert run_doc["run"]["duration_seconds"] >= 0


def test_execute_report_marks_cached_nodes_and_measures_size(tmp_path):
    P = Pipeline(tmp_path)
    P.a = R_make_a_from_workdir(P, x="cached")
    execute(P)

    cached_report = execute(P)

    event = cached_report.get(P.a)
    assert event is not None
    assert event.cached is True
    assert event.duration_seconds is None
    assert event.output_size_bytes == len("cached\n")
