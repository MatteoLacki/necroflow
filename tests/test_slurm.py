"""Tests for necroflow.slurm: sbatch script generation, sacct/scancel argv,
terminal-state mapping, and integration with the executor's retry/failure
classification.

Real `sbatch`/`sacct`/`scancel` are not on PATH in CI, so these patch the thin
subprocess wrappers `_submit`, `_sacct_state`, `_await_terminal_state`
directly rather than faking cluster binaries. Two exceptions: the sbatch
stderr test puts a tiny fake `sbatch` on PATH, since stderr routing is what it
checks; `test_real_slurm_execution` below is opt-in and needs an actual cluster.
"""

import os
import shlex
import signal
import subprocess
import threading
import time
from pathlib import Path

import pytest

from necroflow import (
    DAG,
    Docker,
    NodeType,
    Pipeline,
    RuleCallState,
    SlurmOptions,
    command,
    output,
    run,
    text_file,
)
from necroflow.rules import Constraints, Inputs, Outputs, Rule
from necroflow import slurm as slurm_module
from necroflow.slurm import slurm_runner


def run_pipeline(pipeline, **kwargs):
    """Run one Pipeline through its owning canonical DAG."""
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    return run(pipeline.dag, **kwargs)


class A(NodeType):
    filename = "a.txt"


def test_sbatch_script_has_resource_and_slurm_option_directives(tmp_path, monkeypatch):
    """threads maps to --cpus-per-task; SlurmOptions map to their sbatch flags."""
    rule = Rule(
        "make_a",
        Inputs(x=str),
        Outputs(a=A),
        "touch {a}",
        constraints=Constraints(threads=4),
        slurm=SlurmOptions(
            partition="compute",
            account="labX",
            time_limit="02:00:00",
            memory="16G",
            gres="gpu:1",
        ),
    )
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    call = P.a.rule_call

    def fake_await(job_id, stop):
        call.output_nodes["a"].path.touch()
        return "COMPLETED", "0:0"

    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "1")
    monkeypatch.setattr(slurm_module, "_await_terminal_state", fake_await)

    slurm_runner(call, call.log_path())

    script = (call.workdir / ".rip" / "slurm.sbatch").read_text()
    assert "#SBATCH --cpus-per-task=4" in script
    assert "#SBATCH --partition=compute" in script
    assert "#SBATCH --account=labX" in script
    assert "#SBATCH --time=02:00:00" in script
    assert "#SBATCH --mem=16G" in script
    assert "#SBATCH --gres=gpu:1" in script
    assert f"#SBATCH --output={call.log_path()}" in script
    assert f"#SBATCH --error={call.log_path()}" in script
    assert "touch" in script


def test_submit_invokes_sbatch_parsable(tmp_path, monkeypatch):
    """_submit shells out to `sbatch --parsable <script>` and returns the job id."""
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="123\n", stderr="")

    monkeypatch.setattr(slurm_module.subprocess, "run", fake_run)
    script_path = tmp_path / "job.sbatch"
    script_path.write_text("#!/bin/sh\ntrue\n")

    job_id = slurm_module._submit(script_path, tmp_path / "job.log")

    assert job_id == "123"
    assert calls == [["sbatch", "--parsable", str(script_path)]]


def test_submit_writes_sbatch_stderr_to_job_log(tmp_path, monkeypatch):
    """A rejected submission's reason must land in the job log.

    sbatch explains rejections (bad account, partition, directive) only on
    stderr, and the executor prints `.rip/job.log` after a failure; discarding
    stderr left the user with a bare exit status and an empty log.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_sbatch = bin_dir / "sbatch"
    fake_sbatch.write_text(
        "#!/bin/sh\n"
        "echo 'sbatch: error: Invalid account or account/partition "
        "combination specified' >&2\n"
        "exit 1\n"
    )
    fake_sbatch.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    script_path = tmp_path / "job.sbatch"
    script_path.write_text("#!/bin/sh\ntrue\n")
    log_path = tmp_path / "job.log"

    with pytest.raises(subprocess.CalledProcessError):
        slurm_module._submit(script_path, log_path)

    assert "Invalid account" in log_path.read_text()


def test_sbatch_script_invokes_command_like_local_runner(tmp_path, monkeypatch):
    """The batch script must hand the command to the same shell, with the same
    argv, as the local runner: `{shellpath} -c {command}`.

    shellpath is part of the provenance hash, so a hardcoded `/bin/sh` both
    broke shell-specific commands and recorded outputs under a shell that
    never ran them. Quotes and `$` in the command must reach that shell
    verbatim, unexpanded by the wrapper script.
    """
    record = tmp_path / "argv"
    fake_shell = tmp_path / "fakeshell"
    fake_shell.write_text(
        f"#!/bin/sh\nprintf '%s\\0' \"$@\" > {shlex.quote(str(record))}\n"
    )
    fake_shell.chmod(0o755)
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), 'echo "it\'s $HOME" > {a}')
    P = Pipeline(DAG(tmp_path / "nodes"), shellpath=fake_shell)
    P.a = rule(P, x="x")
    call = P.a.rule_call

    call.run(call.log_path())
    local_argv = record.read_text().split("\0")[:-1]

    scripts = []

    def fake_submit(script_path, log_path):
        scripts.append(script_path)
        return "1"

    monkeypatch.setattr(slurm_module, "_submit", fake_submit)
    monkeypatch.setattr(
        slurm_module, "_await_terminal_state", lambda job_id, stop: ("COMPLETED", "0:0")
    )
    slurm_runner(call, call.log_path())
    subprocess.run(["/bin/sh", str(scripts[0])], check=True)
    slurm_argv = record.read_text().split("\0")[:-1]

    assert local_argv == ["-c", call.resolve()]
    assert slurm_argv == local_argv


def test_sbatch_script_keeps_submitting_working_directory(tmp_path, monkeypatch):
    """The job must start where a local run would: the submitting process's
    working directory, which is sbatch's default.

    The local runner passes no cwd, so relative paths in commands (e.g.
    `symlink_file(P, path="data/reads.fq")`'s `realpath`) resolve against it;
    `--chdir={workdir}` silently resolved them against the workdir instead.
    """
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    call = P.a.rule_call
    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "1")
    monkeypatch.setattr(
        slurm_module, "_await_terminal_state", lambda job_id, stop: ("COMPLETED", "0:0")
    )

    slurm_runner(call, call.log_path())

    script = (call.workdir / ".rip" / "slurm.sbatch").read_text()
    assert "--chdir" not in script


def test_sacct_state_parses_pipe_delimited_output(monkeypatch):
    """`-X` restricts sacct to one line for the whole allocation, not per-step."""

    def fake_run(argv, **kwargs):
        assert argv == ["sacct", "-j", "77", "-X", "-n", "-P", "-o", "State,ExitCode"]
        return subprocess.CompletedProcess(argv, 0, stdout="COMPLETED|0:0\n", stderr="")

    monkeypatch.setattr(slurm_module.subprocess, "run", fake_run)
    assert slurm_module._sacct_state("77") == ("COMPLETED", "0:0")


def test_sacct_state_returns_none_before_accounting_catches_up(monkeypatch):
    """A job submitted moments ago may not be visible in sacct's DB yet; treat
    empty output as not-yet-terminal, not as an error."""

    def fake_run(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 0, stdout="\n", stderr="")

    monkeypatch.setattr(slurm_module.subprocess, "run", fake_run)
    assert slurm_module._sacct_state("77") is None


def test_await_terminal_state_polls_until_terminal(monkeypatch):
    """A PENDING/RUNNING sequence must not return early; only a terminal state does."""
    states = iter([None, ("RUNNING", ""), ("COMPLETED", "0:0")])
    monkeypatch.setattr(slurm_module, "_sacct_state", lambda job_id: next(states))
    monkeypatch.setattr(slurm_module, "POLL_INTERVAL_SECONDS", 0)

    assert slurm_module._await_terminal_state("1", threading.Event()) == (
        "COMPLETED",
        "0:0",
    )


def test_await_terminal_state_strips_cancelled_by_suffix(monkeypatch):
    """sacct reports `CANCELLED by <uid>`; only the bare state word is significant."""
    monkeypatch.setattr(
        slurm_module, "_sacct_state", lambda job_id: ("CANCELLED by 1234", "0:15")
    )
    monkeypatch.setattr(slurm_module, "POLL_INTERVAL_SECONDS", 0)

    assert slurm_module._await_terminal_state("1", threading.Event()) == (
        "CANCELLED",
        "0:15",
    )


@pytest.mark.parametrize(
    "state,exit_code,expect_negative",
    [
        ("FAILED", "1:0", False),
        ("NODE_FAIL", "0:9", False),
        ("OUT_OF_MEMORY", "0:0", False),
        ("CANCELLED", "0:15", True),
        ("TIMEOUT", "0:0", True),
    ],
)
def test_slurm_runner_maps_terminal_states_to_called_process_error(
    tmp_path, monkeypatch, state, exit_code, expect_negative
):
    """The executor's `repeat` retry loop catches any `CalledProcessError`
    regardless of sign; only the *final* reported/persisted state differs —
    positive code -> "failed", negative code -> "interrupted" (executor.py
    checks `exit_code < 0`)."""
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    call = P.a.rule_call

    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "1")
    monkeypatch.setattr(
        slurm_module, "_await_terminal_state", lambda job_id, stop: (state, exit_code)
    )

    with pytest.raises(subprocess.CalledProcessError) as info:
        slurm_runner(call, call.log_path())
    if expect_negative:
        assert info.value.returncode < 0
    else:
        assert info.value.returncode > 0


def test_slurm_runner_scancels_on_interruption(tmp_path, monkeypatch):
    """An interrupted poll loop must cancel the SLURM job, not orphan it."""
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    call = P.a.rule_call

    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "99")

    def raise_interrupt(job_id, stop):
        raise KeyboardInterrupt

    monkeypatch.setattr(slurm_module, "_await_terminal_state", raise_interrupt)
    scancel_calls = []

    def fake_run(argv, **kwargs):
        scancel_calls.append(argv)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(slurm_module.subprocess, "run", fake_run)

    with pytest.raises(KeyboardInterrupt):
        slurm_runner(call, call.log_path())

    assert scancel_calls == [["scancel", "99"]]


def test_ctrl_c_cancels_in_flight_slurm_jobs(tmp_path, monkeypatch):
    """Ctrl-C during a SLURM run must scancel in-flight jobs at once.

    KeyboardInterrupt reaches only the main thread, while each poll loop runs
    in an executor worker. Without the executor's cancel hook, run() waited for
    every job to end, and killing it meanwhile orphaned them on the cluster.
    """
    assert threading.current_thread() is threading.main_thread()
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    polls = []

    def sacct_then_ctrl_c(job_id):
        if not polls:
            signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)
        polls.append(job_id)
        # The job ends at the next poll, so a missing cancel fails, not hangs.
        return ("RUNNING", "") if len(polls) == 1 else ("COMPLETED", "0:0")

    scancel_calls = []
    scancelled = threading.Event()

    def fake_scancel(argv, **kwargs):
        scancel_calls.append(argv)
        scancelled.set()

    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "42")
    monkeypatch.setattr(slurm_module, "_sacct_state", sacct_then_ctrl_c)
    monkeypatch.setattr(slurm_module.subprocess, "run", fake_scancel)

    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        run_pipeline(P, rule_call_runner=slurm_runner)

    # run() may raise before the worker finishes: Ctrl-C can land while the
    # pool is still starting that worker's thread, which shutdown then skips.
    assert scancelled.wait(timeout=2)
    assert time.monotonic() - started < slurm_module.POLL_INTERVAL_SECONDS / 2
    assert polls == ["42"]
    assert scancel_calls == [["scancel", "42"]]


def _sacct_outage():
    return subprocess.CalledProcessError(
        1, ["sacct"], stderr="sacct: error: Problem talking to the database\n"
    )


def test_slurm_runner_rides_out_transient_sacct_failures(tmp_path, monkeypatch):
    """A failed sacct poll says nothing about the job, so a brief accounting
    outage must neither cancel a healthy job nor fail the call.

    Only consecutive failures count; a successful poll resets the budget.
    """
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    call = P.a.rule_call
    outage = _sacct_outage()
    polls = iter(
        [outage, outage, ("RUNNING", ""), outage, outage, ("COMPLETED", "0:0")]
    )

    def flaky_sacct(job_id):
        status = next(polls)
        if isinstance(status, Exception):
            raise status
        return status

    scancel_calls = []
    monkeypatch.setattr(slurm_module, "SACCT_FAILURE_LIMIT", 3)
    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "7")
    monkeypatch.setattr(slurm_module, "_sacct_state", flaky_sacct)
    monkeypatch.setattr(slurm_module, "POLL_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        slurm_module.subprocess,
        "run",
        lambda argv, **kwargs: scancel_calls.append(argv),
    )

    slurm_runner(call, call.log_path())

    assert scancel_calls == []


def test_slurm_runner_cancels_after_sustained_sacct_outage(tmp_path, monkeypatch):
    """After SACCT_FAILURE_LIMIT consecutive failed polls the job is cancelled,
    since an untracked job would keep writing into the workdir, and the call
    fails with sacct's own error.

    That error must not be a CalledProcessError: sacct's exit status is not the
    command's, and `repeat` resubmitting while the cancelled job may still be
    shutting down would put two jobs in one workdir.
    """
    rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")
    call = P.a.rule_call
    polls = []

    def failing_sacct(job_id):
        polls.append(job_id)
        raise _sacct_outage()

    scancel_calls = []
    monkeypatch.setattr(slurm_module, "SACCT_FAILURE_LIMIT", 3)
    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "7")
    monkeypatch.setattr(slurm_module, "_sacct_state", failing_sacct)
    monkeypatch.setattr(slurm_module, "POLL_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        slurm_module.subprocess,
        "run",
        lambda argv, **kwargs: scancel_calls.append(argv),
    )

    with pytest.raises(RuntimeError, match="Problem talking to the database"):
        slurm_runner(call, call.log_path())

    assert len(polls) == 3
    assert scancel_calls == [["scancel", "7"]]


def test_run_executes_materializer_rules_locally_under_slurm(tmp_path, monkeypatch):
    """A materializer is a Python closure with no shell command to hand to
    sbatch, so slurm_runner runs it in the submitting process like the default
    runner does.

    `necroflow init`'s template uses `text_file`; rejecting materializers made
    `--runner slurm` abort any such pipeline.
    """

    class ConfigFile(NodeType):
        filename = "config.txt"

    @text_file
    def write_config(text: str):
        config_file = output(ConfigFile)
        return config_file

    def refuse_submission(script_path, log_path):
        raise AssertionError("materializer rule was submitted to SLURM")

    monkeypatch.setattr(slurm_module, "_submit", refuse_submission)
    P = Pipeline(DAG(tmp_path))
    P.config = write_config(P, text="hello")

    run_pipeline(P, rule_call_runner=slurm_runner)

    assert P.config.path.read_text() == "hello"
    assert P.config.rule_call.state_file.read_text() == "up_to_date"


def test_slurm_runner_rejects_docker_rules(tmp_path):
    """Combining `docker run` with `sbatch` submission is not implemented;
    must fail before any submission rather than silently ignore the container,
    and point at Apptainer, the usual cluster container runtime."""

    class Text(NodeType):
        filename = "text.txt"

    @command("{env}:cat /etc/hostname > {out}")
    def docker_rule(env: Docker):
        out = output(Text)
        return out

    image = "example.org/tool@sha256:" + "0" * 64
    P = Pipeline(DAG(tmp_path))
    P.out = docker_rule(P, env=Docker(image, platform="linux/amd64"))

    with pytest.raises(NotImplementedError, match="Docker.*Apptainer"):
        slurm_runner(P.out.rule_call, P.out.rule_call.log_path())


def test_run_retries_slurm_failed_state_via_repeat(tmp_path, monkeypatch):
    """FAILED must be repeat-eligible; the executor's retry loop, not slurm.py, owns it."""
    rule = Rule(
        "flaky",
        Inputs(x=str),
        Outputs(a=A),
        "touch {a}",
        repeat=3,
    )
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")

    attempts = 0

    def fake_submit(script_path, log_path):
        return "1"

    def fake_await(job_id, stop):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return "FAILED", "1:0"
        P.a.path.touch()
        return "COMPLETED", "0:0"

    monkeypatch.setattr(slurm_module, "_submit", fake_submit)
    monkeypatch.setattr(slurm_module, "_await_terminal_state", fake_await)

    run_pipeline(P, rule_call_runner=slurm_runner)

    assert attempts == 3
    assert P.a.path.exists()
    assert P.a.rule_call.state_file.read_text() == "up_to_date"


def test_run_classifies_slurm_cancelled_as_interrupted(tmp_path, monkeypatch):
    """A cancelled/timed-out job must record `.rip/state = interrupted` after
    the final attempt, the same terminal state a locally signal-killed command
    reaches — not `failed`, which downstream tooling reading `.rip/state`
    would read as an ordinary command error rather than a stopped job."""
    rule = Rule("cancelled", Inputs(x=str), Outputs(a=A), "touch {a}")
    P = Pipeline(DAG(tmp_path))
    P.a = rule(P, x="x")

    monkeypatch.setattr(slurm_module, "_submit", lambda script_path, log_path: "1")
    monkeypatch.setattr(
        slurm_module,
        "_await_terminal_state",
        lambda job_id, stop: ("CANCELLED", "0:15"),
    )

    with pytest.raises(subprocess.CalledProcessError):
        run_pipeline(P, rule_call_runner=slurm_runner)

    assert P.a.rule_call.state_file.read_text() == "interrupted"


def test_cli_sigterm_cancels_only_with_cancellable_runner():
    """Under `--runner slurm`, SIGTERM must unwind run() like Ctrl-C, so
    in-flight jobs are cancelled rather than orphaned when the driver is
    killed (e.g. by its own SLURM time limit). Runners without a cancel hook
    keep SIGTERM's default meaning: exit at once."""
    from necroflow.cli import _sigterm_cancels

    default = signal.getsignal(signal.SIGTERM)
    with _sigterm_cancels(None):
        assert signal.getsignal(signal.SIGTERM) is default
    with pytest.raises(SystemExit) as info:
        with _sigterm_cancels(slurm_runner):
            signal.raise_signal(signal.SIGTERM)

    assert info.value.code == 128 + signal.SIGTERM
    assert signal.getsignal(signal.SIGTERM) is default


def test_cli_runner_flag_resolves_slurm(monkeypatch):
    """`--runner slurm` must resolve to the real built-in, not require users to
    know slurm.py's installed path — the same shortcut `--scheduler fifo` gets."""
    from necroflow.cli import _load_runner

    assert _load_runner(None) is None
    assert _load_runner("slurm") is slurm_runner
    with pytest.raises(SystemExit):
        _load_runner("not:a:valid:spec")


@pytest.mark.skipif(
    not os.environ.get("NECROFLOW_SLURM_TEST"),
    reason=(
        "opt-in: set NECROFLOW_SLURM_TEST=1 with a real sbatch/sacct/scancel on "
        "PATH, e.g. inside giovtorres/slurm-docker-cluster; see docs/slurm.md"
    ),
)
def test_real_slurm_execution(tmp_path):
    """Real sbatch submission, sacct polling, and output validation end to end.

    The node store must live on the cluster's *actual* shared mount, not
    pytest's `tmp_path` — on slurm-docker-cluster that's `/data`, not `/tmp`,
    which is private to whichever container the job lands on. Override with
    NECROFLOW_SLURM_TEST_DIR for other clusters' shared mount points.
    """
    import shutil
    import tempfile

    shared_root = os.environ.get("NECROFLOW_SLURM_TEST_DIR", str(tmp_path))
    nodes_dir = Path(tempfile.mkdtemp(dir=shared_root, prefix="necroflow-slurm-"))
    try:
        rule = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
        P = Pipeline(DAG(nodes_dir))
        P.a = rule(P, x="x")

        run_pipeline(P, rule_call_runner=slurm_runner)

        assert P.a.path.exists()
        assert P.a.rule_call.state_file.read_text() == "up_to_date"
    finally:
        shutil.rmtree(nodes_dir, ignore_errors=True)
