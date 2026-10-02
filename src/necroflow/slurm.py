"""SLURM execution: a `rule_call_runner` that submits one RuleCall via `sbatch`.

Drop-in replacement for the default `RuleCall.run` local runner. Pass it to
`necroflow.executor.run(rule_call_runner=slurm_runner)`, or select it from the
CLI with `--runner slurm`.

Submission is non-blocking (`sbatch --parsable`); this function then polls
`sacct` for the job's terminal state, holding the job id so it can `scancel`
the job if its poll loop fails or is stopped. Ctrl-C raises `KeyboardInterrupt`
only in the main thread, while this runs in an executor worker thread, so the
executor calls `slurm_runner.cancel()`, which stops every in-flight poll loop
(see `docs/slurm.md#interrupting-a-run`). Each running call holds its declared
`threads` against the executor's thread cap (default `os.cpu_count()`), so
jobs in flight number roughly the cap divided by per-rule `threads`.

Prerequisite: the node store must sit on a filesystem shared between the
submitting host and every compute node (NFS, Lustre, GPFS, ...). This runner
does no input/output staging; a compute node must see the exact same
`call.workdir` path, with writes visible back on the submitting host once the
job ends, or output validation and hashing will see nothing there.

The job runs the command as the local runner does: `{shellpath} -c {command}`
(`/bin/sh` without a shellpath), started in the submitting process's working
directory, so relative paths in commands resolve the same way provided that
directory is shared too.

Rules with a `materializer` (e.g. `text_file`) have no shell command to hand
to `sbatch`; they run in the submitting process via `RuleCall.run`, exactly
as under the default runner.

Not supported in this version: rules whose resolved command selects a
`Docker` input (`call.container`). They raise `NotImplementedError` before any
submission: clusters typically run containers with Apptainer rather than
Docker, and Apptainer support is work in progress.
"""

from __future__ import annotations

import shlex
import subprocess
import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from necroflow.rule_call import RuleCall

POLL_INTERVAL_SECONDS = 5.0
# A failed sacct query says nothing about the job; give up only after this
# many consecutive failed polls (about five minutes at POLL_INTERVAL_SECONDS).
SACCT_FAILURE_LIMIT = 60

# Stop events of in-flight slurm_runner calls; `slurm_runner.cancel` sets them.
_active_stops: set[threading.Event] = set()
_active_stops_lock = threading.Lock()

# sacct State values, https://slurm.schedmd.com/sacct.html#OPT_state
_SUCCESS_STATES = {"COMPLETED"}
_CANCELLED_STATES = {"CANCELLED", "TIMEOUT"}
_FAILURE_STATES = {
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
}

_SBATCH_FLAGS = {
    "partition": "--partition={value}",
    "account": "--account={value}",
    "time_limit": "--time={value}",
    "memory": "--mem={value}",
}


def _sbatch_directives(call: RuleCall, log_path: Path) -> list[str]:
    """Build `#SBATCH` directive lines from resources and `SlurmOptions`."""
    directives = [
        f"#SBATCH --job-name={call.rule.__name__}",
        f"#SBATCH --output={log_path}",
        f"#SBATCH --error={log_path}",
        f"#SBATCH --cpus-per-task={call.resources.get('threads', 1)}",
    ]
    for name, value in call.slurm_options.items():
        template = _SBATCH_FLAGS.get(name)
        flag = (
            template.format(value=value)
            if template is not None
            else f"--{name.replace('_', '-')}={value}"
        )
        directives.append(f"#SBATCH {flag}")
    return directives


def _write_sbatch_script(call: RuleCall, command: str, log_path: Path) -> Path:
    script_path = call.workdir / ".rip" / "slurm.sbatch"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    # The same `{shell} -c {command}` argv as the local runner's shell=True.
    shell = call.shellpath or "/bin/sh"
    invocation = f"exec {shlex.quote(shell)} -c {shlex.quote(command)}"
    lines = ["#!/bin/sh", *_sbatch_directives(call, log_path), "", invocation, ""]
    script_path.write_text("\n".join(lines), encoding="utf-8")
    return script_path


def _submit(script_path: Path, log_path: Path) -> str:
    """Submit the script and return its job id; sbatch's stderr goes to the job log."""
    with log_path.open("w") as log:
        # Own session: a terminal Ctrl-C must not kill sbatch mid-submission,
        # which could queue a job whose id never comes back to be cancelled.
        result = subprocess.run(
            ["sbatch", "--parsable", str(script_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            start_new_session=True,
        )
    return result.stdout.strip().split(";")[0]


def _sacct_state(job_id: str) -> tuple[str, str] | None:
    """Return `(State, ExitCode)` for the whole job allocation, or None if unknown yet."""
    result = subprocess.run(
        ["sacct", "-j", job_id, "-X", "-n", "-P", "-o", "State,ExitCode"],
        check=True,
        capture_output=True,
        text=True,
    )
    line = result.stdout.strip()
    if not line:
        return None
    state, exit_code = line.split("|", 1)
    return state.strip(), exit_code.strip()


def _await_terminal_state(job_id: str, stop: threading.Event) -> tuple[str, str]:
    """Poll until the job ends; raise `RuntimeError` once sacct keeps failing
    or `stop` is set."""
    failures = 0
    while not stop.is_set():
        try:
            status = _sacct_state(job_id)
        except subprocess.CalledProcessError as exc:
            failures += 1
            if failures >= SACCT_FAILURE_LIMIT:
                raise RuntimeError(
                    f"giving up on SLURM job {job_id} after {failures} consecutive "
                    f"sacct failures: {(exc.stderr or '').strip()}"
                ) from exc
        else:
            failures = 0
            if status is not None:
                state = status[0].split()[0]  # e.g. "CANCELLED by 1234" -> "CANCELLED"
                if state in _SUCCESS_STATES | _CANCELLED_STATES | _FAILURE_STATES:
                    return state, status[1]
        stop.wait(POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"run interrupted; cancelling SLURM job {job_id}")


def _cancel_in_flight() -> None:
    """Stop every in-flight poll loop; each then cancels its own job."""
    with _active_stops_lock:
        for stop in _active_stops:
            stop.set()


def slurm_runner(call: RuleCall, log_path: Path) -> None:
    """Submit `call` to SLURM via `sbatch`, block until terminal state.

    Materializer rules run locally via `RuleCall.run` instead.

    Raises `subprocess.CalledProcessError` on job failure (positive
    `returncode`, eligible for `repeat` retry) or on cancellation/timeout
    (negative `returncode`, also retried by `repeat`; if it ends the last
    attempt, the executor classifies the call `INTERRUPTED` — see
    `executor.py`'s `interrupted = exit_code is not None and exit_code < 0`).
    An `sbatch` rejection also raises it, with sbatch's stderr in the job log.
    After `SACCT_FAILURE_LIMIT` consecutive failed polls it cancels the job and
    raises `RuntimeError`: lost monitoring is not a command failure, so `repeat`
    does not resubmit while the cancelled job may still be shutting down.
    `slurm_runner.cancel()` makes every in-flight call cancel its job and raise
    `RuntimeError` the same way; the executor calls it when run() is interrupted.
    """
    if call.rule.materializer is not None:
        call.run(log_path)
        return
    command = call.resolve()
    if command is None:
        raise RuntimeError(f"rule {call.rule.__name__!r} has no command")
    if call.container is not None:
        raise NotImplementedError(
            f"slurm_runner: rule {call.rule.__name__!r} selects a Docker input, "
            "which is not supported under SLURM. Clusters typically run "
            "containers with Apptainer (formerly Singularity) rather than "
            "Docker; Apptainer support is work in progress."
        )
    call.workdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    script_path = _write_sbatch_script(call, command, log_path)
    stop = threading.Event()
    with _active_stops_lock:
        _active_stops.add(stop)
    try:
        job_id = _submit(script_path, log_path)
        try:
            state, exit_code = _await_terminal_state(job_id, stop)
        except BaseException:
            subprocess.run(["scancel", job_id], check=False)
            raise
    finally:
        with _active_stops_lock:
            _active_stops.discard(stop)
    if state in _SUCCESS_STATES:
        return
    if state in _CANCELLED_STATES:
        raise subprocess.CalledProcessError(-1, ["sbatch", str(script_path)])
    returncode = int(exit_code.split(":")[0]) if ":" in exit_code else 1
    raise subprocess.CalledProcessError(returncode or 1, ["sbatch", str(script_path)])


# The executor's interrupt hook; see `executor._cancel_runner_on_interrupt`.
slurm_runner.cancel = _cancel_in_flight
