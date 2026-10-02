# SLURM

[Previous: Scheduler Protocol](schedulers.md) | [README](../README.md) | [Next: Release](release.md)

`necroflow.slurm.slurm_runner` is a `rule_call_runner` that submits one `RuleCall` to a
SLURM cluster via `sbatch`, in place of the default local-subprocess runner. It plugs into
the existing runner hook (`docs/executor.md#runner-and-retries`); nothing else about
planning, caching, scheduling, or reporting changes.

## Prerequisite: shared filesystem

The node store must sit on a filesystem shared between the submitting host and every
compute node (NFS, Lustre, GPFS, ...). `slurm_runner` does no input/output staging — a
submitted job writes into `call.workdir` on whichever compute node runs it, and once the
job ends the *submitting* process validates outputs and computes hashes at that same path.
If the compute node cannot write to a location the submitting host can then read, output
validation sees nothing there and the call is reported `MISSING`.

This mirrors how Snakemake's and Nextflow's own SLURM/grid executors work: both assume a
shared POSIX mount for classic HPC schedulers, and only do explicit staging for
object-storage-backed cloud executors (S3, GCS, Kubernetes, ...), which necroflow does not
target.

## Enabling it

CLI:

```bash
necroflow run --runner slurm job.toml
```

Python API:

```python
from necroflow.slurm import slurm_runner

dag.run(rule_call_runner=slurm_runner)
```

## Submission model

`slurm_runner` submits non-blocking (`sbatch --parsable`), then polls `sacct` for the
job's terminal state from inside the executor's own thread for that call — the same
thread-per-running-call model the local runner already uses
(`docs/executor.md#scheduling`).

### Interrupting a run

Ctrl-C cancels in-flight jobs. Python raises `KeyboardInterrupt` only in the main thread,
while each poll loop runs in a worker thread, so the executor calls `slurm_runner.cancel()`
(the optional runner cancel hook, `docs/executor.md#runner-and-retries`). Every poll loop then
stops at once, `scancel`s its job, and returns; `necroflow run` exits once all have. Under
`necroflow run --runner slurm`, SIGTERM does the same, so a driver killed by `kill` or by its
own SLURM time limit cancels its jobs too. `sbatch` runs in its own session, so a Ctrl-C
landing mid-submission cannot kill it after the job is queued but before its id comes back.

Cancelled calls keep `.rip/state = running`, so the next run reruns them.

Signals that cannot be caught (SIGKILL), or that keep their default action (SIGHUP from a
dropped SSH session, and SIGTERM when calling `run()` from Python), still leave jobs running.
Cancel them yourself: each job is named after its rule, so `squeue -u $USER` lists them and
`scancel -u $USER --name=<rule>` cancels them.

### Sizing `-c`

The thread cap (`-c`, or `resource_caps={"threads": N}`) is not a pool size. Each running
call holds its declared `threads` (default 1) against the cap, so the number of jobs in
flight is roughly the cap divided by per-rule `threads`. The default cap is the submitting
host's CPU count, which allows only that many single-thread jobs at once. Raise it for more:

```bash
necroflow run --runner slurm -c 200 job.toml   # up to 200 threads=1 jobs, or 25 threads=8 jobs
```

Two side effects of a large cap:

- A rule declaring `threads="all"` binds to the whole cap: it requests
  `--cpus-per-task=<cap>` (200 above) and runs alone. Give rules sent to SLURM an explicit
  thread count.
- Before anything runs, necroflow hashes existing outputs on the submitting host with up to
  `-c` threads. After each job, it hashes that job's outputs there with the rule's own
  `threads`.

## What the job runs

`slurm_runner` writes the batch script to `.rip/slurm.sbatch` and submits it. The script runs
the resolved command exactly as the local runner does: `{shellpath} -c '<command>'`, or
`/bin/sh` without `--shellpath`. Shell-specific syntax therefore behaves the same, and outputs
are never recorded under a shell that didn't run them. The script sets no `--chdir`: like a
local run, the job starts in the directory `necroflow run` was started from, so relative paths
in commands resolve the same way, provided that directory is on the shared filesystem too.

## Resources and submission options

`Constraints(threads=...)` maps to `--cpus-per-task`; no other `Constraints` value has a
defined SLURM mapping. Cluster-specific submission options — partition, account, wall-clock
limit, memory, or anything else `sbatch` accepts — are opaque strings, declared separately
so they never enter `RuleCall.resources`, `parse_resource`, fingerprints, or local
resource-cap admission:

```python
from necroflow import NodeType, SlurmOptions, command, output


class Fastq(NodeType):
    filename = "reads.fq"


class Bam(NodeType):
    filename = "aligned.bam"


@command(
    "align {reads} > {bam}",
    threads=8,
    slurm=SlurmOptions(partition="compute", account="labX", time_limit="02:00:00", memory="16G"),
)
def align(reads: Fastq):
    bam = output(Bam)
    return bam
```

`partition`, `account`, `time_limit`, and `memory` map to `--partition`, `--account`,
`--time`, and `--mem`. Any other keyword passes through as `--{key.replace("_", "-")}=value`
(e.g. `SlurmOptions(gres="gpu:1")` → `--gres=gpu:1`), so cluster-specific flags don't need
code changes here.

## Failure and retry mapping

`sacct` terminal states map onto the executor's existing FAILED/INTERRUPTED distinction
(`docs/executor.md#runtime-state-machine`):

| `sacct` State | Result |
|---|---|
| `COMPLETED` | success |
| `FAILED`, `NODE_FAIL`, `OUT_OF_MEMORY`, `BOOT_FAIL`, `DEADLINE`, `PREEMPTED` | `subprocess.CalledProcessError`, positive code — `repeat=N` retries as normal |
| `CANCELLED`, `TIMEOUT` | `subprocess.CalledProcessError`, negative code — `repeat=N` retries it too; if it ends the last attempt, the call is recorded `interrupted`, not `failed` |

The sign of the code only decides the final recorded state; `repeat` retries both. So a rule
with `repeat=3` that hits its time limit is submitted three times, each running to the limit,
and a job you `scancel` yourself is resubmitted while attempts remain.

If `sbatch` rejects the script (bad account, partition, or directive), the runner raises
`subprocess.CalledProcessError` with sbatch's exit code. sbatch's stderr is written to
`.rip/job.log`, which the executor prints after a failure.

A failed `sacct` query says nothing about the job, so polling rides it out. `slurm_runner`
gives up only after `SACCT_FAILURE_LIMIT` consecutive failed polls (60, about five minutes at
the 5-second poll interval). It then cancels the job, since an untracked job would keep
writing into the workdir, and raises `RuntimeError` carrying sacct's last stderr. The call is
recorded `failed` with no exit code, and `repeat` does not resubmit it.

## Materializer rules

Rules with a `materializer` (e.g. `text_file`) have no shell command to hand to `sbatch`: a
materializer is a Python closure. `slurm_runner` runs them in the submitting process via
`RuleCall.run`, exactly as the default runner does.

## Not supported

Rules whose resolved command selects a `Docker` input (`docs/rules.md#docker-execution`).
`slurm_runner` raises `NotImplementedError` for them before any submission. Clusters
typically run containers with Apptainer (formerly Singularity) rather than Docker, and
Apptainer support is work in progress.

## Testing locally

[`giovtorres/slurm-docker-cluster`](https://github.com/giovtorres/slurm-docker-cluster)
brings up a real multi-container SLURM cluster (`slurmctld`, `slurmd`, `munge`, `mysql`) via
`docker compose`, with a shared mount across containers — closer to production SLURM than
stubbing `sbatch`/`sacct`, and reusable in CI.

Only `/data` (the `slurm_jobdir` volume) is actually shared between `slurmctld` and the
worker containers — `/tmp` is not. Run necroflow itself from inside `slurmctld` (it has the
`sbatch`/`sacct`/`scancel` client binaries), with the node store under `/data`, or a job's
outputs land on whichever worker ran it and are invisible to the process that submitted it —
exactly the missing-output failure the shared-filesystem prerequisite above describes.
`tests/test_slurm.py::test_real_slurm_execution` is the opt-in test exercising this; set
`NECROFLOW_SLURM_TEST=1` and, if your shared mount isn't at the container's `tmp_path`
default, `NECROFLOW_SLURM_TEST_DIR=/data`.

[Previous: Scheduler Protocol](schedulers.md) | [README](../README.md) | [Next: Release](release.md)
