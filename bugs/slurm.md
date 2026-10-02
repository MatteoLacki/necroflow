# `slurm_runner`: outstanding issues

**Status:** open, recorded 2026-10-02 on `dev/slurm` after a code review of the SLURM runner.
Fixed before this report (not repeated here): materializer rules now run locally, the batch
script honours `shellpath`, `--chdir` is gone, sbatch stderr reaches `.rip/job.log`,
transient `sacct` failures are tolerated, and Ctrl-C/SIGTERM cancel in-flight jobs.

**Location:** `src/necroflow/slurm.py` unless stated otherwise. Line numbers are as of the
commit that adds this file.

| # | Issue | Severity |
|---|---|---|
| 1 | Requeued jobs are treated as failed and resubmitted | high |
| 2 | Outputs are checked before the shared filesystem shows them | high |
| 3 | Multi-cluster job ids lose their `;cluster` suffix | medium |
| 4 | Unrecognised final states make the poll loop spin forever | medium |
| 5 | No runner-wide `SlurmOptions`; built-in rules cannot take `slurm=` | medium |
| 6 | `#SBATCH` values are unescaped and runner-owned directives can be overridden | medium |
| 7 | Fail-fast waits for in-flight calls, then discards their results | medium |
| 8 | One `sacct` process per job every 5 s | low |
| 9 | Clusters without accounting cannot use the runner | low |
| 10 | Gaps left by the interrupt fix | low |
| 11 | `repeat` and `threads="all"` under SLURM (decision pending) | design |

---

## 1. Requeued jobs are treated as failed and resubmitted

**Severity:** high. Two jobs can write the same workdir.

**Location:** `_FAILURE_STATES` (lines 60-67, includes `NODE_FAIL` and `PREEMPTED`),
consumed at line 156.

SLURM requeues batch jobs under the **same job id** after a node failure (`JobRequeue=1`
is the slurm.conf default) or a preemption with `PreemptMode=REQUEUE`. Until the new run
shows up, `sacct` can report the old run as `NODE_FAIL` or `PREEMPTED`. The runner treats
both as final and raises a positive `CalledProcessError`:

- with `repeat > 1`, `_run_with_retries` clears outputs and submits a second job while
  SLURM's requeued job is pending or already running;
- with `repeat = 1`, the call is marked `failed`, and the requeued job later finishes and
  writes outputs behind necroflow's back.

**Proposed fix:** treat `NODE_FAIL`, `PREEMPTED`, `REQUEUED`, `REQUEUE_HOLD` and
`REQUEUE_FED` as "maybe coming back". On seeing one, ask `squeue -h -j <id> -o %T`, which
lists only jobs slurmctld still considers live. If it is listed, keep polling the same id;
if not, report a final failure. necroflow never resubmits; SLURM's requeue is the retry.

## 2. Outputs are checked before the shared filesystem shows them

**Severity:** high. Successful jobs are reported as failed.

**Location:** `slurm_runner` returns at line 215 as soon as `sacct` says `COMPLETED`; the
executor's `_complete_call` then checks `path.exists()` immediately.

On NFS the submitting host's attribute and negative-dentry caches (`acregmin`/`acdirmin`,
often several seconds) can hide files a compute node has just written. The call fails with
"command succeeded but output missing" although the files appear moments later.
Snakemake's `--latency-wait` exists for exactly this.

**Proposed fix:** after `COMPLETED`, poll until every declared output exists, up to a cap
(e.g. 60 s, a module constant like `SACCT_FAILURE_LIMIT`). Each attempt lists the parent
directory to force NFS to revalidate its directory cache, as Snakemake does. It returns as
soon as everything is visible, so it costs nothing in the normal case; if outputs are still
missing at the cap, the executor's existing error stands.

## 3. Multi-cluster job ids lose their `;cluster` suffix

**Severity:** medium.

**Location:** `_submit`, line 120: `result.stdout.strip().split(";")[0]`; `sacct` (line 126)
and `scancel` (line 210) take no `-M`.

With `SlurmOptions(clusters="gpu")`, `sbatch --parsable` prints `123;gpu`. The runner keeps
`123`, so `sacct -j 123` and `scancel 123` query the default cluster: either nothing comes
back and the poll loop never ends, or a different job 123 is reported (or cancelled).

**Proposed fix:** keep the cluster part and pass `-M <cluster>` to `sacct` and `scancel`.

## 4. Unrecognised final states make the poll loop spin forever

**Severity:** medium.

**Location:** line 156 only returns for states in
`_SUCCESS_STATES | _CANCELLED_STATES | _FAILURE_STATES`; every other state means "keep
polling".

SLURM has other final states, e.g. `REVOKED` (federation: the job ran on a sibling cluster)
and `SPECIAL_EXIT`. If `sacct` reports one, the runner polls until the run is interrupted.

**Proposed fix:** invert the test. List the non-final states (`PENDING`, `RUNNING`,
`SUSPENDED`, `COMPLETING`, `CONFIGURING`, `RESIZING`, `STOPPED`, plus the requeue states
from issue 1) and treat anything else as a final failure.

## 5. No runner-wide `SlurmOptions`; built-in rules cannot take `slurm=`

**Severity:** medium. Blocks clusters that require an account.

**Location:** `SlurmOptions` exists only per rule (`src/necroflow/rules.py:293`);
`symlink_file` (`_make_symlink_file_rule`, `src/necroflow/rules.py:979`) builds its `Rule`
without one.

Many clusters reject any job without `--account` (billing) or `--partition`. Users must
repeat the options on every rule, and `symlink_file` jobs are always rejected because there
is nowhere to put them.

**Proposed fix:** runner-wide defaults that rule-level options override, e.g. a repeatable
CLI `--slurm-option account=labX` and/or a job TOML section. Workaround today:
`export SBATCH_ACCOUNT=labX SBATCH_PARTITION=compute`, which `sbatch` reads as defaults.

## 6. `#SBATCH` values are unescaped and runner-owned directives can be overridden

**Severity:** medium.

**Location:** `_sbatch_directives`, lines 77-93.

Paths and option values are pasted raw into `#SBATCH --flag=value` lines:

- `%` is a SLURM filename pattern (`%j` becomes the job id), so a node store under e.g.
  `/scratch/run%j` sends the job log somewhere other than `.rip/job.log`;
- a space in a path truncates it;
- a newline in a `SlurmOptions` value injects extra script lines, i.e. arbitrary commands;
- `SlurmOptions(output=...)`, `error=...` or `chdir=...` silently override the runner's own
  directives, so `_logger.job_output` prints the wrong (empty) file.

**Proposed fix:** pass runner-owned flags (`--output`, `--error`, `--job-name`,
`--cpus-per-task`) on the `sbatch` command line instead of in the script. Argv needs no
word-splitting, and command-line options take precedence over `#SBATCH` lines. Escape `%`
as `%%` in paths, and reject newlines and runner-owned keys in `SlurmOptions`.

## 7. Fail-fast waits for in-flight calls, then discards their results

**Severity:** medium under SLURM. This is executor-wide, not SLURM-specific.

**Location:** `src/necroflow/executor.py:510` (`if not keep_going: raise`), inside the
`ThreadPoolExecutor` block whose exit waits for every running future.

Without `--keep-going`, the first failed call re-raises out of the scheduling loop. The
pool's shutdown then waits for every other running call to finish, but the loop no longer
processes their futures: their `.rip/state` stays `running`, so the next run reruns them
even when they succeeded. Locally that is a short wait; under SLURM it can mean hours of
waiting for work that is then thrown away.

**Options:** reuse the `runner.cancel()` hook to stop sibling calls on fail-fast, or keep
waiting but record each sibling's outcome before re-raising. Needs a decision.

## 8. One `sacct` process per job every 5 s

**Severity:** low, but cluster admins throttle or ban this.

**Location:** each in-flight call runs its own `_sacct_state` (line 123) every
`POLL_INTERVAL_SECONDS` (line 48).

200 concurrent jobs means about 40 `sacct` queries per second against slurmdbd. Trivial
rules such as `symlink_file`'s `ln -s` also wait in the batch queue for a slot.

**Proposed fix:** one shared poller per run. It keeps the set of in-flight ids, runs a
single `sacct -X -n -P -j id1,id2,... -o JobIDRaw,State,ExitCode` every ~30 s, and wakes
each waiting runner thread with its result (the same event mechanism the cancel fix added).
Separately, let trivial built-in rules run locally, as materializers already do.

## 9. Clusters without accounting cannot use the runner

**Severity:** low; most HPC centres run accounting.

**Location:** `_sacct_state`, line 123.

`sacct` reads slurmdbd, the optional accounting database. Sites with
`AccountingStorageType=accounting_storage/none` (small lab clusters, test and some cloud
setups) make `sacct` fail permanently, so after `SACCT_FAILURE_LIMIT` polls (~5 min) every
job is cancelled.

**Proposed fix, if needed:** the batch script writes the command's exit code into `.rip/`
on the shared filesystem, and necroflow only needs `squeue` to see that the job has left
the queue.

## 10. Gaps left by the interrupt fix

**Severity:** low.

- **Registration race.** `slurm_runner` registers its stop event at line 204, after
  resolving the command and writing the script. If `cancel()` fires in that window, the
  call submits anyway and `run()` waits for that job to end. The window is milliseconds.
- **Uncaught signals.** SIGKILL cannot be caught. SIGHUP (a dropped SSH session outside
  tmux) and SIGTERM when calling `run()` from Python keep their default action. All three
  leave jobs running.
- **State after cancel.** Cancelled calls keep `.rip/state = running`. That is safe (the next
  run reruns them) but not accurate; `interrupted` would be.

## 11. `repeat` and `threads="all"` under SLURM (decision pending)

**Severity:** design. Current behaviour is documented in `docs/slurm.md`.

- `repeat` retries every `CalledProcessError` (`src/necroflow/executor.py:258`), including
  `CANCELLED` and `TIMEOUT`: a timed-out job runs to its limit `N` times, and a job the user
  `scancel`s is resubmitted.
- `threads="all"` binds to the local `-c` cap (`src/necroflow/rule_call.py:272`) and is
  submitted as `--cpus-per-task=<cap>` (line 83), e.g. 200 CPUs with `-c 200`.

**Proposal:** under SLURM, reject `repeat > 1` (SLURM's requeue becomes the retry, see issue
1) and reject `threads="all"`. Open question: reject inside `slurm_runner` at submission
(trivial, but the run fails partway) or before anything runs, through a second optional
runner hook such as `check(call)` on the same pattern as `cancel()` (recommended).
