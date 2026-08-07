# Executor, Classification, Scheduling, and Cleanup

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Scheduler Internals](schedulers.md)

This document starts where Pipeline construction ends. It covers request selection, cache classification, execution, failure handling, cleanup, reports, and CLI result materialization.

Source of truth is `dag.py`, `nodes.py`, `executor.py`, and the execution handoff in `cli.py`.

## Flow at a glance

```text
factory returns
  -> P.finish()
  -> select requested labels or sinks
  -> dag.require(requested Nodes)
  -> dag.execute()
       -> validate scheduler
       -> lock node store
       -> classify required ancestor closure
       -> apply forced and compromised invalidation
       -> optionally remove orphans
       -> promote and schedule runnable Nodes
       -> execute one representative per rule call
       -> validate outputs and commit metadata
       -> optionally clean completed intermediates
       -> run CLI completion callback
       -> unlock node store
```

`P.finish()` performs no classification or execution. Classification begins inside `execute()` after all Pipelines have contributed requirements to the shared DAG.

## From `P.finish()` to DAG requirements

`P.finish()` marks the root Pipeline and every subpipeline view finished. Later rule calls, label assignments, and view creation fail.

Finishing does not close the DAG. Other root Pipelines may still compile into it, and equivalent rule calls still intern to the same canonical `RuleCall` and Nodes.

The normal Python handoff is:

```python
factory(P, config)
P.finish()
dag.require(P.sinks())
dag.execute()
```

`P.sinks()` returns labeled canonical Nodes that are not parents of another Node recorded by that Pipeline. It requires finished construction.

The CLI uses explicit job `.requests` labels when present. Otherwise it selects every label bound to a sink Node.

Several labels may request the same canonical Node as distinct visible results.

`dag.require(nodes)` validates that each value is a Node from that DAG, then adds its `relative_path` to the requirement set. It does not traverse parents yet.

Requirements accumulate across expanded jobs and root Pipelines sharing the DAG.

## Entering `execute()`

`DAG.execute(**kwargs)` forwards to `necroflow.executor.execute(dag, **kwargs)`. On a normal return, it stores and returns the same report as `dag.last_execution_report`.

Before touching the node store, `execute()` validates the scheduler's three-argument protocol.

Without an explicit scheduler, it creates a fresh incremental connected-component scheduler for this invocation.

Resource caps start with `threads = os.cpu_count() or 1`. Values supplied through `resource_caps` replace or extend those defaults.

The executor acquires an exclusive non-blocking `fcntl.flock` on:

```text
<nodes-dir>/.rip/necroflow.lock
```

Failure to acquire it raises before jobs start.

The lock remains held through classification, execution, `on_complete`, and CLI result copying.

Only one executor per exact node-store root is supported. Overlapping roots, such as `nodes/` and `nodes/sub/`, cannot be detected and are unsupported.

## Required ancestor closure

Classification receives every eagerly interned DAG Node plus explicitly required Nodes. It walks recursively from each requirement through `node.parents`.

Identity during this walk is `node.relative_path`.

The closure contains requested Nodes plus every ancestor needed to produce them.

Co-output siblings are not added merely because they share a `RuleCall`. A sibling joins only when it is required or is an ancestor of a required Node.

Each output Node is classified separately. Co-output grouping matters later, when the executor chooses one representative submission.

## Nodes outside the required closure

Every DAG Node outside the closure receives:

```text
output exists -> ORPHAN
output absent -> state = None
```

`None` excludes a Node from execution and cleanup.

`ORPHAN` also excludes it from execution, but makes the old artifact eligible for `autoclean`.

An orphan is not an unknown file found by scanning the node store. It is a compiled Node whose current requirements do not need its existing output.

## Required-node classification

Required Nodes are topologically sorted, parents first. One pass propagates missing or stale parent state through descendants.

For each required Node:

```text
output path absent
  -> MISSING

output path present
  -> inspect parent states
  -> inspect newer immutable parent content
  -> inspect NodeType invalidator
  -> STALE or UP_TO_DATE
```

A missing path wins immediately. Metadata cannot make an absent required output up to date.

### Parent-state propagation

An existing Node becomes stale when any parent is `MISSING` or `STALE`. This applies to mutable and immutable parents.

Because parents were classified first, stale state propagates transitively during the same pass.

### Parent mtime and content hash

If no parent is missing or stale, each existing immutable parent is checked against the child:

```text
parent mtime <= child mtime
  -> fast-path success

parent mtime > child mtime
  -> read parent/.rip/<parent-filename>.hash
  -> hash current parent content
  -> equal: content unchanged
  -> missing/different hash: child is STALE
```

File hashes are SHA-256 over bytes.

Directory hashes cover every non-`.rip` file in sorted relative-path order, including relative filenames and file bytes.

The stored hash belongs to the parent's last successful materialization. It determines whether a newer artifact changed, not whether its process reran.

A parent that reruns but emits identical bytes does not stale consumers.

Changing bytes while preserving or lowering parent mtime is outside this check. Such a parent passes the mtime fast path.

### Mutable parents

For `NodeType.mutable = True`, classification skips only the newer-mtime/content-hash comparison.

Mutable parents still provide identity, lineage, ordering, and failure propagation.

Missing, stale, forced, compromised, or invalidator-changed mutable parents still stale consumers.

### NodeType invalidators

After parent checks, classification calls the NodeType invalidator when present. It must return a string.

The Node becomes stale when its token file is absent or differs:

```text
<workdir>/.rip/<output-filename>.invalidation
```

An invalidator exception aborts classification. A non-string result raises `TypeError`.

### What classification does not read

Classification does not read `dependencies.toml`.

Recipe, config, shell policy, and parent lineage already determine the content-addressed rule/provenance path.

Changing identity inputs compiles a different path. Its output is normally `MISSING`; an old compiled output outside the closure may become `ORPHAN`.

## Preparing active Nodes

After base classification:

```python
active = [
    node
    for node in dag.nodes
    if node.state is not None and node.state != NodeState.ORPHAN
]
```

The executor records `active_keys` from `relative_path`, then applies these steps in order.

### Forced invalidation

An active `UP_TO_DATE` Node whose key appears in `forced_stale_keys` becomes `STALE`.

Stale state then propagates repeatedly into active `UP_TO_DATE` descendants. Mutable edges do not suppress explicit invalidation.

CLI `--invalidate LABEL` and `--reap NAME` produce these keys.

An inactive invalidated label does not become required and does not run.

### Orphan cleanup

With `autoclean=True` and `dry_run=False`, orphan cleanup happens before jobs.

If no active output shares the orphan's rule-call directory, the whole directory is removed.

If an active sibling shares it, only the orphan output path is removed.

Any mutable output protects its complete rule-call directory. A mutable output path is never removed individually.

### Compromised prior state

Each active `UP_TO_DATE` Node checks the shared rule-call `.rip/state` file:

```text
state file absent                  -> trusted
state file contains up_to_date    -> trusted
state file contains anything else -> compromised
```

`running`, `failed`, `interrupted`, malformed text, and unknown values are compromised.

The Node becomes `STALE`, then stale state propagates to active descendants.

This check occurs after forced invalidation and orphan cleanup.

## Runtime state machine

Initial active states are `MISSING`, `STALE`, or `UP_TO_DATE`. Cache hits remain `UP_TO_DATE` and never enter the worker pool.

Before every scheduling pass:

```text
MISSING / STALE
  |-- any FAILED or INTERRUPTED parent -> FAILED
  |-- all parents UP_TO_DATE           -> READY
  |-- otherwise                        -> wait

READY -> RUNNING -> UP_TO_DATE
                 |-> FAILED
                 |-> INTERRUPTED
```

A dependency-blocked Node is never attempted. It has no report event because it was neither cached nor submitted.

## Scheduling

Schedulers prioritize eligible work. They do not control dependency gates, resource admission, submission, retries, or state transitions.

Protocol:

```python
def scheduler(
    ready: list[Node],
    remaining: list[Node],
    available_resources: dict[str, int],
) -> list[Node]:
    ...
```

`ready` contains Nodes currently in `READY`.

`remaining` contains active Nodes in `MISSING`, `STALE`, `READY`, or `RUNNING`.

`available_resources` reports remaining capacity only for configured caps. Uncapped resource names do not constrain admission.

The return value must be a list containing only currently ready Nodes, with no duplicate `relative_path`.

Invalid selections fail before any selected job is submitted.

Returning an empty list while no job is running makes the executor stop its loop.

Custom schedulers must therefore select work whenever progress is possible.

### Built-in schedulers

`fifo_scheduler` returns ready Nodes in current registration order.

The default scheduler incrementally orders ready Nodes by connected-component size in remaining work. Smaller components come first.

A fresh default scheduler is created for every `execute()` call.

Library callers passing one explicitly should also create a fresh closure per execution:

```python
dag.execute(scheduler=make_connected_component_scheduler())
```

The CLI accepts `--scheduler connected-components`, `--scheduler fifo`, or a local Python callable such as `--scheduler schedulers.py:my_scheduler`.

See [Scheduler Internals](schedulers.md) for the incremental algorithm.

## Resource admission and parallelism

The worker pool allows up to `len(active)` futures. Actual concurrency is gated by declared job resources and configured caps.

Each scheduler selection is checked before submission. Adding its capped resource requirements must stay within available capacity.

When no job is running, a job may run even if its requirement exceeds a cap. This solo fallback prevents an undersized cap from deadlocking execution.

Resources stay reserved while the job's `Future` remains in the running map. They are released after its result is handled.

Rule resources are declared with `@command` keyword arguments:

```python
@command("bwa mem {ref} {fastq} > {bam}", threads=4, ram="8Gi")
def align(fastq: Fastq, ref: str):
    bam = output(Bam)
    return bam

dag.execute(
    resource_caps={
        "threads": 16,
        "ram": parse_resource("64Gi"),
    }
)
```

Values accept SI suffixes (`K`, `M`, `G`, `T`, `P`) and binary suffixes (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`).

Rule constraints are available to command templates. Direct placeholders such as `{threads}` resolve to the declared value; `{threads}` defaults to `1` when omitted.

Use `{constraint:name}` to force constraint lookup when a config input has the same name.

## Co-output submission

Schedulers see output Nodes, not unique `RuleCall` objects. Several active co-outputs may become ready together.

Before submission, the executor inspects active siblings in `node.output_nodes`. If one is already `RUNNING`, another selected sibling is skipped.

The first admitted sibling becomes representative. One future and one command attempt sequence execute for that rule call.

On success, active runnable siblings receive separate report events with shared timing and directory size. They become `UP_TO_DATE` together.

An already cached sibling stays cached in the report, even if another active sibling causes the shared command to run.

Inactive co-output siblings are not promoted, reported, or state-transitioned.

The command may still create them because command arguments expose every declared output path.

Successful completion validates every active co-output, not every inactive declared sibling.

A missing active sibling changes provisional command success into failure.

## Command realization and shell selection

Built-in materializers write through Python. Other rules resolve to one shell command string inside the worker.

Callable commands receive immutable `CommandArgs` with named input paths, config values, output paths, constraints, and the rule-call workdir.

The realized command is cached on the canonical `RuleCall`, so co-outputs and equivalent factory calls share it.

Without `shellpath`, commands use Python's default `subprocess.run(..., shell=True)` behavior.

With `shellpath`, it is passed as the shell executable.

The shell path is fixed during Pipeline compilation. It contributes to provenance when applicable and is stored in `dependencies.toml`.

Literal shell braces must be doubled so command formatting preserves them:

```python
@command("printf '%s\n' {{left,right}} > {out}")
def make_out():
    out = output(Out)
    return out

P = Pipeline(dag, shellpath="/bin/bash")
P.out = make_out(P)
```

The equivalent CLI flag is:

```bash
necroflow --shellpath /bin/bash job.toml
```

Standard output and error go to:

```text
<workdir>/.rip/job.log
```

Each attempt opens the log for writing, so a retry replaces output captured from the previous attempt.

## Retries

`@command(..., repeat=N)` allows at most `N` attempts. The first attempt counts, so default `repeat=1` performs no retry.

Only `subprocess.CalledProcessError` is retried. This covers nonzero exits and negative return codes representing signals.

Materializer errors, runner bugs, missing outputs detected after exit, metadata errors, and other exceptions are not retried.

One scheduler submission and one future own the complete retry sequence. Resources remain reserved across attempts.

`repeat` is execution policy and is excluded from fingerprints.

## Successful completion

A worker returning without error is provisional success. The executor commits success in this order:

1. Verify every active co-output path exists.
2. Create success `ExecutionEvent` values and write `.rip/run.toml`.
3. Write dependency metadata, content hashes, and invalidator tokens.
4. Write `.rip/graph.txt`.
5. Mark active runnable co-outputs `UP_TO_DATE`.
6. Write `.rip/state` as `up_to_date`.
7. Optionally remove completed intermediate parents.

Exit zero with a missing active declared output fails during step 1.

`write_dependencies()` writes shared `dependencies.toml` with identity, accumulated config, declared outputs, parents, shell selection, and realized command.

For each declared output that exists, it writes the output content hash. For an output with an invalidator, it also writes the current token.

`run.toml` records timestamps, duration, exit code, and total rule-call directory size excluding `.rip`.

Side files under `{workdir}` therefore count toward size.

After success, the shared metadata directory may contain:

```text
.rip/
  dependencies.toml
  <output-filename>.hash
  <output-filename>.invalidation
  graph.txt
  job.log
  run.toml
  state
```

Hash and invalidation files are per materialized output when applicable. Other files belong to the shared rule-call directory.

## Failures and interruption

For `subprocess.CalledProcessError`:

```text
return code >= 0 -> FAILED; state file = failed
return code < 0  -> INTERRUPTED; state file = interrupted
```

Every other handled exception marks the representative `FAILED` and writes `failed` to the shared state file.

The executor records one failure event for the attempted representative and prints `job.log`.

With `keep_going=False`, the first handled failure is re-raised. The completion callback does not run, so CLI results and summaries are not refreshed.

The thread-pool context waits for already submitted futures while unwinding.

Results from those futures are not committed after control leaves the completion loop.

With `keep_going=True`, independent branches continue. Waiting descendants become `FAILED` when a direct parent is `FAILED` or `INTERRUPTED`.

After all possible attempts, `on_complete(report)` runs.

The executor then raises an `ExceptionGroup` containing attempted-job errors and attaches the report as `execution_report`.

## Execution reports

`execute()` returns `dict[str, ExecutionEvent]` keyed by `node.relative_path.as_posix()`.

The report contains:

- Every active `UP_TO_DATE` cache hit.
- Every successfully attempted active output.
- The representative Node for each failed or interrupted attempt.

It excludes `ORPHAN`, `None`, and dependency-blocked Nodes.

A blocked Node has no event because it was never cached or attempted.

Cached events have `cached=True` and no execution timestamps or duration. Their current rule-call directory size is still measured.

Co-output success events use separate Node keys but share timing and output-directory size.

`DAG.execute()` stores the report only when `execute()` returns normally.

A keep-going `ExceptionGroup` carries its report on the exception instead.

## Dry runs

`dry_run=True` performs classification, forced invalidation, and compromised-state handling under the lock.

It does not run commands, delete orphans, clean intermediates, call `on_complete`, copy CLI results, or write execution summaries.

The returned report contains active cache-hit events only.

Missing and stale Nodes are logged as would-run work but have no attempted event.

## Intermediate cleanup

`autoclean=True` builds reverse active edges after classification. An active Node with no active children is final and protected.

After a successful job, a parent is removable only when all active consumers of every active co-output sibling are `UP_TO_DATE`.

The whole parent rule-call directory is removed, including declared outputs, side files, logs, hashes, and other `.rip` metadata.

A final active co-output protects its shared directory. Any mutable co-output also protects the whole directory.

Post-job cleanup is triggered only by successful job completion.

A fully cached invocation removes eligible orphans before scheduling, but does not revisit cached intermediates for deletion.

```python
dag.execute(autoclean=True)
```

## Completion callbacks and CLI results

`on_complete(report)` runs after scheduling while the node-store lock remains held. It is skipped for dry runs and fail-fast errors.

The CLI callback copies requested canonical outputs into visible per-job result trees, then writes `execution.toml`.

Requested paths are validated before execution and again before copying.

Result paths derive from the Pipeline label plus declared output filename.

Each copy is staged under a temporary name.

A destination may be replaced only when prior `manifest.toml` identifies it as Necroflow-owned.

Unmanaged destinations are never overwritten. A malformed manifest aborts copying because ownership cannot be established safely.

Linux uses `cp -a --reflink=auto`; macOS uses `cp -a -c`.

Both preserve intentional symlink outputs and fall back to physical copies when cloning is unavailable.

After all requested outputs are staged, old manifest-owned results are cleared.

Staged values replace destinations with `os.replace`, then `manifest.toml` is atomically replaced.

Each manifest entry records visible relative path, canonical origin Node key, and copied content SHA-256.

Missing requested paths are skipped during materialization.

Under normal success, active requested outputs cannot be missing because completion validated them.

Per-job `execution.toml` groups events by shared `RuleCall`, preventing co-output runtime from being counted twice.

With `keep_going=True`, the callback still runs before the final `ExceptionGroup`.

Successful outputs and failure events can therefore appear in the summary.

## Inspecting classification

Use live introspection when cache behavior is surprising:

```bash
necroflow graph --json job.toml
necroflow outputs --json job.toml
necroflow explain job.toml
necroflow doctor job.toml
```

`explain` uses the same preparation path in dry-run mode.

It reports missing outputs, forced invalidation, compromised state, invalidator changes, parent state, parent content changes, and ignored mutable content.

## Precise boundaries

- `P.finish()` freezes Pipeline construction only.
- `dag.require()` records endpoint keys only.
- Classification constructs the ancestor closure.
- Classification is per output Node.
- Scheduling is per Node; submission deduplicates running co-outputs.
- `dependencies.toml` records provenance but is not a classification input.
- Cache-hit Nodes never enter the worker pool.
- Dependency-blocked Nodes never receive report events.
- CLI visible results are copies, not canonical node-store paths.

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Scheduler Internals](schedulers.md)
