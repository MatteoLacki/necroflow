# Executor

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Scheduler Protocol](schedulers.md)

Necroflow plans, caches, schedules, executes, reports, and cleans one canonical `RuleCall` at a time. A `Node` identifies one typed output and one graph edge; it is not an execution unit.

## Lifecycle

After a workflow returns:

```python
P.finish()
dag.require(P.sinks())  # or explicitly selected output Nodes
report = dag.run()
```

`P.finish()` freezes construction. `dag.require(...)` selects visible endpoints. `run()` acquires `nodes/.rip/necroflow.lock`, builds the required RuleCall closure, classifies available calls, schedules work, optionally copies CLI results through `on_complete`, then releases the lock.

Equivalent invocations intern eagerly into one canonical RuleCall. RuleCall insertion order is therefore stable and supplies FIFO order.

Progress lines (`start`, `done`, `failed`, the summary) go to the `necroflow` logger. The CLI gives it a terminal handler; the library never configures logging. A script calling `dag.run()` sees them after `logging.basicConfig(level=logging.INFO)` or `necroflow.logger.setup()`.

## Atomic RuleCalls and co-outputs

Requesting any output Node activates its complete RuleCall. Every declared output:

- shares one identity, workdir, state, command, scheduler submission, and report event;
- is validated after execution;
- is hashed and retained in the node store;
- is deleted only with the complete call workdir.

Only explicitly requested Nodes are copied into `results/`. This separates atomic cache ownership from user-visible result selection.

If any declared output is missing, the whole call is `MISSING`. Exit zero with any declared output absent is failure.

## Required closure and orphans

Planning starts from `dag.required_nodes`, converts each Node to its RuleCall, then follows `parent_calls`. Active calls retain canonical DAG registration order.

Compiled calls outside this closure are inactive. Inactive calls with an existing workdir are listed in `plan.orphans`; absent inactive calls are ignored. Orphans never run. With `autoclean=True`, their whole workdirs may be removed.

Identity uses `call.relative_path` and `node.relative_path`, never `id()`.

## Cache classification

Classification gives a call one `Verdict` (`necroflow.planning.Verdict`):

```text
MISSING, STALE, UP_TO_DATE, UNDECIDED
```

Verdicts and their reasons belong to the invocation's `ExecutionPlan` (`plan.verdicts`, `plan.reasons`), not to the RuleCall, so `explain` and a run over the same DAG never share state. `UNDECIDED` arises only when untrusted parents may not be hashed, i.e. in `explain`.

Planning is one pass in registration order, which is topological because a call is interned after the calls producing its inputs. By default, a call that will run also marks every active descendant for execution: `MISSING` if an output is absent, otherwise `STALE`, with reason `parent_will_run`. Execution still waits for every parent to succeed.

With `reuse_unchanged=True` (`--reuse-unchanged` on the CLI), descendants of a parent that will run instead remain unclassified. After each parent finishes, the executor classifies those children against final parent bytes, allowing cache reuse when the bytes match.

An available call is:

- `MISSING` when any declared output is absent;
- `STALE` when forced, compromised, invalidator-changed, a parent will rerun under the default policy, or parent-content evidence requires replay;
- otherwise `UP_TO_DATE`.

A `.rip/state` file containing anything except `up_to_date` is compromised. Missing state remains trusted for older complete cache entries.

### Consumed parent hashes

On success, `dependencies.toml` records every parent Node in declaration order:

```toml
node_key = "producer/<provenance_hash>/out.txt"
consumed_hash = "blake3:<hash consumed by this call>"
```

When classifying a consumer, Necroflow compares `consumed_hash` with each parent output's current hash from the same hasher. Missing, malformed, other-hasher, reordered, or mismatched parent metadata makes the consumer stale.

Rebuilding a parent reruns its active descendants by default, regardless of byte equality. Only with `reuse_unchanged=True` may identical rebuilt bytes leave a consumer cached. Different bytes replay the consumer under either policy. This option remains outside fingerprints.

Classification never writes dependency records; only successful call completion does. The one exception to read-only classification is the hash cache below: holders of the node-store lock (real runs and dry runs) may refresh a parent's stored hash.

### Hash fast path and mtime

For each output, `.rip/{filename}.hash` stores the last successful tagged hash. Within one run, computed current hashes are memoized by Node key. A stored hash from another hasher is ignored and recomputed.

Necroflow trusts the stored hash only when output mtime is not newer than the hash-file mtime. A newer output is hashed from current bytes. Thus mtime is hash invalidation for ordinary external edits, not cache identity.

A parent that never reruns never refreshes its own stored hash, so without write-back an externally edited parent, a changed `@symlink` target, or a hasher switch would make every later run read the parent in full again. Lock holders therefore write a fresh digest back to `.rip/{filename}.hash`, unless the output's mtime changed while it was being hashed; the parent is then read once, not on every run. `explain` holds no lock, so it never hashes such a parent and never writes; it reports the consumer as undecided instead.

Files hash their bytes. Directories hash sorted non-`.rip` relative paths plus file bytes.

External edits that preserve or backdate output mtime are unsupported and can evade detection.

### Invalidators

A NodeType may define `invalidator(node) -> str`. Its token lives at `.rip/{filename}.invalidation`. Missing or changed token stales the owning call. Invalidators are checked when that call becomes classifiable.

## Forced invalidation

`forced_stale_call_keys` contains canonical RuleCall paths. A forced active call becomes stale. It does not add new requirements; inactive calls stay inactive.

CLI `--invalidate LABEL` and `--reap NAME` resolve selected Nodes to owning call keys.

## Runtime state machine

Each active call has one `Phase` (`necroflow.executor.Phase`), owned by the run's `Execution` object:

```text
WAITING --all parents DONE--> classify
  verdict UP_TO_DATE        --> DONE (cached)
  verdict MISSING / STALE   --> READY -> RUNNING -> DONE
                                                 |-> FAILED
                                                 |-> INTERRUPTED
WAITING --an ancestor FAILED or INTERRUPTED--> BLOCKED
```

Entering or leaving `RUNNING` writes `.rip/state` (`running`, then `up_to_date`, `failed`, or `interrupted`). Cached and blocked calls write no state. A blocked call is never submitted, gets no report event, and is counted as blocked in the run summary.

## Scheduling

Protocol:

```python
def scheduler(
    ready: list[RuleCall],
    remaining: list[RuleCall],
    available_resources: dict[str, int],
) -> list[RuleCall]:
    ...
```

`ready` contains runnable calls. `remaining` contains every waiting or ready call: not done and not running. `available_resources` contains remaining capacity for capped resources. The executor consults the scheduler whenever at least one call is ready and no failure has stopped the run.

Returned value must be a list containing only ready calls, without duplicate call keys. Invalid selections fail before submission. Returning no calls while nothing runs ends scheduling; the executor logs a warning naming the calls left ready.

`fifo_scheduler` is sole built-in and default. It preserves RuleCall registration order. CLI accepts `--scheduler fifo` or `--scheduler path.py:callable`.

See [Scheduler Protocol](schedulers.md).

## Resources and parallelism

Default cap is `threads = os.cpu_count()` (`rules.default_thread_cap()`). `resource_caps` overrides or adds caps. Uncapped resources do not limit admission. `hasher` selects the output content hasher (default BLAKE3); see `docs/caching.md`.

A rule declaring `threads="all"` resolves against the run's thread cap: `run()` and
`cli explain` call `DAG.bind_thread_cap(cap)` before scheduling or resolving commands,
so `RuleCall.resources["threads"]` and the realized command reflect the actual cap for
that run. `Rule.resources_for(cap)` is the one place that resolves `"all"`; an unbound
call, and `Rule.resources`, use the default cap. See [Rules](rules.md#all-available-threads).

Scheduler order is priority, not guaranteed start order under resource pressure. Executor admits selected calls whose capped resource totals fit (`ResourcePool`). When nothing runs, one oversized call may run alone to prevent deadlock.

Resources remain reserved across retries.

## Runner and retries

`rule_call_runner(call, log_path)` executes one complete call. The default is `necroflow.runners.run_call`, which invokes a Python materializer or realizes and runs one shell command. Output and error go to `<workdir>/.rip/job.log`.

`runners.attempt` calls the runner in a worker thread and returns an `Outcome`: start and finish timestamps plus the error, if any. Exit code and interruption (a negative exit code) are read from that `Outcome`, so the coordinator never inspects exception types.

When the realized command selects a `Docker` input (see [Docker execution](rules.md#docker-execution)), `run_call` launches `docker run` with argv from `necroflow.containers.docker_argv` instead of a host shell. Exit codes, retries, logs, and missing-output checks are unchanged. Cached calls launch nothing and pull nothing. A custom `rule_call_runner` owns whatever it replaces, including container launch.

`repeat=N` permits at most N command attempts within one scheduler submission. Only `subprocess.CalledProcessError` retries. Materializer errors, missing outputs, runner bugs, and metadata errors do not.

## Successful completion

After runner success, executor:

1. validates every declared output;
2. writes `dependencies.toml`, output hashes, and invalidator tokens;
3. writes `graph.tgf`;
4. records one `RuleCallExecution` and `run.toml`;
5. writes state `up_to_date`;
6. optionally cleans eligible parent calls;
7. classifies the call's children.

A missing output or a metadata error in steps 1–5 fails the call like a failed command.

Shared metadata:

```text
<workdir>/.rip/
  dependencies.toml
  <filename>.hash
  <filename>.invalidation
  graph.tgf
  job.log
  run.toml
  state
```

`run.toml` stores call-level timestamps, `duration_seconds`, exit code, and total non-`.rip` workdir size.
Timestamps are taken in the worker thread around the command attempts, so a call's duration never includes
coordinator work such as hashing another call's outputs.
`graph.tgf` stores every output of the call plus the ancestor Nodes it consumed, in DAG registration order:
numbered Nodes followed by parent-to-child edge pairs in
[Trivial Graph Format](https://docs.gephi.org/desktop/User_Manual/Import/Trivial_Graph_Format/).

## Execution report

`run() -> dict[str, RuleCallExecution]` is keyed by `call.relative_path.as_posix()`. One event represents one call and contains all output Node keys and paths.

Report includes:

- every active cache hit;
- every successful execution attempt;
- every failed or interrupted attempt.

Cached events have `cached=True` and no execution duration. Dependency-blocked calls have no event. `DAG.run()` stores a normally returned report as `dag.last_execution_report`.

With `keep_going=True`, independent branches continue. The final `ExecutionFailed` (an `ExceptionGroup` subclass in `necroflow.executor`) carries `execution_report`. `on_complete(report)` runs before that exception, allowing CLI result and summary materialization.

Without `keep_going`, the first failure stops new submissions. Calls already running finish and are recorded normally, so their work stays cached. `on_complete` is skipped and the first failure is re-raised.

## Dry run and explain

`dry_run=True` classifies currently available calls under the lock. It executes nothing, deletes nothing, and skips `on_complete`. Its only write is refreshing stored parent hashes it had to recompute, as a real run would.

By default, descendants of calls that would run are also reported as `will_run = true`, with reason `parent_will_run`. With `--reuse-unchanged`, those descendants remain unknown until real parent bytes exist (`state = null`, `will_run = null`). Explain nests output Nodes under each RuleCall. The MCP explain tool accepts the equivalent `reuse_unchanged=True` argument.

`explain` (CLI and MCP) takes no lock and may run beside a real run, so it never reads whole outputs and never writes. A consumer whose parent's stored hash is not trusted gets `state = "undecided"`, `will_run = null`, and reason `parent_hash_unchecked`; its descendants get `state = null` and reason `parent_undecided`. A real run or dry run settles them.

## Autoclean

Cleanup is RuleCall-atomic:

- requested calls and active final calls are protected;
- orphan cleanup removes whole inactive workdirs;
- after a consumer executes successfully or is reused from cache, an intermediate parent may be removed once all active consumers are up to date.

No individual sibling output cleanup exists.

On a later run, rebuilding an intermediate removed by autoclean reruns its active
descendants under the default policy. With `reuse_unchanged=True`, a cached
consumer can survive that rebuild, and the intermediate is still cleaned after
the consumer settles.

## CLI results

CLI result paths remain Node-level selections. After successful execution, only requested Nodes are copied from canonical workdirs into `results/`. `manifest.toml` records visible path, canonical Node origin, and content hash.

`execution.toml` uses RuleCall report entries, so duration is never attached to an individual Node and co-output runtime is never double-counted.
The CLI `--reporter path.py:function` callback receives each job label and this
summary as a plain dict after the file is written.
Its top-level timings separate summed work from elapsed time:

- `cumulative_duration_seconds` sums the job's command durations; parallel calls add up.
- `command_wall_seconds` is the time at least one of the job's commands was running; overlaps count once.
- `[invocation]` holds whole-run figures, repeated in every job's file: `wall_seconds` from `dag.run()` start until
  results are copied, and `hash_seconds` spent hashing during planning, call completion, and result manifests.

These overlap and need not sum to `wall_seconds`; the rest is scheduling, metadata, and copying. Hashing runs one
file at a time on the coordinator, so summed hash time equals hashing wall time.

## Worked execution example

Consider five RuleCalls registered in this order:

```text
source
├── analyze → (data, log) ──→ report  [requested]
├── checksum                         [requested]
└── unused                           [not requested]

registration order: source, analyze, checksum, report, unused
```

The caller requests two output Nodes:

```python
dag.require([P.report, P.checksum])
dag.run()
```

### Planning

The planner follows owning RuleCalls and their parents from the requested Nodes:

```text
active:   source, analyze, checksum, report
inactive: unused
```

If `unused` has an existing workdir, it enters `plan.orphans`; otherwise it is
ignored. `analyze` remains one atomic call containing both `data` and `log`, even
though only `data` feeds `report`.

Assume every active output is initially missing. Classification begins as:

```text
source    → MISSING
analyze   → unknown; parent will run
checksum  → unknown; parent will run
report    → unknown; parent will run
```

Classification is lazy: consumers wait for final parent bytes before deciding
whether their cached result remains valid.

### First scheduler pass

The executor promotes `source` from `MISSING` to `READY`. The scheduler receives:

```python
ready = [source]
remaining = [source, analyze, checksum, report]
```

FIFO returns `[source]`. The executor checks resources, writes
`source/.rip/state = "running"`, and submits
`rule_call_runner(source, log_path)`.

After runner success, the executor validates every declared output, writes
dependency metadata, hashes and invalidator tokens, writes the ancestor graph
and successful state, creates one `RuleCallExecution`, and writes `run.toml`.

### Newly available children

Once `source` is `DONE`, its children become classifiable:

```text
analyze   → MISSING → READY
checksum  → MISSING → READY
report    → unknown; analyze must settle
```

The next FIFO input is:

```python
ready = [analyze, checksum]
```

The executor may submit both concurrently when resources permit. A custom
scheduler may reverse them, but cannot select `report` before it is ready.

`analyze` executes once and must produce both `data` and `log`. Both outputs are
validated and hashed together. Its metadata records the exact source bytes it
consumed:

```toml
[[parents]]
node_key = "source/<provenance_hash>/raw.txt"
consumed_hash = "blake3:<64 lowercase hexadecimal characters>"
```

There is one call-level duration and one `RuleCallExecution`, containing both
output Node keys.

After `analyze` settles, `report` becomes classifiable. Its output is missing,
so it moves through `READY → RUNNING → DONE`.

For CLI runs, only `report` and `checksum` are copied into `results/`.
`analyze.data` and `analyze.log` remain together in the node store by default.
With `autoclean=True`, the intermediate `analyze` workdir may be removed after
`report` succeeds, and an existing `unused` orphan workdir is removed. Atomic
co-outputs are never cleaned separately.

### Fully cached second run

When all required workdirs were retained, the next run classifies parent-first:

```text
source   → UP_TO_DATE
analyze  → source current SHA == consumed SHA → UP_TO_DATE
checksum → source current SHA == consumed SHA → UP_TO_DATE
report   → data current SHA == consumed SHA   → UP_TO_DATE
```

No runner is called. The report contains four cached `RuleCallExecution`
entries without new durations. Current hashes use the stored hash while output
mtime proves it safe; a newer output is rehashed. Hashes are memoized for the
invocation.

### Rebuilt parent

If `source` is forced stale, all active descendants rerun by default, even if
every output is byte-identical:

```text
source   → rerun (forced)
analyze  → rerun (parent will run)
checksum → rerun (parent will run)
report   → rerun (parent will run)
```

With `reuse_unchanged=True`, identical rebuilt source bytes instead preserve
the `analyze` and `checksum` caches, and `report` remains cached too. If source
bytes change, `analyze` and `checksum` become stale; `report` then reruns only
when the resulting `data` bytes disagree with its consumed hash or its other
cache checks require replay.

If `analyze` fails, `report` becomes `BLOCKED` without submission and receives
no execution event. With `keep_going=True`, independent `checksum` may still
finish; otherwise the first failure stops new submissions, and calls already
running finish and are recorded.

## Precise boundaries

- Node = typed output, dependency edge, result selector.
- RuleCall = cache, state, scheduling, execution, report, cleanup unit.
- mtime = stored-hash invalidation fast path, not freshness policy.
- consumed content hash = immutable dependency freshness policy.
- scheduling = RuleCall FIFO by default.
- visible CLI results = requested Node copies.

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Scheduler Protocol](schedulers.md)
