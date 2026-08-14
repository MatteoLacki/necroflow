# Executor

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Scheduler Protocol](schedulers.md)

Necroflow plans, caches, schedules, executes, reports, and cleans one canonical `RuleCall` at a time. A `Node` identifies one typed output and one graph edge; it is not an execution unit.

## Lifecycle

After a factory returns:

```python
P.finish()
dag.require(P.sinks())  # or explicitly selected output Nodes
report = dag.run()
```

`P.finish()` freezes construction. `dag.require(...)` selects visible endpoints. `run()` acquires `nodes/.rip/necroflow.lock`, builds the required RuleCall closure, classifies available calls, schedules work, optionally copies CLI results through `on_complete`, then releases the lock.

Equivalent invocations intern eagerly into one canonical RuleCall. RuleCall insertion order is therefore stable and supplies FIFO order.

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

Compiled calls outside this closure are inactive. Existing inactive workdirs are `ORPHAN`; absent inactive calls remain unclassified. Orphans run never. With `autoclean=True`, their whole non-mutable workdirs may be removed.

Identity uses `call.relative_path` and `node.relative_path`, never `id()`.

## Cache classification

State belongs to RuleCall:

```text
MISSING, STALE, UP_TO_DATE, ORPHAN,
READY, RUNNING, FAILED, INTERRUPTED
```

Classification is lazy. A call is classified only after all parent calls are `UP_TO_DATE`. At initial planning time, a descendant whose parent will run has `state = None` and reason `parent_will_run`. After each parent settles, the executor classifies newly available children against final parent bytes.

An available call is:

- `MISSING` when any declared output is absent;
- `STALE` when forced, compromised, invalidator-changed, or parent-content evidence requires replay;
- otherwise `UP_TO_DATE`.

A `.rip/state` file containing anything except `up_to_date` is compromised. Missing state remains trusted for older complete cache entries.

### Consumed parent hashes

On success, `dependencies.toml` records every parent Node in declaration order. Immutable parents include:

```toml
node_key = "producer/<provenance_hash>/out.txt"
mutable = false
consumed_sha256 = "<hash consumed by this call>"
```

When classifying a consumer, Necroflow compares `consumed_sha256` with each immutable parent output current SHA-256. Missing, malformed, reordered, or mismatched parent metadata makes the consumer stale.

This makes parent process history irrelevant. A parent rebuilt during this invocation but producing identical bytes leaves an immutable consumer cached. Different bytes replay it.

Classification reads metadata but never writes it. Only successful call completion writes dependency records and hashes.

### Hash fast path and mtime

For each output, `.rip/{filename}.hash` stores the last successful SHA-256. Within one run, computed current hashes are memoized by Node key.

Necroflow trusts the stored hash only when output mtime is not newer than the hash-file mtime. A newer output is hashed from current bytes. Thus mtime is hash invalidation for ordinary external edits, not cache identity.

Files hash their bytes. Directories hash sorted non-`.rip` relative paths plus file bytes.

External edits that preserve or backdate output mtime are unsupported and can evade detection.

### Mutable Rules

Mutability is execution policy on Rule, not a NodeType annotation:

```python
@command("update-db {db}", mutable=True)
def update_db(...):
    ...
```

A mutable Rule must declare exactly one output. External byte changes to that output do not stale consumers. But if the mutable parent executes during the current run, all consumers replay, regardless of resulting hash. Missing, forced, compromised, invalidator-changed, and failed mutable parents retain normal propagation.

This deliberately avoids supporting multiple mutable siblings or nondeterministic immutable outputs.

### Invalidators

A NodeType may define `invalidator(node) -> str`. Its token lives at `.rip/{filename}.invalidation`. Missing or changed token stales the owning call. Invalidators are checked when that call becomes classifiable.

## Forced invalidation

`forced_stale_call_keys` contains canonical RuleCall paths. A forced active call becomes stale. It does not add new requirements; inactive calls stay inactive.

CLI `--invalidate LABEL` and `--reap NAME` resolve selected Nodes to owning call keys.

## Runtime state machine

```text
MISSING / STALE
  | all parents UP_TO_DATE
  v
READY -> RUNNING -> UP_TO_DATE
                 |-> FAILED
                 |-> INTERRUPTED
```

A call blocked by failed or interrupted parents becomes `FAILED` without submission or report event.

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

`ready` contains runnable calls. `remaining` contains unclassified, missing, stale, ready, and running calls. `available_resources` contains remaining capacity for capped resources.

Returned value must be a list containing only ready calls, without duplicate call keys. Invalid selections fail before submission. Returning no calls while nothing runs ends scheduling.

`fifo_scheduler` is sole built-in and default. It preserves RuleCall registration order. CLI accepts `--scheduler fifo` or `--scheduler path.py:callable`.

See [Scheduler Protocol](schedulers.md).

## Resources and parallelism

Default cap is `threads = os.cpu_count()`. `resource_caps` overrides or adds caps. Uncapped resources do not limit admission.

Scheduler order is priority, not guaranteed start order under resource pressure. Executor admits selected calls whose capped resource totals fit. When nothing runs, one oversized call may run alone to prevent deadlock.

Resources remain reserved across retries.

## Runner and retries

`rule_call_runner(call, log_path)` executes one complete call. Default runner invokes a Python materializer or realizes and runs one shell command. Output and error go to `<workdir>/.rip/job.log`.

`repeat=N` permits at most N command attempts within one scheduler submission. Only `subprocess.CalledProcessError` retries. Materializer errors, missing outputs, runner bugs, and metadata errors do not.

## Successful completion

After runner success, executor:

1. validates every declared output;
2. writes `dependencies.toml`, output hashes, and invalidator tokens;
3. writes `graph.tgf`;
4. writes state `up_to_date`;
5. records one `RuleCallExecution` and `run.toml`;
6. optionally cleans eligible parent calls.

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
`graph.tgf` stores numbered Nodes followed by parent-to-child edge pairs in
[Trivial Graph Format](https://docs.gephi.org/desktop/User_Manual/Import/Trivial_Graph_Format/).

## Execution report

`run() -> dict[str, RuleCallExecution]` is keyed by `call.relative_path.as_posix()`. One event represents one call and contains all output Node keys and paths.

Report includes:

- every active cache hit;
- every successful execution attempt;
- every failed or interrupted attempt.

Cached events have `cached=True` and no execution duration. Dependency-blocked calls have no event. `DAG.run()` stores a normally returned report as `dag.last_execution_report`.

With `keep_going=True`, independent branches continue. Final `ExceptionGroup` carries `execution_report`. `on_complete(report)` runs before that exception, allowing CLI result and summary materialization.

## Dry run and explain

`dry_run=True` classifies currently available calls under the lock. It executes nothing, deletes nothing, writes nothing, and skips `on_complete`.

Descendants of calls that would run remain unknown until real parent bytes exist. CLI `explain` reports these calls with `state = null`, `will_run = null`, and reason `parent_will_run`. Explain nests output Nodes under each RuleCall.

Mutable external content changes may appear as advisory `mutable_parent_content_ignored`; they do not alter state.

## Autoclean

Cleanup is RuleCall-atomic:

- mutable workdirs are never deleted;
- requested calls and active final calls are protected;
- orphan cleanup removes whole inactive workdirs;
- after successful execution, an intermediate parent may be removed once all active consumers are up to date.

No individual sibling output cleanup exists.

## CLI results

CLI result paths remain Node-level selections. After successful execution, only requested Nodes are copied from canonical workdirs into `results/`. `manifest.toml` records visible path, canonical Node origin, and content hash.

`execution.toml` uses RuleCall report entries, so duration is never attached to an individual Node and co-output runtime is never double-counted.

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

Once `source` is `UP_TO_DATE`, its children become classifiable:

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
mutable = false
consumed_sha256 = "<64 lowercase hexadecimal characters>"
```

There is one call-level duration and one `RuleCallExecution`, containing both
output Node keys.

After `analyze` settles, `report` becomes classifiable. Its output is missing,
so it moves through `MISSING → READY → RUNNING → UP_TO_DATE`.

For CLI runs, only `report` and `checksum` are copied into `results/`.
`analyze.data` and `analyze.log` remain together in the node store by default.
With `autoclean=True`, the intermediate `analyze` workdir may be removed after
`report` succeeds, and an existing `unused` orphan workdir is removed. Atomic
co-outputs are never cleaned separately.

### Fully cached second run

On the next run, classification proceeds parent-first:

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

If `source` is forced stale and rebuilds identical bytes, child classification
happens after source completion:

```text
analyze consumed SHA == rebuilt source SHA  → cached
checksum consumed SHA == rebuilt source SHA → cached
report                                      → cached
```

Process history alone does not invalidate immutable consumers. If source bytes
change, `analyze` and `checksum` become stale. `report` waits for `analyze`, then
reruns only when the resulting `data` bytes disagree with its consumed hash.

For a mutable source, an external byte edit without source execution is ignored.
Executing the mutable source during this run makes its consumers stale.

If `analyze` fails, `report` becomes dependency-failed without submission and
receives no execution event. With `keep_going=True`, independent `checksum` may
still finish; otherwise the first failure aborts the run.

## Precise boundaries

- Node = typed output, dependency edge, result selector.
- RuleCall = cache, state, scheduling, execution, report, cleanup unit.
- mtime = stored-hash invalidation fast path, not freshness policy.
- consumed SHA-256 = immutable dependency freshness policy.
- scheduling = RuleCall FIFO by default.
- visible CLI results = requested Node copies.

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Scheduler Protocol](schedulers.md)
