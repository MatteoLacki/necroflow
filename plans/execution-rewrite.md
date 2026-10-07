# Execution subsystem rewrite

Status: implemented on `dev/semplifications` (uncommitted), 2026-10-07. All recommended
options in 1.18 were accepted. Stages 1-7 landed together rather than as separate commits.
Date: 2026-10-07. Branch: `dev/semplifications`.

Deviations from the outline below:

- 1.14.6/1.15.6: `RuleCall.write_state(value)` validates against `STORED_STATES`.
- 1.15.2: the plan class keeps its name `ExecutionPlan`; it also gained `requested_call_keys`.
- 1.17.2: the scheduler is consulted only while some call is ready.
- Not done, still open: 1.2.7 (library configures logging), 1.6.6 (three `threads="all"`
  resolutions), 1.7.4 (`graph.tgf` written from `outputs[0]` only), 1.7.5, 1.10.2 (CLI
  reshapes `to_toml_dict()`), 1.12.1 (four ancestor walks), 1.12.3 (`DAG.run` lazy import).
Line references are to `src/necroflow/` at commit `e1b9356`.

1. Execution subsystem rewrite

  1.1 Scope and assumptions

    1.1.1 Scope is the execution path: `executor.py`, `planning.py`, the execution half of
          `rule_call.py`, `DAG.run`/`DAG.dependencies`, `logger.py`, `schedulers.py`, and the
          CLI `run`/`explain`/`graph` adapters that read call state.

    1.1.2 Out of scope: fingerprints, Pipeline/workflow, rule declaration, grid/config, gc,
          containers (except the call site), MCP server (adapts to API changes only).

    1.1.3 Goal is readability and logical structure; behaviour stays the same except where a
          decision in 1.18 explicitly changes it.

    1.1.4 On-disk formats stay byte-compatible (`.rip/state` values, `dependencies.toml`,
          `{filename}.hash`, `{filename}.invalidation`, `run.toml`, `graph.tgf`), so existing
          node stores remain cached after the rewrite.

    1.1.5 The report shape (`dict[str, RuleCallExecution]` keyed by call path) stays.

  1.2 Shortcomings: shape of `run()`

    1.2.1 `run()` is one ~200-line function (`executor.py:326-524`) with six nesting levels.

    1.2.2 The scheduling loop body performs nine jobs inline: re-classify, record cache hits,
          promote ready, build ready/remaining, call the scheduler, admit by resources,
          submit, wait, and handle success or failure.

    1.2.3 None of the loop's steps is named, so the state machine in `docs/executor.md` must
          be reconstructed by reading the loop.

    1.2.4 Run-local state is ten loose variables (`report`, `running`, `running_resources`,
          `errors`, `executed_call_keys`, `n_run`, `n_skipped`, `n_failed`, `n_cleaned`,
          `autoclean_plan`) rather than one object.

    1.2.5 `running: dict = {}` (`executor.py:380`) is untyped and holds `(call, timing)`
          tuples keyed by future.

    1.2.6 Dry run is an early `return` buried after locking and planning
          (`executor.py:369-378`), with the "would run" filter written twice.

    1.2.7 `run()` configures global logging (`executor.py:348`) as a side effect.

  1.3 Shortcomings: state model

    1.3.1 `RuleCallState` (`rule_call.py:38-47`) mixes three concepts: cache verdict
          (MISSING, STALE, UP_TO_DATE, UNDECIDED, ORPHAN), run progress (READY, RUNNING) and
          outcome (FAILED, INTERRUPTED).

    1.3.2 `None` acts as an undeclared tenth state meaning "not classified yet"
          (`executor.py:399`, `executor.py:418`, `planning.py:202`).

    1.3.3 UP_TO_DATE means both "cache hit" and "executed this run".

    1.3.4 A call blocked by a failed parent is marked FAILED (`planning.py:205`) although it
          never ran.

    1.3.5 Run state lives on `RuleCall`, the canonical identity object shared by every
          Pipeline and by `explain`.

    1.3.6 Because state lives on the shared object, `plan_execution` must reset every call's
          state before each plan (`planning.py:235-236`).

    1.3.7 Both `planning.py` and `executor.py` write `call.state`; no single module owns it.

    1.3.8 In-memory and on-disk state are updated by separate paired statements, e.g.
          `call.mark_done("up_to_date"); call.state = RuleCallState.UP_TO_DATE`
          (`executor.py:310-311`).

    1.3.9 The on-disk state is a bare string while the in-memory state is an enum.

    1.3.10 `RuleCallExecution.state` is a third representation, a string
           (`executor.py:33`).

    1.3.11 The set `{MISSING, STALE}` ("will run") is spelled out at `executor.py:288`,
           `executor.py:371`, `executor.py:375`, `planning.py:248` and `cli.py:487`.

    1.3.12 The `unfinished` set is a local definition inside `run()` (`executor.py:387-392`).

  1.4 Shortcomings: planning and classification

    1.4.1 The loop polls: every iteration rescans all active calls via `classify_available`,
          `_record_cached`, `_promote_ready` and two list comprehensions.

    1.4.2 `classify_available` (`planning.py:192-215`) runs a `while changed` fixpoint that is
          unnecessary, because registration order is already topological (the comment at
          `planning.py:247` relies on exactly that).

    1.4.3 Double counting is prevented only implicitly, e.g. `_record_cached` skips keys
          already in the report (`executor.py:149`).

    1.4.4 `classify_call` (`planning.py:80-189`) is 110 lines covering outputs, forced
          invalidation, compromised state, invalidators and lineage in one body.

    1.4.5 `classify_call` returns the state but also writes `call.state` and `plan.reasons` as
          side effects.

    1.4.6 Transitive "parent will run" is detected by comparing a reason tuple to the literal
          `({"kind": "parent_will_run"},)` (`planning.py:254`).

    1.4.7 Orphan detection uses `call in orphans` (`planning.py:236`), i.e. generated
          dataclass equality on a list, instead of `relative_path` keys.

    1.4.8 `ExecutionPlan` (`planning.py:53-77`) is half result (active, orphans, reasons) and
          half mutable hashing context (hasher, memo, thread budget, two policy flags).

    1.4.9 The executor mutates `plan.hash_threads` on every loop iteration
          (`executor.py:404`).

    1.4.10 `DAG.dependencies` (`dag.py:114`) reads `dependencies.toml`, while
           `RuleCall.write_dependencies` writes it; reader and writer live in different
           classes.

  1.5 Shortcomings: output hashing plumbing

    1.5.1 `current_output_hash` (`planning.py:17-50`) takes six parameters, two of them
          policy flags (`may_hash`, `store`).

    1.5.2 `rule_call.py` imports `planning` inside `write_dependencies`
          (`rule_call.py:347`) to dodge an import cycle.

    1.5.3 The hash memo travels as a bare dict (`plan.hash_cache`) into both planning and
          `write_dependencies`.

    1.5.4 The invalidator token check, including its `TypeError` message, is duplicated
          (`planning.py:102-119`, `rule_call.py:421-430`).

  1.6 Shortcomings: `RuleCall`'s execution half

    1.6.1 `RuleCall.run` (`rule_call.py:221-249`) puts process launching (materializer,
          Docker, shell) on the identity object.

    1.6.2 `RuleCall.run` takes `log_path` although the path is always the call's own
          `log_path()`.

    1.6.3 `resolve()` sets `_container_input` as a side effect.

    1.6.4 The `container` property calls `resolve()` only to trigger that side effect
          (`rule_call.py:251-257`).

    1.6.5 `write_dependencies` (`rule_call.py:336-430`) is ~95 lines doing lineage, consumed
          hashes, output metadata, identity, recipe, input values, execution context,
          command record, output hashes and invalidator tokens.

    1.6.6 `threads="all"` is resolved in three places: `Rule.resources` (`rules.py:480`),
          `RuleCall.resources` (`rule_call.py:266-274`) and `RuleCall._constraints`
          (`rule_call.py:201-207`).

    1.6.7 The `bind_thread_cap` loop over every DAG call is duplicated in `run()`
          (`executor.py:352-353`) and CLI explain (`cli.py:456-457`).

  1.7 Shortcomings: running one command

    1.7.1 Retry, timing and the runner call are three layers: `_run_with_retries`,
          `_timed_run_with_retries` and the `_CommandTiming` mutable box
          (`executor.py:257-283`).

    1.7.2 The coordinator derives exit code and interruption by inspecting exception types
          and the sign of `returncode` (`executor.py:469-479`).

    1.7.3 `_complete_call` (`executor.py:294-323`) both validates outputs and commits metadata,
          state, report and run stats, and it mutates the passed-in report as well as
          returning the event.

    1.7.4 `graph.tgf` is written from `outputs[0]` only (`executor.py:308-309`), so sibling
          co-outputs are missing from a multi-output call's graph.

    1.7.5 `write_ancestor_tgf` scans every DAG node per completed call (`tgf.py:102-106`).

  1.8 Shortcomings: resources and scheduling

    1.8.1 Resource accounting is inline in three places: admission (`executor.py:427-431`),
          acquire (`executor.py:442-445`) and release (`executor.py:511-513`).

    1.8.2 The "an oversized call may run alone" rule is hidden in `not running or all(...)`
          (`executor.py:427`).

    1.8.3 `remaining` passed to schedulers includes RUNNING calls (`executor.py:415-419`).

    1.8.4 `CLAUDE.md`, `docs/schedulers.md` and the write-a-scheduler skill say `remaining`
          excludes running calls, while `docs/executor.md` says it includes them.

    1.8.5 A scheduler returning no calls while nothing runs ends the run normally
          (`executor.py:447-448`), leaving READY calls unexecuted without any log line or
          error.

  1.9 Shortcomings: failure handling

    1.9.1 The failure branch is ~40 inline lines inside the completion loop
          (`executor.py:468-510`).

    1.9.2 Without `keep_going`, `raise` inside `with ThreadPoolExecutor` triggers
          `shutdown(wait=True)`, so running siblings finish but their results are discarded.

    1.9.3 Those discarded siblings keep `.rip/state = running`, so the next run treats them as
          compromised and reruns finished work.

    1.9.4 The fail-fast semantics in 1.9.2 are implicit in the `with` block; no code or comment
          states them.

    1.9.5 Without `keep_going`, `on_complete` is skipped, while with `keep_going` it runs
          before the `ExceptionGroup`; the asymmetry is implicit.

    1.9.6 Blocked descendants are discovered lazily by the next `classify_available` pass
          rather than when the parent fails.

    1.9.7 The report is attached to the `ExceptionGroup` via `setattr` (`executor.py:522`).

  1.10 Shortcomings: report, logging, summaries

    1.10.1 `RuleCallExecution.to_toml_dict` (`executor.py:83-109`) and `_write_run_stats`
           (`executor.py:126-142`) duplicate the optional-field and human-size logic.

    1.10.2 The CLI pops fields from `to_toml_dict()` and re-adds them under other names
           (`cli.py:855-867`).

    1.10.3 Durations are re-derived by parsing ISO strings in two places (`executor.py:75-81`,
           `cli.py:873-874`).

    1.10.4 The summary counters duplicate information the report already holds.

    1.10.5 `logger.summary` labels the cleaned count "orphans cleaned" (`logger.py:104`), but
           it also counts intermediate parents removed by autoclean (`executor.py:465`).

    1.10.6 Logger functions name their parameter `node` but receive a `RuleCall`.

    1.10.7 Logger functions guard `node.rule` against `None` (`logger.py:23`, `logger.py:44`),
           which cannot happen.

    1.10.8 `_human_size` ends with an unreachable `return` (`executor.py:123`).

  1.11 Shortcomings: autoclean

    1.11.1 `_AutocleanPlan` carries an `enabled` flag instead of being absent when disabled
           (`executor.py:161-165`).

    1.11.2 `_build_autoclean_plan` reaches the DAG through `active[:1]`
           (`executor.py:177-181`).

    1.11.3 The children map is built only for autoclean, although child lookup is what
           event-driven settling needs.

    1.11.4 `_clean_orphans` receives `autoclean` and `dry_run` only to return 0
           (`executor.py:208-210`).

  1.12 Shortcomings: duplication across modules

    1.12.1 Ancestor walks are written four times: `DAG.required_call_keys` (`dag.py:102`),
           `cli._requested_with_ancestors` (`cli.py:802`), `write_ancestor_tgf`
           (`tgf.py:92`) and `RuleCall._accumulated_config` (`rule_call.py:322`).

    1.12.2 CLI explain recomputes `will_run` from the state enum (`cli.py:486-492`).

    1.12.3 `DAG.run` imports the executor lazily (`dag.py:154-158`) because the executor
           imports `DAG`.

  1.13 Shortcomings: dead code and documentation drift

    1.13.1 `executed_call_keys` is threaded into `classify_call` but never read
           (`planning.py:87`), a leftover from the removed `mutable` flag.

    1.13.2 `stale_reasons = list(reasons)` is a redundant copy (`planning.py:176`).

    1.13.3 `docs/rule-call-lifecycle.md:491` still describes mutable parents.

    1.13.4 The necroflow-style skill still mentions `consumed_sha256` and mutable parents.

  1.14 Target design: module layout

    1.14.1 Keep the module name `executor.py` for the orchestration (see 1.18.6).

    1.14.2 Add `runners.py`: the default runner (materializer, Docker, shell) moved out of
           `RuleCall.run`, plus `attempt()` and `Outcome`.

    1.14.3 Add `report.py`: `RuleCallExecution`, one TOML serializer, `run.toml` writing and
           summary counts derived from the report.

    1.14.4 Add `OutputHashes` to `hashers.py`: hasher, memo, `may_hash` and `store` policy,
           with `current(node, threads)` and `record(node, threads)`.

    1.14.5 `planning.py` keeps closure and classification but returns values instead of
           writing call state.

    1.14.6 `rule_call.py` keeps identity, command realization and the call's own on-disk
           record (state file, metadata read and write).

    1.14.7 `DAG.dependencies` moves to `RuleCall.read_dependencies`, next to its writer.

  1.15 Target design: state model

    1.15.1 This sketch introduces two enums, one per owner:

```python
class Verdict(Enum):      # planning: is the cache good?
    MISSING = "missing"
    STALE = "stale"
    UP_TO_DATE = "up_to_date"
    UNDECIDED = "undecided"

class Phase(Enum):        # executor: where is this call in this run?
    WAITING = "waiting"   # parents not settled
    READY = "ready"
    RUNNING = "running"
    DONE = "done"         # cached or executed
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    BLOCKED = "blocked"   # a parent failed or was interrupted
```

    1.15.2 `Plan.verdicts: dict[Path, Verdict]` and `Plan.reasons` are planning's only
           outputs.

    1.15.3 `Execution.phase: dict[Path, Phase]` is the executor's only run state.

    1.15.4 `RuleCall.state` is removed (see 1.18.1).

    1.15.5 One `Execution._transition(call, phase)` updates the phase and, for executed
           calls, the `.rip/state` file in the same statement.

    1.15.6 `RuleCall.mark_running`/`mark_done` collapse into one `RuleCall.write_state(value)`
           accepting only the four stored values.

    1.15.7 "Will run" becomes `Verdict.will_run` (MISSING or STALE), defined once.

    1.15.8 ORPHAN disappears as a state; orphans are a list on the plan.

  1.16 Target design: planning

    1.16.1 Planning is one forward pass in registration order, which is topological; the
           fixpoint loop goes.

    1.16.2 A call is classified only when every parent call's verdict is UP_TO_DATE.

    1.16.3 Otherwise its reason is `parent_will_run` or `parent_undecided`, computed from a
           set of will-run keys rather than by comparing reason tuples.

    1.16.4 `classify_call` splits into small reason collectors: missing outputs, forced or
           compromised, invalidators, lineage.

    1.16.5 Reasons stay JSON-ready dicts, since `explain` emits them directly.

    1.16.6 Orphans are found by key membership, not list equality.

    1.16.7 The executor reuses the same `classify` function for calls whose parents finish
           mid-run.

  1.17 Target design: the execution loop

    1.17.1 `run()` becomes a short script around one `Execution` object:

```python
def run(dag, *, resource_caps=None, scheduler=None, keep_going=False,
        autoclean=False, dry_run=False, rule_call_runner=None,
        forced_stale_call_keys=None, on_complete=None, hasher=None):
    caps = _resource_caps(resource_caps)
    dag.bind_thread_cap(caps["threads"])
    with _acquire_lock(dag.nodes_dir):
        hashes = OutputHashes(load_hasher(hasher), may_hash=True, store=True)
        plan = plan_execution(dag, hashes, forced=forced_stale_call_keys,
                              threads=caps["threads"])
        if dry_run:
            return _report_dry_run(plan)
        if autoclean:
            _clean_orphans(plan)
        execution = Execution(plan, hashes, scheduler, ResourcePool(caps),
                              runner, keep_going=keep_going, autoclean=autoclean)
        execution.run()
        if on_complete is not None and execution.completes_normally:
            on_complete(execution.report)
    execution.raise_failures()
    return execution.report
```

    1.17.2 `Execution.run()` reads as the state machine:

```python
def run(self) -> None:
    self._settle(self.plan.active)
    with ThreadPoolExecutor(...) as self._pool:
        while self._ready_calls() or self._running:
            if not self._stopping:
                self._submit(self._pick())
            if not self._running:
                break
            for call, outcome in self._wait_for_any():
                self._finish(call, outcome)
```

    1.17.3 `_settle(calls)` takes calls whose parents are all DONE, classifies them, records
           cache hits as DONE and settles their children, and marks the rest READY.

    1.17.4 After a call finishes, only its children are re-examined, using one children map
           built at construction.

    1.17.5 Ready and remaining lists are still built by one ordered scan per iteration, so
           FIFO stays registration order with no extra bookkeeping.

    1.17.6 `_finish` dispatches to `_succeed` or `_fail` on `outcome.ok`.

    1.17.7 `_succeed` validates outputs, commits metadata, transitions to DONE, autocleans
           parents and settles children.

    1.17.8 `_fail` transitions to FAILED or INTERRUPTED, marks all descendants BLOCKED
           immediately and, without `keep_going`, sets `_stopping`.

    1.17.9 Mid-run classification receives the free thread count as an argument instead of a
           mutated plan field.

  1.18 Decisions needed

    1.18.1 State ownership: recommend moving run state off `RuleCall` into `Plan` and
           `Execution` (1.15); the alternative keeps `RuleCall.state` and only splits the
           enum.

    1.18.2 Fail-fast: recommend stopping new submissions, letting running siblings finish and
           recording them normally; the alternative keeps today's discard-and-rerun
           behaviour.

    1.18.3 Blocked calls: recommend a BLOCKED phase counted in the log summary with no report
           event; the alternative adds a `"blocked"` report event, which changes
           `execution.toml`.

    1.18.4 Public API: recommend replacing the exported `RuleCallState` with `Verdict` and
           `Phase` while keeping `run()`'s signature and report unchanged.

    1.18.5 Scheduler `remaining`: recommend excluding RUNNING calls to match three of the four
           documents; the alternative keeps the code and fixes those three documents.

    1.18.6 Module name: recommend keeping `executor.py` to avoid churn across docs and
           imports.

    1.18.7 Scheduler returning nothing while calls are ready: recommend a warning that names
           the unscheduled calls; the alternatives are today's silent stop or an error.

  1.19 Invariants preserved

    1.19.1 RuleCalls stay atomic: one submission produces and validates every co-output.

    1.19.2 The scheduler protocol keeps three arguments and the same return validation.

    1.19.3 Admission semantics stay, including "an oversized call may run alone".

    1.19.4 `repeat=N` still allows at most N attempts and retries only process failures.

    1.19.5 Planning hashes with the full thread cap.

    1.19.6 Mid-run classification hashes with the free threads only.

    1.19.7 Completion hashes with the call's own `threads`.

    1.19.8 Hashing stays on the coordinator thread.

    1.19.9 `explain` never reads whole untrusted outputs and never writes.

    1.19.10 Real and dry runs hold the node-store lock.

    1.19.11 Timestamps are still taken in the worker around command attempts.

  1.20 Staging

    1.20.1 Each stage leaves the full test suite green.

    1.20.2 Each stage updates the docs it touches in the same change, as `CLAUDE.md` requires
           for the execution handoff.

    1.20.3 Commits happen only when explicitly requested.

    1.20.4 Stage 1, cleanup: remove dead code (1.13.1, 1.13.2, 1.10.8), fix the summary
           label (1.10.5) and fix documentation drift (1.13.3, 1.13.4).

    1.20.5 Stage 2, hashing: introduce `OutputHashes`, strip hash fields from
           `ExecutionPlan`, remove the lazy import and share the invalidator check.

    1.20.6 Stage 3, runners: add `runners.py` with `attempt()` and `Outcome`, and move
           `RuleCall.run` there.

    1.20.7 Stage 4, report: add `report.py` with one serializer and derived summary counts,
           and rename logger parameters.

    1.20.8 Stage 5, executor structure: introduce `Execution` and `ResourcePool` while still
           using `RuleCallState`, preserving behaviour exactly.

    1.20.9 Stage 6, state split: introduce `Verdict` and `Phase`, make planning single-pass,
           remove `RuleCall.state`, and adapt the CLI and tests.

    1.20.10 Stage 7, behaviour changes chosen in 1.18.2, 1.18.3, 1.18.5 and 1.18.7, each
            with its regression test.

  1.21 Test plan

    1.21.1 Behavioural tests through `run()` and the CLI stay unchanged through stages 1-5.

    1.21.2 About 20 assertions on `rule_call.state` across seven test files move to plan
           verdicts or report entries in stage 6.

    1.21.3 Add `ResourcePool` tests for fit, oversized-alone admission and release.

    1.21.4 Add a test that single-pass planning settles a three-deep cached chain.

    1.21.5 Add a test that a fail-fast run records a running sibling's success (if 1.18.2 is
           accepted).

    1.21.6 Add a test that descendants of a failed call are BLOCKED and counted (if 1.18.3 is
           accepted).

    1.21.7 Add a test that a scheduler declining all ready calls produces the chosen warning
           or error (1.18.7).

    1.21.8 Manually check on-disk compatibility: run the examples on `main`, switch to the
           branch, rerun, and expect every call cached.

  1.22 Non-goals

    1.22.1 No change to fingerprints, paths or the node-store layout.

    1.22.2 No parallel hashing off the coordinator thread.

    1.22.3 No cluster backends.

    1.22.4 No consolidation of `threads="all"` resolution beyond one shared
           `bind_thread_cap` helper (1.6.6 stays recorded but deferred).
