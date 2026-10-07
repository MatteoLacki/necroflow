"""Local atomic RuleCall execution."""

from __future__ import annotations

import concurrent.futures
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from enum import Enum
import inspect
from pathlib import Path

from necroflow import logger as _logger
from necroflow.dag import DAG
from necroflow.fs import _acquire_lock
from necroflow.hashers import Hasher
from necroflow.planning import ExecutionPlan, Verdict, plan_execution
from necroflow.report import RuleCallExecution, count_events
from necroflow.rule_call import RuleCall
from necroflow.rules import default_thread_cap
from necroflow.runners import Outcome, Runner, attempt, run_call
from necroflow.schedulers import Scheduler, fifo_scheduler
from necroflow.tgf import write_ancestor_tgf


class Phase(Enum):
    """Where one active RuleCall is within this run."""

    WAITING = "waiting"  # some parent call is not done yet
    READY = "ready"
    RUNNING = "running"
    DONE = "done"  # cached, or executed successfully
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    BLOCKED = "blocked"  # an ancestor failed or was interrupted


# `.rip/state` values for the phases of a call that runs.
_STORED_STATE = {
    Phase.RUNNING: "running",
    Phase.DONE: "up_to_date",
    Phase.FAILED: "failed",
    Phase.INTERRUPTED: "interrupted",
}


class ExecutionFailed(ExceptionGroup):
    """Raised by a keep-going run with failures; carries the whole report."""

    def __new__(cls, report: dict[str, RuleCallExecution], errors: list[Exception]):
        group = super().__new__(cls, "necroflow: some RuleCalls failed", errors)
        group.execution_report = report
        return group

    def __init__(self, report: dict[str, RuleCallExecution], errors: list[Exception]):
        super().__init__("necroflow: some RuleCalls failed", errors)


class ResourcePool:
    """Capped resources held by running calls.

    Resources without a cap never limit admission. A call exceeding a cap on
    its own may still start while nothing else runs, so it cannot stall the run.
    """

    def __init__(self, caps: dict[str, int]):
        self.caps = dict(caps)
        self._used = dict.fromkeys(caps, 0)
        self._holders = 0

    def available(self) -> dict[str, int]:
        return {name: cap - self._used[name] for name, cap in self.caps.items()}

    def free_threads(self) -> int:
        """Threads no running call holds, at least one."""
        return max(1, self.available()["threads"])

    def admits(self, call: RuleCall) -> bool:
        if self._holders == 0:
            return True
        return all(
            self._used[name] + amount <= self.caps[name]
            for name, amount in self._capped(call)
        )

    def acquire(self, call: RuleCall) -> None:
        self._holders += 1
        for name, amount in self._capped(call):
            self._used[name] += amount

    def release(self, call: RuleCall) -> None:
        self._holders -= 1
        for name, amount in self._capped(call):
            self._used[name] -= amount

    def _capped(self, call: RuleCall) -> list[tuple[str, int]]:
        return [(name, n) for name, n in call.resources.items() if name in self.caps]


class Execution:
    """One run over a plan's active RuleCalls: settle, schedule, run, record.

    Owns all run-local state: each call's Phase, the resources held by running
    calls, the report, and the failures to raise once the run ends. A call
    whose parents are all DONE is settled: cached calls become DONE at once,
    the rest READY for the scheduler. After a call finishes, only its children
    are settled again.
    """

    def __init__(
        self,
        plan: ExecutionPlan,
        *,
        scheduler: Scheduler,
        pool: ResourcePool,
        runner: Runner,
        keep_going: bool,
        autoclean: bool,
    ):
        self.plan = plan
        self.scheduler = scheduler
        self.pool = pool
        self.runner = runner
        self.keep_going = keep_going
        self.autoclean = autoclean
        self.report: dict[str, RuleCallExecution] = {}
        self.failures: list[Exception] = []
        self.stopped = False  # a failure without keep_going stops submissions
        self.cleaned = 0
        self.phase = {call.relative_path: Phase.WAITING for call in plan.active}
        self.children: dict[Path, list[RuleCall]] = {key: [] for key in self.phase}
        for call in plan.active:
            for parent in call.parent_calls:
                self.children[parent.relative_path].append(call)
        # Autoclean never removes requested calls or calls without consumers.
        self.protected = plan.requested_call_keys | {
            key for key, children in self.children.items() if not children
        }
        self._running: dict[concurrent.futures.Future[Outcome], RuleCall] = {}

    def run(self) -> None:
        try:
            if self.autoclean:
                self._clean_orphans()
            self._settle(self.plan.active)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.plan.active) or 1
            ) as threads:
                while True:
                    ready = self._calls_in(Phase.READY)
                    if ready and not self.stopped:
                        self._submit(threads, ready)
                    if not self._running:
                        break
                    self._finish_some()
            unscheduled = self._calls_in(Phase.READY)
            if unscheduled and not self.stopped:
                _logger.unscheduled(unscheduled)
        finally:
            completed, cached, failed = count_events(self.report)
            blocked = len(self._calls_in(Phase.BLOCKED))
            _logger.summary(completed, cached, failed, self.cleaned, blocked)

    def raise_failures(self) -> None:
        """Re-raise the first failure, or all of them for a keep-going run."""
        if not self.failures:
            return
        if not self.keep_going:
            raise self.failures[0]
        raise ExecutionFailed(self.report, self.failures)

    def _calls_in(self, *phases: Phase) -> list[RuleCall]:
        """Active calls in any of `phases`, in registration (FIFO) order."""
        return [
            call
            for call in self.plan.active
            if self.phase[call.relative_path] in phases
        ]

    def _transition(self, call: RuleCall, phase: Phase) -> None:
        """Move a call to `phase`; entering or leaving RUNNING updates `.rip/state`."""
        key = call.relative_path
        if Phase.RUNNING in (phase, self.phase[key]):
            call.write_state(_STORED_STATE[phase])
        self.phase[key] = phase

    def _settle(self, calls: list[RuleCall]) -> None:
        """Decide waiting calls whose parents are all done: cached or ready."""
        pending = deque(calls)
        while pending:
            call = pending.popleft()
            key = call.relative_path
            if self.phase[key] is not Phase.WAITING or not self._parents_done(call):
                continue
            verdict = self.plan.verdicts.get(key)
            if verdict is None:
                # Mid-run, hashing may use only the threads no running call holds.
                verdict = self.plan.classify(call, self.pool.free_threads())
            if verdict is Verdict.UP_TO_DATE:
                self._transition(call, Phase.DONE)
                self._record(_cached_event(call))
                pending.extend(self.children[key])
            else:
                self._transition(call, Phase.READY)

    def _parents_done(self, call: RuleCall) -> bool:
        return all(
            self.phase[parent.relative_path] is Phase.DONE
            for parent in call.parent_calls
        )

    def _submit(
        self, threads: concurrent.futures.Executor, ready: list[RuleCall]
    ) -> None:
        """Start the scheduler's picks, in its order, while resources admit them."""
        remaining = self._calls_in(Phase.WAITING, Phase.READY)
        available = self.pool.available()
        for call in _validated_schedule(self.scheduler, ready, remaining, available):
            if not self.pool.admits(call):
                continue
            self.pool.acquire(call)
            self._transition(call, Phase.RUNNING)
            _logger.job_start(call)
            self._running[threads.submit(attempt, call, self.runner)] = call

    def _finish_some(self) -> None:
        """Wait for at least one running call, then record every finished one."""
        done, _ = concurrent.futures.wait(
            self._running, return_when=concurrent.futures.FIRST_COMPLETED
        )
        for future in done:
            self._finish(self._running.pop(future), future.result())

    def _finish(self, call: RuleCall, outcome: Outcome) -> None:
        self.pool.release(call)
        if outcome.ok:
            try:
                self._commit(call, outcome)
            except Exception as exc:
                # A missing declared output or failed metadata write fails the call.
                outcome = replace(outcome, error=exc)
        if not outcome.ok:
            self._fail(call, outcome)
            return
        self._clean_parents(call)
        self._settle(self.children[call.relative_path])

    def _commit(self, call: RuleCall, outcome: Outcome) -> None:
        """Validate every co-output, then record the call as up to date."""
        missing = [output.path for output in call.outputs if not output.path.exists()]
        if missing:
            raise RuntimeError(
                "command succeeded but output missing: " + ", ".join(map(str, missing))
            )
        call.write_dependencies(self.plan.hashes)
        write_ancestor_tgf(call)
        event = RuleCallExecution.from_call(
            call,
            state="up_to_date",
            cached=False,
            started_at=outcome.started_at,
            finished_at=outcome.finished_at,
            exit_code=0,
            output_size_bytes=call.output_size_bytes(),
        )
        event.write_run_toml()
        self._record(event)
        self._transition(call, Phase.DONE)
        _logger.job_done(call, event.duration_seconds())

    def _fail(self, call: RuleCall, outcome: Outcome) -> None:
        phase = Phase.INTERRUPTED if outcome.interrupted else Phase.FAILED
        self._transition(call, phase)
        event = RuleCallExecution.from_call(
            call,
            state=phase.value,
            cached=False,
            started_at=outcome.started_at,
            finished_at=outcome.finished_at,
            exit_code=outcome.exit_code,
            error=str(outcome.error),
        )
        self._record(event)
        _logger.job_failed(
            call, event.duration_seconds(), outcome.exit_code, outcome.error
        )
        self._block_descendants(call)
        self.failures.append(outcome.error)
        if not self.keep_going:
            # Running calls still finish and are recorded; nothing new starts.
            self.stopped = True

    def _block_descendants(self, call: RuleCall) -> None:
        pending = list(self.children[call.relative_path])
        while pending:
            child = pending.pop()
            if self.phase[child.relative_path] is Phase.WAITING:
                self._transition(child, Phase.BLOCKED)
                pending.extend(self.children[child.relative_path])

    def _record(self, event: RuleCallExecution) -> None:
        self.report[event.call_key] = event

    def _clean_orphans(self) -> None:
        for call in self.plan.orphans:
            if call.remove_workdir():
                _logger.cleaned(call)
                self.cleaned += 1

    def _clean_parents(self, call: RuleCall) -> None:
        """Remove parent workdirs whose consumers are all done (with autoclean)."""
        if not self.autoclean:
            return
        for parent in call.parent_calls:
            key = parent.relative_path
            if key in self.protected:
                continue
            consumers_done = all(
                self.phase[child.relative_path] is Phase.DONE
                for child in self.children[key]
            )
            if consumers_done and parent.remove_workdir():
                _logger.cleaned(parent)
                self.cleaned += 1


def _cached_event(call: RuleCall) -> RuleCallExecution:
    return RuleCallExecution.from_call(
        call,
        state="up_to_date",
        cached=True,
        output_size_bytes=call.output_size_bytes(),
    )


def _validate_scheduler(scheduler: Scheduler) -> None:
    try:
        signature = inspect.signature(scheduler)
    except (TypeError, ValueError):
        return
    try:
        signature.bind([], [], {})
    except TypeError:
        name = getattr(scheduler, "__name__", type(scheduler).__name__)
        raise TypeError(
            f"scheduler {name!r} does not match protocol: "
            "scheduler(ready, remaining, available_resources) -> list[RuleCall]"
        ) from None


def _validated_schedule(
    scheduler: Scheduler,
    ready: list[RuleCall],
    remaining: list[RuleCall],
    available_resources: dict[str, int],
) -> list[RuleCall]:
    selected = scheduler(ready, remaining, available_resources)
    if not isinstance(selected, list):
        raise TypeError(
            f"scheduler must return list[RuleCall], got {type(selected).__name__}"
        )
    ready_by_key = {call.relative_path: call for call in ready}
    seen: set[Path] = set()
    result: list[RuleCall] = []
    for call in selected:
        key = getattr(call, "relative_path", None)
        if key not in ready_by_key:
            raise ValueError(f"scheduler returned RuleCall that is not ready: {key}")
        if key in seen:
            raise ValueError(f"scheduler returned duplicate RuleCall: {key}")
        seen.add(key)
        result.append(ready_by_key[key])
    return result


def _dry_run_report(plan: ExecutionPlan) -> dict[str, RuleCallExecution]:
    """Log what would run; report only the calls already known to be cached."""
    report: dict[str, RuleCallExecution] = {}
    would_run = 0
    for call in plan.active:
        verdict = plan.verdicts.get(call.relative_path)
        if verdict is Verdict.UP_TO_DATE:
            event = _cached_event(call)
            report[event.call_key] = event
        elif verdict is not None and verdict.will_run:
            _logger.dry_run_call(call, verdict.name)
            would_run += 1
    _logger.dry_run_summary(would_run, len(report))
    return report


def run(
    dag: DAG,
    resource_caps: dict[str, int] | None = None,
    scheduler: Scheduler | None = None,
    keep_going: bool = False,
    autoclean: bool = False,
    dry_run: bool = False,
    rule_call_runner: Runner | None = None,
    forced_stale_call_keys: set[Path] | None = None,
    on_complete: Callable[[dict[str, RuleCallExecution]], None] | None = None,
    hasher: Hasher | str | None = None,
) -> dict[str, RuleCallExecution]:
    """Execute required RuleCalls atomically; return report keyed by call path.

    • forced_stale_call_keys is a set of canonical RuleCall.relative_path values forced to rerun despite valid cache.
    • hasher names the output content hasher (default BLAKE3); see `necroflow.hashers`.
    • rule_call_runner(call, log_path) replaces the default runner, `runners.run_call`.
    • Without keep_going, the first failure stops new submissions; running calls
      finish and are recorded, on_complete is skipped, and that failure is raised.
      With keep_going, independent branches continue, on_complete runs, and an
      `ExecutionFailed` group carrying the report is raised.
    """
    if not isinstance(dag, DAG):
        raise TypeError(f"run requires a DAG, got {type(dag).__name__}")
    scheduler = fifo_scheduler if scheduler is None else scheduler
    _validate_scheduler(scheduler)
    runner = run_call if rule_call_runner is None else rule_call_runner
    caps = {"threads": default_thread_cap(), **(resource_caps or {})}
    dag.bind_thread_cap(caps["threads"])

    with _acquire_lock(dag.nodes_dir):
        # Nothing runs yet, so hashes taken while planning may use every thread.
        # Holding the lock, even for a dry run, makes refreshed hashes safe to store.
        plan = plan_execution(
            dag,
            forced_stale_call_keys=forced_stale_call_keys,
            hasher=hasher,
            hash_threads=caps["threads"],
            store_refreshed_hashes=True,
        )
        if dry_run:
            return _dry_run_report(plan)
        execution = Execution(
            plan,
            scheduler=scheduler,
            pool=ResourcePool(caps),
            runner=runner,
            keep_going=keep_going,
            autoclean=autoclean,
        )
        execution.run()
        if on_complete is not None and not execution.stopped:
            on_complete(execution.report)

    execution.raise_failures()
    return execution.report
