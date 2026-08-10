# Scheduler Protocol

[Previous: Executor](executor.md) | [README](../README.md) | [Next: Release](release.md)

Schedulers order ready `RuleCall` objects. Executor owns dependency gates, resource admission, submission, retries, state transitions, and failure handling.

## Protocol

```python
def scheduler(
    ready: list[RuleCall],
    remaining: list[RuleCall],
    available_resources: dict[str, int],
) -> list[RuleCall]:
    ...
```

- `ready`: calls whose parents are up to date and which are not running.
- `remaining`: every not-done, not-running call; superset of `ready`.
- `available_resources`: remaining capacity for capped resources.

Return a `list` containing only ready calls, without duplicate `relative_path` values. Necroflow validates scheduler arity before mutating node-store state and validates every selection before submission.

## FIFO default

```python
from necroflow.schedulers import fifo_scheduler

dag.run(scheduler=fifo_scheduler)
```

`fifo_scheduler` returns `ready` unchanged. Active calls were selected from insertion-ordered `DAG.calls`, so this means first canonical registration wins whenever dependencies and resources permit.

FIFO is sole built-in and default. There is no connected-component scheduler.

## Custom scheduler

A plain function or callable object may reorder or filter ready calls:

```python
def largest_thread_job_first(ready, remaining, available_resources):
    return sorted(
        ready,
        key=lambda call: call.resources.get("threads", 1),
        reverse=True,
    )
```

CLI loads one with:

```bash
necroflow run --scheduler schedulers.py:largest_thread_job_first job.toml
```

Returning an empty list while no jobs run ends scheduling. Custom schedulers must therefore select work whenever progress is possible.

Resource limits remain executor policy. Scheduler order does not authorize over-cap submission, except executor solo fallback for one oversized call.

[Previous: Executor](executor.md) | [README](../README.md) | [Next: Release](release.md)
