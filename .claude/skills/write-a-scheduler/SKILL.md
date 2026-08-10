---
name: write-a-scheduler
description: How to write a custom necroflow RuleCall scheduler using current three-argument protocol.
---

# Writing a necroflow scheduler

## Protocol

```python
def my_scheduler(ready, remaining, available_resources):
    """Return ready RuleCalls in priority order."""
    return sorted(
        ready,
        key=lambda call: call.resources.get("threads", 1),
        reverse=True,
    )
```

- `ready: list[RuleCall]`: parent calls done; call not running.
- `remaining: list[RuleCall]`: all not-done, not-running calls; superset of ready.
- `available_resources: dict[str, int]`: remaining capped capacity.

Return a list containing only objects from `ready`, without duplicate `call.relative_path`. Executor rejects wrong arity before node-store mutation and invalid selections before submission. Callable objects work.

## Registration

```python
dag.run(scheduler=my_scheduler)
```

```bash
necroflow run --scheduler schedulers.py:my_scheduler job.toml
necroflow run --scheduler fifo job.toml
```

FIFO is sole built-in and default. It preserves canonical RuleCall registration order.

## Rules

- Ordering is priority. Executor may skip selected calls whose resources do not currently fit.
- One call exceeding a cap may run alone when nothing else runs.
- Key calls by `call.relative_path`, never `id(call)`.
- Schedulers must not mutate `call.state`, graph structure, or filesystem state.
- Returning an empty list while nothing runs ends scheduling.
- `remaining` may contain calls with `state is None`: lazy classification waits for parent bytes.
- Scheduler sees RuleCalls, never individual co-output Nodes.

## Test pattern

Record calls with a closure and assert submission order through runner side effects. Test docstrings state invariants.
