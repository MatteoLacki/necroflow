---
name: write-a-scheduler
description: How to write a custom necroflow scheduler — current 3-argument protocol, registration via Python or CLI, and pitfalls. Load before writing or modifying scheduler code.
---

# Writing a necroflow scheduler

## Protocol (3 arguments — not 2)

```python
def my_scheduler(ready, remaining, available_resources):
    """Return ready nodes in priority order; the executor submits from the front."""
    return sorted(ready, key=lambda n: n.rule.constraints.get("threads", 1), reverse=True)
```

- `ready: list[Node]` — parents all done, not yet running
- `remaining: list[Node]` — all not-yet-done, not-yet-running nodes (superset of `ready`)
- `available_resources: dict[str, int]` — remaining capacity for capped resources, e.g. `{"threads": 12}`

The protocol used to be 2 arguments; `execute()` now rejects wrong-arity callables up front
with a `TypeError` naming the 3-argument form. Callable objects (a class with `__call__`)
work too — see `make_connected_component_scheduler()` in `src/necroflow/schedulers.py`,
detailed in `docs/schedulers.md`.

## Registration

```python
dag.execute(scheduler=my_scheduler)                 # Python
```

```bash
necroflow --scheduler schedulers.py:my_scheduler job.toml   # CLI
necroflow --scheduler fifo job.toml                          # built-ins: fifo, connected-components
```

## Rules of the game

- Ordering is advisory: the executor walks your list from the front and skips jobs whose
  resource requirements don't fit `available_resources` right now. A job exceeding a cap
  still runs solo when nothing else is running.
- Return only nodes from `ready` (a permutation/subset). Returning others is ignored at best.
- **Key nodes by `node.relative_path`, never `id(node)`** — DAG deduplication aliases node objects.
- If the scheduler keeps state across calls (adjacency, component sizes), build it from a
  factory function that returns a fresh closure per `execute()` call — never a shared
  singleton, which leaks state across separate executions.
  `make_connected_component_scheduler()` is the reference implementation (incremental re-BFS
  on the affected component only); see `docs/schedulers.md` for exactly how it works and its
  cost tradeoffs.
- Schedulers must not mutate nodes or touch `node.state` — that is the executor's state machine.

## Testing pattern

See `tests/test_executor.py` (`test_scheduler_receives_available_resources`,
`test_fork_scheduler_order`): record calls with a closure, assert on submission order via
side effects. Docstring states the invariant, not the steps.
