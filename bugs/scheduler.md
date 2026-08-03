# `connected_component_scheduler` leaks state between `execute()` calls

**Severity:** medium. Silent wrong scheduling order, never an exception. Not reachable
through `necroflow run` today — only through library use of `execute()`.

**Location:** `src/necroflow/schedulers.py:112-129`, reached via the default argument at
`src/necroflow/executor.py:552`.

---

## Summary

`connected_component_scheduler` is a single long-lived object, not a function:

```python
# src/necroflow/schedulers.py:132
connected_component_scheduler = ConnectedComponentScheduler()
```

That one instance is the default argument of `execute()`:

```python
# src/necroflow/executor.py:549
def execute(
    dag: DAG,
    resource_caps: dict[str, int] | None = None,
    scheduler: Scheduler = connected_component_scheduler,
```

and the CLI hands out the same object:

```python
# src/necroflow/cli.py:80
builtins = {
    "connected-components": connected_component_scheduler,
    "fifo": fifo_scheduler,
}
```

The instance carries six mutable attributes describing *one particular graph*. They are
never reset. A second `execute()` in the same process inherits the first run's leftovers
and mistakes them for its own graph.

---

## The code

State lives on the instance:

```python
# src/necroflow/schedulers.py:30
def __init__(self) -> None:
    self._adj: dict[Path, list[Path]] = {}
    self._component_of: dict[Path, int] = {}
    self._members: dict[int, set[Path]] = {}
    self._sizes: dict[int, int] = {}
    self._prev_keys: set[Path] = set()
    self._next_cid: int = 0
```

Every call decides between a full rebuild and an incremental update:

```python
# src/necroflow/schedulers.py:112
def __call__(
    self, ready: list, remaining: list, available_resources: dict[str, int]
) -> list:
    if self._adj is None or (not self._component_of and remaining):
        self._build(remaining)
        self._prev_keys = {n.relative_path for n in remaining}
    else:
        current_keys = {n.relative_path for n in remaining}
        for key in self._prev_keys - current_keys:
            self._remove(key)
        self._prev_keys = current_keys

    return sorted(
        ready,
        key=lambda n: self._sizes.get(
            self._component_of.get(n.relative_path, -1), 0
        ),
    )
```

Everything below follows from that `if` on line 115.

---

## Step 1 — the rebuild guard cannot fire on a second run

The condition is `self._adj is None or (not self._component_of and remaining)`.

**The first disjunct is dead code.** `_adj` is initialised to `{}` on line 31 and the only
other assignment is `self._adj = adj` on line 51 inside `_build`. It is never `None`, so
`self._adj is None` is always `False`.

**So a rebuild happens only when `_component_of` is empty.** `_component_of` is filled by
`_build`:

```python
# src/necroflow/schedulers.py:66
self._component_of[k] = cid
```

and drained only by `_remove`:

```python
# src/necroflow/schedulers.py:72
cid = self._component_of.pop(key, None)
if cid is None:
    return
```

For `_component_of` to be empty at the end of a run, every key must have passed through
`_remove`.

## Step 2 — the last run's keys never pass through `_remove`

`_remove` is called only for keys that *disappeared* from `remaining`:

```python
# src/necroflow/schedulers.py:120
for key in self._prev_keys - current_keys:
    self._remove(key)
```

So the question becomes: does the executor ever call the scheduler with an empty
`remaining` at the end of a run? It does not:

```python
# src/necroflow/executor.py:654
while any(n.state in needs_run for n in active):
    _promote_states(active)

    ready = [n for n in active if n.state == NodeState.READY]
    remaining = [n for n in active if n.state in needs_run]
    ...
    for node in _validated_schedule(
        scheduler, ready, remaining, available_resources
    ):
```

The loop condition is checked *before* the scheduler runs. Once the final node leaves
`needs_run`, the loop exits without another scheduler call. The last call the scheduler
ever sees therefore always has a non-empty `remaining`, and those keys stay in
`_component_of` forever.

**Result:** at the end of every run, `_component_of` is non-empty, so the next run takes
the `else` branch and treats a brand-new graph as an incremental update of the old one.

## Step 3 — the sort silently accepts garbage

```python
# src/necroflow/schedulers.py:124
return sorted(
    ready,
    key=lambda n: self._sizes.get(
        self._component_of.get(n.relative_path, -1), 0
    ),
)
```

Two chained `.get` calls with defaults. A node the scheduler has never seen resolves to
component `-1`, and `-1` resolves to size `0`. No `KeyError`, no warning — it just sorts
first, as if it were the smallest possible component.

Had this been written `self._sizes[self._component_of[key]]`, the second run would raise
on its first unknown node and the bug would have been found immediately.

---

## Why this bites necroflow specifically

Node identity is `relative_path`, which is content-addressed:

```
{rule}/{rule_hash}/{provenance_hash}/{filename}
```

Those paths are *stable across runs by design*. So a second `execute()` in the same process
does not merely see unknown keys — it sees **some of the same keys as last time**, still
carrying last run's component ids and last run's shrunken sizes. Stale data mixes with
zeros, and the ordering is driven entirely by which keys happened to survive.

---

## Worked trace

Graph: chain `b → c → d` (one component of 3) plus singleton `x` (component of 1).
The scheduler should always pick `x` first.

### Warm-up run

| call | `remaining` | branch | `_component_of` after | `_sizes` | picks |
|---|---|---|---|---|---|
| 1 | `b c d x` | **build** | `b,c,d → 0`, `x → 1` | `{0:3, 1:1}` | `x` (size 1 wins) |
| 2 | `b c d` | incremental | `_remove(x)` → cid 1 deleted | `{0:3}` | `b` |
| 3 | `c d` | incremental | `_remove(b)`, re-BFS → `{c,d}` | `{0:2}` | `c` |
| 4 | `d` | incremental | `_remove(c)` → `{d}` | `{0:1}` | `d` |

Loop exits. Leftovers: `_component_of = {d: 0}`, `_sizes = {0: 1}`, `_prev_keys = {d}`.

### Second run — same graph, same keys

| call | `remaining` | `_component_of.get(…, -1)` per node | sort keys | picks |
|---|---|---|---|---|
| 1 | `b c d x` | `b,c,x → -1`; `d → 0` | `b=0 c=0 x=0` **`d=1`** | `b` |
| 2 | `c d x` | same | `c=0 x=0 d=1` | `c` |
| 3 | `d x` | same | `x=0 d=1` | `x` |
| 4 | `d` | — | — | `d` |

Call 1 never rebuilds, because `_component_of` still holds `{d: 0}` from the warm-up.
`_prev_keys - current_keys` is `{d} - {b,c,d,x}` = empty, so nothing is removed either.
The scheduler is now sorting a four-node graph using one leftover fact about `d`.

`d` — which is the *last* node of the largest component — gets the only non-zero size and
sorts last. `x`, the singleton the scheduler exists to prioritise, sorts third.

---

## Reproduction

```python
"""Reproduce the state leak in the shared ConnectedComponentScheduler singleton."""
import sys
sys.path.insert(0, "src")
from pathlib import Path
from necroflow.schedulers import (
    connected_component_scheduler as SHARED,
    ConnectedComponentScheduler,
)


class FakeNode:
    def __init__(self, key, parents=()):
        self.relative_path = Path(key)
        self.parents = list(parents)

    def __repr__(self):
        return str(self.relative_path)


def drain(scheduler, nodes):
    """Repeatedly run the scheduler's top pick, as execute() does."""
    remaining = list(nodes)
    order = []
    while remaining:
        pick = scheduler(remaining, remaining, {})[0]
        order.append(pick)
        remaining = [n for n in remaining if n is not pick]
    return order


def make_graph():
    """Chain b -> c -> d (component of 3), plus singleton x (component of 1)."""
    b = FakeNode("b")
    c = FakeNode("c", [b])
    d = FakeNode("d", [c])
    x = FakeNode("x")
    return [b, c, d, x]


graph = make_graph()
print("fresh instance      :", drain(ConnectedComponentScheduler(), graph))

warmup = make_graph()
drain(SHARED, warmup)
print("shared, after warmup:", drain(SHARED, graph))

print()
fresh_keys = [FakeNode("p"), FakeNode("q"), FakeNode("r", [FakeNode("q")])]
print("shared, unseen keys :", drain(SHARED, fresh_keys), "(input order — priority lost)")
```

Output:

```
fresh instance      : [x, b, c, d]
shared, after warmup: [b, c, x, d]

shared, unseen keys : [p, q, r] (input order — priority lost)
```

Same scheduler class, same graph, two different answers. The third line shows the other
failure mode: when *no* key is recognised, every sort key is `0`, `sorted` is stable, and
the scheduler silently degenerates into `fifo_scheduler`.

---

## Why it has not been noticed

**The CLI runs `execute()` exactly once per process.** `_build_dag_from_jobs` accumulates
every job and every grid variant into a single shared `DAG`:

```python
# src/necroflow/cli.py:213
dag = DAG(nodes_dir)
combos: list[_Combo] = []
...
for job_path_str in args.jobs:
    ...
    for job_config in job_configs:
        ...
        pipeline = Pipeline(dag, shellpath=shellpath)
```

and `_run` then calls `dag.execute(...)` once (`cli.py:656`). So `necroflow run` cannot
currently trigger this, no matter how many jobs or `__grid` variants are involved.

**The tests that exercise ordering construct fresh instances.** `tests/test_executor.py:702`
and `:763` both use `ConnectedComponentScheduler()` directly, which is exactly the case that
works. `tests/test_executor.py:553` uses the shared singleton but is the only such test, so
it is always the "first run" in its process.

So the bug is latent: it is a correctness trap for anyone embedding necroflow as a library,
and for any future CLI change that executes more than one DAG per process.

---

## Fixes

### Option A — reset per run (smallest change)

Give the class a `reset()` and call it at the top of `execute()`. Keeps the incremental
machinery. Downside: `execute()` now has to know that some schedulers are stateful, which
is not part of the scheduler protocol.

### Option B — make it a function (recommended)

The incremental re-BFS exists to make a *sort key* cheaper. Nothing in the repo measures
that it needed to be. The executor loop already performs three O(active) scans per
iteration (`executor.py:657-664`), and `nodes.py:218` already contains a correct,
stateless component walk:

```python
# src/necroflow/nodes.py:218
def iter_connected_components(nodes: list[Node]):
    """Yield each connected component of nodes as a list (undirected parent↔child edges)."""
```

Replacing the class with a function built on it removes ~110 lines, removes the second
connected-component implementation, and makes the bug structurally impossible:

```python
def connected_component_scheduler(ready, remaining, available_resources):
    """Prioritise nodes from the smallest connected component of remaining work."""
    size_of = {}
    for component in iter_connected_components(remaining):
        for node in component:
            size_of[node.relative_path] = len(component)
    return sorted(ready, key=lambda n: size_of[n.relative_path])
```

Note the plain `[]` lookup rather than `.get(..., 0)`: every node in `ready` is by
definition in `remaining`, so a miss is a bug and should say so loudly.

If profiling later shows the recomputation matters, memoise then — with the measurement in
hand.

### Regardless of option

Delete the dead `self._adj is None` disjunct on line 115. It has never been able to fire.
