# Scheduler Internals

[Previous: Executor, Classification, Scheduling, and Cleanup](executor.md) | [README](../README.md) | [Next: Release Checklist](release.md)

This page documents how the two built-in schedulers actually work. For the scheduler
*protocol* — the 3-argument callable contract, registration, and how to write your own —
see `.claude/skills/write-a-scheduler/SKILL.md` and the "Scheduling" section
of [executor.md](executor.md).

## `fifo_scheduler`

```python
def fifo_scheduler(ready, remaining, available_resources) -> list:
    return ready
```

Stateless, one line. `ready` is already in registration order because that is the order
`execute()` builds it in each iteration, so this is a no-op pass-through. No adjacency, no
per-call bookkeeping, `available_resources` unused. This is the baseline every other
scheduler's added complexity has to justify.

## `make_connected_component_scheduler()` — the default

`dag.execute()` uses this unless `scheduler=` says otherwise (`--scheduler
connected-components` on the CLI). It prioritises `ready` nodes by the size of the
**connected component** (undirected: parent↔child edges either direction) they currently
belong to within `remaining` — smallest first. The intent is to finish small, largely
independent subgraphs (e.g. one sample's whole pipeline) before starting new large ones,
keeping the number of simultaneously in-flight intermediates — and therefore peak disk and
memory — lower than a scheduler with no topology awareness.

`make_connected_component_scheduler()` is a **factory**, not a class: it returns a closure
over one fresh `_ConnectedComponentState`, so state cannot leak between separate `execute()`
calls. This replaced an earlier shared-singleton implementation
(`ConnectedComponentScheduler`) that leaked exactly that way — the same `remaining` set
across two different `execute()` calls silently treated the second graph as an incremental
update of the first and produced wrong or arbitrary orderings. Always create a new one per
execution; `execute(scheduler=None)` does this for you.

### State

```python
@dataclass
class _ConnectedComponentState:
    adj: dict[Path, list[Path]]        # node_key -> undirected neighbour keys
    component_of: dict[Path, int]      # node_key -> component id
    members: dict[int, set[Path]]      # component id -> member keys
    sizes: dict[int, int]              # component id -> len(members)
    prev_keys: set[Path] | None        # remaining's key set as of the previous call
    next_cid: int                      # monotonic component-id counter
```

### First call: full build

`prev_keys is None` triggers `_build_components`. It builds `adj` from every parent→child
edge where *both* endpoints are still in `remaining` (edges are undirected in `adj` — both
directions are appended), then flood-fills unvisited nodes one at a time, assigning each a
fresh component id and recording `component_of`/`members`/`sizes`. Cost: `O(V + E)`.

### Every later call: diff and remove, never rebuild

```python
current_keys = {node.relative_path for node in remaining}
for key in state.prev_keys - current_keys:
    _remove_component_key(state, key)
state.prev_keys = current_keys
```

Only the keys that disappeared since the previous call (jobs that just finished) are
processed. `_remove_component_key`:

1. Drops the key from `component_of` and `members[cid]`.
2. If the component is now empty, deletes `members[cid]`/`sizes[cid]` and returns.
3. Otherwise **re-floods only that one component**: walks the surviving members via `adj`
   (filtered to neighbours still inside the component) to find its connected subcomponent(s).
   The first subcomponent found reuses the old `cid`; every additional one — a split caused
   by removing a bridge node — gets a fresh id from `next_cid`.

`adj` itself is never pruned; it keeps edges to removed keys for the life of the closure.
Harmless, since every walk filters against the live member set, but it means the full
initial edge set stays resident in memory for the whole run.

### The payoff: the sort

```python
return sorted(ready, key=lambda node: state.sizes[state.component_of[node.relative_path]])
```

This is the only place `sizes`/`component_of` are read. Everything above exists solely to
make this sort key cheap to compute on every call. The sort itself is inexpensive — `ready`
is bounded by `remaining` and usually much smaller — and stable, so nodes tied on component
size keep registration order (FIFO within a size tier). `available_resources` is accepted
for protocol conformance and ignored; resource admission is the executor's responsibility,
not the scheduler's.

### Cost is graph-shape dependent

The incremental design pays off when `remaining` is many small, mostly-independent
components (each removal re-floods a small piece), and degrades toward the cost of a full
rebuild — `O(V)` per removal, `O(V^2)` over a run — when the graph is one large connected
component (e.g. many samples that all funnel through one shared reference-building stage).
Measured on 1500 nodes: a forest of 750 independent two-node chains finished its scheduling
calls roughly 7x faster than one long 1500-node chain. Neither case regresses below a naive
full-rebuild-every-call baseline; the incremental design is never *worse*, but its advantage
over that baseline shrinks to near zero on a single connected graph.

## Comparison: Snakemake's scheduler

Snakemake defaults to `--scheduler ilp`, solving a 0-1 multidimensional knapsack over ready
jobs with PuLP, and falls back to a greedy heuristic when the ILP times out (10s), errors, or
selects nothing. Its greedy path ranks jobs by the lexicographic tuple
`(priority, temp_size, input_size)` and packs them into remaining resource capacity.

`examples/snakemake_scheduler.py` ports that greedy selector to this protocol, with its
deviations documented in the module docstring — necroflow has no `priority:` directive and no
`temp()` marker, and its executor requires the scheduler to keep offering work rather than
returning an empty selection. Neither Snakemake scheduler is topology-aware in the way the
default here is: they optimise resource packing per batch, while
`make_connected_component_scheduler()` optimises which *subgraph* to finish next.

## Choosing between them

`fifo_scheduler` has no bookkeeping cost at any graph shape and is the right choice for a
Python callback scheduler comparison baseline, for a wide flat DAG with no meaningful
component structure, or when scheduling overhead itself is what's being profiled. The default
connected-component scheduler is worth its cost — and it is a real, measured cost, not
speculative — chiefly for pipelines built from many largely-independent per-sample subgraphs,
which is the more common shape in practice.

[Previous: Executor, Classification, Scheduling, and Cleanup](executor.md) | [README](../README.md) | [Next: Release Checklist](release.md)
