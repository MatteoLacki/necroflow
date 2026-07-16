---
name: necroflow-style
description: necroflow coding style for AI agents — invariants and anti-patterns derived from the git history. Load before writing or refactoring necroflow source code.
---

# necroflow coding style for AI agents

Derived from the git history: what the refactors converged on, and why.

## Core principles

### Filesystem is state — no databases
Prefer plain files over embedded databases for local state.
- State lives in `.rip/state` (text: `running` / `up_to_date` / `failed`)
- Concurrency lock is `fcntl.flock` on `.rip/necroflow.lock`
- Provenance is `dependencies.toml` + `{filename}.hash` — readable without tooling
- Rationale: SQLite was dropped because plain files compose better with standard Unix tools, survive crashes identically, and eliminate a dependency.

### Content-addressed, not time-addressed
mtime is a fast-path shortcut, not the source of truth.
- Staleness check: `mtime fast-path → SHA-256 content hash fallback`
- A parent that re-ran without changing its output must NOT invalidate children
- Constraints (`threads`, `memory`) are intentionally excluded from fingerprints — execution resources ≠ computation identity
- Fingerprints use `hashlib.sha256` updated incrementally, not `repr(tuple(...))`

### Identity via stable keys, never `id()`
`id(obj)` is object identity — it breaks across DAG deduplication.
Use `node.key` (`rule/fingerprint/filename`) everywhere adjacency or visited-sets are needed.
The regression: the original scheduler used `{id(n): ...}` dicts; deduplication created aliased nodes whose `id()` was never in the dict.

### Single responsibility per module
When a module grows a second conceptual domain, extract it:
- `nodes.py` — Node, NodeType, NodeState, topo_sort, connected components
- `rules.py` — Inputs, Outputs, Constraints, Rule, Rules, parse_resource
- `schedulers.py` — Scheduler protocol, fifo_scheduler, ConnectedComponentScheduler
- `executor.py` — orchestration only; no state logic, no resource parsing

### Methods own their domain
State transitions belong on the object that owns the state.
- `node.mark_running()`, `node.mark_done()`, `node.is_compromised` → on `Node`
- `rule.resources` → on `Rule`
- Free functions in `executor.py` that operated on node internals were moved to methods

### Stateful class over stateless function when init cost matters
The connected-component scheduler was a stateless function recomputing all components on every call — O(n) per scheduler tick.
Replaced with `ConnectedComponentScheduler`: computes once, then on each job completion only re-BFS's the affected component.
Pattern: if a callable needs to remember work across calls within one `execute()` run, make it a class with `reset()` logic.

### Context managers for paired operations
`_acquire_lock()` is a `@contextmanager`, not a manual try/finally.
Prefer `with` over manual resource management for anything that pairs acquire/release.

### Security: quote paths going to shell
`resolve_command()` wraps Path substitutions with `shlex.quote()` before formatting string commands.
Config values (`str`/`int`) are NOT quoted — they're user-controlled content.
List commands bypass the shell entirely and need no quoting.

## Test style

### Every edge case gets a docstring
Test docstrings explain the INVARIANT, not the steps:
```python
def test_execute_handles_outdir_with_spaces(tmp_path):
    """String command placeholders must survive output paths containing spaces.

    Necroflow owns the generated output paths, so callers should not have to
    manually quote every {output} and {input} placeholder just because the
    selected outdir contains whitespace.
    """
```

### Regression tests land in the same commit as the fix
The pattern throughout: `fix: <description>` commits include both the fix and the test in one diff.

### Read the failing test docstring first
The docstring IS the spec. Before investigating a failure, read what the test says it is guarding.

## Origin-era anti-patterns (June 10–20)

These were the vibe-coded starting points that all got replaced. Don't recreate them.

### Magic auto-registration via `ContextVar`
The first pipeline API was `with Pipeline() as P:` — rules auto-registered nodes into the active `ContextVar`. Clever but untestable and surprising.
Replaced with explicit attribute assignment `P.bam = R.align(...)`. Explicit beats implicit.

### External deps for things pure Python can do
`pipeline.plot()` used `networkx` + `matplotlib` for DAG rendering. Removed. Replaced with pure terminal ASCII using box-drawing chars. No dependency beats a dependency.

### Set-based done-tracking via `id()`
The first executor: `done_ids = {id(n) for n in nodes if check_cache(n)}`. Two problems:
1. `id()` breaks after deduplication (aliased nodes)
2. A set of done ids carries no failure/running state — the state machine (`NodeState` enum) makes every state queryable.

### SQLite as "proper" persistence
`StateDB` (SQLite) was introduced June 20, removed June 25 — 5 days. The plain `.rip/state` file approach was simpler, survived crashes identically, required no dependency, and composed with standard tools. Don't reach for a database for simple flags.

### No tests until it hurts
First 9 days had zero tests. First tests (June 19) immediately exposed a co-output collision bug (`_node_hash` colliding for sibling outputs) that had been invisible. Write tests from the first non-trivial function.

### Dead-on-arrival functions
`check_cache()` existed from the initial executor and was never correctly integrated — it was deleted without ever being properly called. `@input_rule` was a separate decorator (vs `@rule`) that lasted ~2 days. Don't write code you're not sure you need.

## What to avoid (summary)

- `repr(tuple(...))` for hashing — use `hashlib.sha256` updated incrementally
- `id(n)` as dict keys or set members — use stable `.key`
- `ContextVar` magic for implicit registration — use explicit assignment
- External deps (networkx, matplotlib) when pure Python works
- SQLite for simple local state flags
- Free functions that mutate object internals — make them methods
- Monolithic modules — extract when a second domain appears
- Dead code — delete it immediately (`check_cache`, `StateDB`, `@input_rule`)
- `object.__setattr__`/`__getattribute__` overrides when plain attribute access works
- Tests that arrive after the first bug — write them with the feature
