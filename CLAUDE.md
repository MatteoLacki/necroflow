# necroflow

Python pipeline framework inspired by Snakemake. Rules are declared as module-level values with `@command`;
the framework owns path generation, DAG construction, and execution.

**Source of truth is the code.** This file records only stable invariants, conventions, and a
routing table. For API details read the docstrings and the docs below — if this file ever
disagrees with the code, the code wins (and this file should be fixed).
`tests/test_docs.py` guards this file against drift; if it fails, update this file.

## Where to look things up

| Topic | Read |
|---|---|
| Compact map of the whole feature surface | `features.txt` |
| Recent / agent-relevant feature notes | `AI.md` |
| Rules, typed outputs, subtypes, conditional pipelines, sections | `docs/rules.md` |
| Execution, scheduling, resources, failure handling, autoclean | `docs/execution.md` |
| Output layout, `.rip/` metadata, caching, STALE detection | `docs/caching.md` |
| CLI flags and subcommands (`run`, `graph`, `outputs`, `provenance`, `doctor`, `explain`) | `docs/cli.md` |
| Job TOML format and `__grid` parameter grids | `docs/job-toml.md` |
| Config validation callbacks | `docs/config-validation.md` |
| Generated config files (`text_file` rules) | `docs/generated-config-files.md` |
| Dev workflow, release | `docs/development.md`, `docs/release.md` |
| Coding style and anti-patterns | `.claude/skills/necroflow-style/SKILL.md` |
| Adding/editing a rule (placeholders, typed outputs, mistakes) | `.claude/skills/add-a-rule/SKILL.md` |
| Node re-ran or cached unexpectedly | `.claude/skills/debug-stale-classification/SKILL.md` |
| Writing a custom scheduler | `.claude/skills/write-a-scheduler/SKILL.md` |

Skills under `.claude/skills/` are auto-loaded by Claude Code; other agents should read them
as plain markdown via this table.

`AGENTS.md` is a symlink to this file for non-Claude agents.

## Verify against the live code, not prose

Before asserting how something behaves, prefer machine-readable introspection over docs:

```bash
necroflow graph --json job.toml        # DAG structure as JSON
necroflow outputs --json job.toml      # requested output paths
necroflow explain job.toml             # what would run and why (per-node reasons)
necroflow doctor job.toml              # preflight checks with stable NF_* issue codes
necroflow provenance --json nodes/rule/hash/file
python -c "import inspect, necroflow.executor as e; print(inspect.signature(e.execute))"
```

## Setup

```bash
make venv          # creates .venv with Python 3.14 via uv, installs package editable
source .venv/bin/activate
```

## Testing

- When investigating pytest failures, **read the failing test docstring first** — it states the
  invariant the test guards, not the steps.
- A pre-commit hook at `.githooks/pre-commit` (via `core.hooksPath`) runs `black` on all tracked
  Python files, re-stages them, then runs `pytest`; a failing test rejects the commit.
  **If a commit is rejected, diagnose and fix the failing tests before re-attempting.**
- Regression tests land in the same commit as the fix.

## Stable invariants (safe to rely on)

These have been true since the June refactors and are load-bearing design decisions:

- **Filesystem is state, no databases.** Run state is plain text in `.rip/state`
  (`running` / `up_to_date` / `failed` / `interrupted`); a leftover `running` after a crash
  marks the node compromised and forces a re-run. The concurrency lock is `fcntl.flock` on
  `.rip/necroflow.lock` — one instance per node store.
- **Content-addressed, not time-addressed.** Staleness uses an mtime fast path, then falls back
  to the stored SHA-256 content hash (`.rip/{filename}.hash`). A parent that re-ran but produced
  identical output must NOT invalidate children.
- **Fingerprints name directories.** `node.fingerprint` (16-hex) hashes rule name + command +
  config + parent fingerprints + Inputs/Outputs types; co-outputs of one rule call share it.
  `node.key` = `rule/fingerprint/filename` is the unique identity. Constraints (`threads`,
  `ram`) are intentionally excluded — execution resources are not computation identity.
- **Identity via `node.key`, never `id()`.** DAG deduplication aliases node objects; `id()`-keyed
  dicts and sets break. Use `.key` for adjacency, visited-sets, done-tracking.
- **Co-outputs run once.** All outputs of one rule call are produced by a single submission; the
  siblings are marked done together.
- **Exit 0 with a missing declared output is a failure.** The executor checks `path.exists()`
  after every job.
- **`.rip/` per-output metadata**: `dependencies.toml` (accumulated ancestor config),
  `{filename}.hash`, `job.log`, `state`, `run.toml` (timings/size), `graph.txt` (ancestor
  render), `{filename}.invalidation` (NodeType invalidator token, when set).
- **Explicit over implicit.** Nodes are registered by attribute assignment (`P.bam = align(...)`);
  there is no context-manager or `ContextVar` auto-registration, and there never should be again.

## Scheduler protocol (current — 3 arguments)

```python
def my_scheduler(ready: list[Node], remaining: list[Node],
                 available_resources: dict[str, int]) -> list[Node]:
    """Return ready nodes in priority order; the executor submits from the front."""
```

- `ready` — nodes whose parents are all done, not yet running
- `remaining` — all not-yet-done, not-yet-running nodes (superset of ready)
- `available_resources` — remaining capacity for capped resources, e.g. `{"threads": 12}`
- Plain callables and callable objects both work; `execute()` rejects wrong-arity schedulers
  up front with a `TypeError` naming this protocol.
- Built-ins in `src/necroflow/schedulers.py`: `connected_component_scheduler` (default;
  incremental smallest-component-first) and `fifo_scheduler` (registration order).
  CLI: `--scheduler connected-components | fifo | file.py:callable`.

## `execute()` — check the docstring for details

`necroflow.executor.execute(pipeline, outdir, resource_caps=None, scheduler=..., keep_going=False,
autoclean=False, dry_run=False, node_runner=None, forced_stale_keys=None, shellpath=None)
-> ExecutionReport`

`DAG.execute()` forwards all kwargs and stores the report as `dag.last_execution_report`.
Full semantics: the `execute()` docstring and `docs/execution.md`.

## File map

```
src/necroflow/
  nodes.py           — Node, NodeState, NodeType/NodeTypeMeta, topo sort, connected components,
                       per-node state files (.state_file, .is_compromised, .mark_running/.mark_done)
  rules.py           — Rule internals plus command, text-file, and symlink-file declarations,
                       parse_resource with SI/binary suffixes
  schedulers.py      — Scheduler protocol, fifo_scheduler, ConnectedComponentScheduler
  dag.py             — resolve_paths (incl. path-length checks), resolve_command, write_dependencies,
                       classify_nodes, content hashing
  pipeline.py        — _GraphBase, Pipeline (sections, labels), DAG, ASCII rendering, save()
  executor.py        — execute(), resource caps, lock, ExecutionReport, autoclean, keep_going
  logger.py          — thread-safe job logging
  config.py          — job TOML loading and grid expansion (iter_job_configs, JobConfig)
  grid.py            — __grid TOML expansion (vendored from snakemakeconfigs)
  cli.py             — CLI: run + init/graph/outputs/provenance/doctor/explain subcommands,
                       split nodes-dir/results-dir layout, manifests, symlink trees
  graphviz_render.py — optional PNG rendering (dev extra)
  keywords.py        — reserved pipeline label names
  _compat.py         — ExceptionGroup backport
  templates/         — `necroflow init` project template (canonical pipeline + schema)
  tools/             — config_set.py: config-file transformation helper
```

Tests live in `tests/` (one file per concern); runnable examples in `examples/`.

## Human review markers

- A comment `#<name> reviewed` (e.g. `#matteo reviewed`) means a human reviewed that code. The
  marker covers the statements at its indentation level and everything indented deeper.
- If you change code covered by such a marker, replace the marker with `#needs human review`.
- Never add `#<name> reviewed` yourself — only humans mark code as reviewed.

## What is NOT yet implemented

- Cluster/cloud backends (long-term goal, not currently prioritised)
- Long-range edges in the ASCII renderer (edges skipping layers are omitted; planned fix:
  dummy-node insertion)
