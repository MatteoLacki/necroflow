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
| Rules, typed outputs, subtypes, conditional pipelines, subpipelines | `docs/rules.md` |
| Execution, scheduling, resources, failure handling, autoclean | `docs/execution.md` |
| Output layout, `.rip/` metadata, caching, STALE detection | `docs/caching.md` |
| CLI flags and subcommands (`run`, `graph`, `outputs`, `provenance`, `doctor`, `explain`, `gc`) | `docs/cli.md` |
| Job TOML format and `__grid` parameter grids | `docs/job-toml.md` |
| Config validation callbacks | `docs/config-validation.md` |
| Generated config files (`text_file` rules) | `docs/generated-config-files.md` |
| Rule-call compilation, interning, paths, labels, requests, execution handoff | `docs/rule-call-lifecycle.md` |
| Dev workflow, release | `docs/development.md`, `docs/release.md` |
| Coding style and anti-patterns | `.claude/skills/necroflow-style/SKILL.md` |
| Adding/editing a rule (placeholders, typed outputs, mistakes) | `.claude/skills/add-a-rule/SKILL.md` |
| Node re-ran or cached unexpectedly | `.claude/skills/debug-stale-classification/SKILL.md` |
| Writing a custom scheduler | `.claude/skills/write-a-scheduler/SKILL.md` |
| Doctor preflight checks, findings, side effects, and limits | `docs/doctor.md` |

Skills under `.claude/skills/` are auto-loaded by Claude Code; other agents should read them
as plain markdown via this table.

`AGENTS.md` is a symlink to this file for non-Claude agents.

**Keep the lifecycle document synchronized with pipeline internals.** Any change to
Pipeline or DAG construction, rule-call compilation, fingerprint/path derivation,
canonical interning, label assignment, request selection, or the execution handoff must
update `docs/rule-call-lifecycle.md` in the same change.

## Verify against the live code, not prose

Before asserting how something behaves, prefer machine-readable introspection over docs:

```bash
necroflow graph --json job.toml        # DAG structure as JSON
necroflow outputs --json job.toml      # requested output paths
necroflow explain job.toml             # what would run and why (per-node reasons)
necroflow doctor job.toml              # preflight checks with stable NF_* issue codes
necroflow provenance --json nodes/rule/rule_hash/provenance_hash/file
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
- A pre-commit hook at `.githooks/pre-commit` (via `core.hooksPath`) acts only when
  staged changes include Python files. It runs `black` on existing changed Python paths,
  re-stages them, then runs `pytest`; a failing test rejects the commit.
  **If a commit is rejected, diagnose and fix the failing tests before re-attempting.**
- Regression tests land in the same commit as the fix.

## Stable invariants (safe to rely on)

These have been true since the June refactors and are load-bearing design decisions:

- **Filesystem is state, no databases.** Run state is plain text in `.rip/state`
  (`running` / `up_to_date` / `failed` / `interrupted`); a leftover `running` after a crash,
  or any unrecognized state value, marks the node compromised and forces a re-run.
  The concurrency lock is `fcntl.flock` on
  `.rip/necroflow.lock` — one instance per node store.
- **Content-addressed, not time-addressed.** Staleness uses an mtime fast path, then falls back
  to the stored SHA-256 content hash (`.rip/{filename}.hash`). A parent that re-ran but produced
  identical output must NOT invalidate children.
- **Split hashes name directories.** Fingerprint v3 uses framed canonical values and paths
  `{rule}/{rule_hash}/{provenance_hash}/{filename}`. The local rule hash covers recipe
  structure; the provenance hash covers config, shell, and parent lineage. Co-outputs share
  one canonical `RuleCall`, both hashes, workdir, and realized command. Constraints and
  `repeat` remain excluded. Fingerprinting is framework-owned.
- **Identity via `node.relative_path`, never `id()`.** It is a `Path` relative to
  `dag.nodes_dir` and is stable across node-store roots. Use it for adjacency, visited sets,
  requested outputs, and executor bookkeeping; serialize it with `.as_posix()` in JSON.
- **Co-outputs run once.** All outputs of one rule call are produced by a single submission; the
  siblings are marked done together.
- **`repeat` counts command attempts.** `repeat=N` makes one scheduler submission
  and runs the selected command runner at most `N` times, stopping at the first
  success. Only process failures are retried; the default `repeat=1` makes one
  attempt. Retry policy remains outside fingerprints.
- **Exit 0 with a missing declared output is a failure.** The executor checks `path.exists()`
  after every job.
- **`.rip/` per-output metadata**: `dependencies.toml` (accumulated ancestor config),
  `{filename}.hash`, `job.log`, `state`, `run.toml` (timings/size), `graph.txt` (ancestor
  render), `{filename}.invalidation` (NodeType invalidator token, when set).
- **Canonicalization is eager; labels are explicit.** Every `Pipeline(dag, ...)` references a
  shared DAG. A rule call fingerprints and interns its `RuleCall` immediately; equivalent calls
  return identical Node objects. Attribute/item assignment records qualified Pipeline-local labels.
  Several labels may alias one Node; Nodes do not carry a singular pipeline label.
- **Subpipelines are prefixed views.** `P.subpipeline(prefix)` shares its root Pipeline's DAG,
  shell policy, nodes, labels, and finished state while qualifying attribute/item access with a
  canonical non-empty request prefix. Prefixes remain outside fingerprints, and nested prefixes
  compose. Reusable subpipeline factories receive external input Nodes explicitly.
- **Pipeline construction has an explicit boundary.** `P.finish()` freezes the root and every
  subpipeline view. Later rules fail before fingerprinting/interning; later bindings and view
  creation also fail. Only the root may finish, `finish()` is idempotent, and `P.sinks()` requires
  finished construction. The CLI finishes each Pipeline after its factory returns successfully.
- **Pipeline labels are safe visible paths.** Item labels may be canonical relative POSIX
  paths. Assignment rejects absolute, empty, dot-prefixed, `.`/`..`, repeated/trailing
  separator, Linux byte-limit, and file/directory-conflicting result paths. Labels select
  visible result paths and remain outside fingerprints. CLI result paths receive an exact
  destination-filesystem preflight before execution and again before result materialization.
- **CLI results are copies.** Only requested outputs are copied into `results/`; Linux uses
  GNU `cp -a --reflink=auto`, macOS uses `cp -a -c`, and both fall back to physical copies.
  `manifest.toml` records each visible path, canonical origin node key, and content hash.
- **Filename-less NodeTypes are input-only.** A `NodeType` with `filename = None` may be
  used as a fixed, union, or variadic input contract, but every Rule output must resolve to an
  explicit filename. Rule declaration rejects filename-less outputs; output names are not fallbacks.
- **Mutable NodeTypes ignore content changes only.** `NodeType.mutable` defaults to `False`
  and is copied to each output Node. Mutable parents retain identity, ordering, provenance, and
  failure propagation, but newer changed bytes do not stale consumers. Missing, stale, forced,
  compromised, and invalidator-changed parents still propagate. Autoclean preserves mutable state.
- **Variadic Node inputs retain groups.** `tuple[NodeType, ...]` accepts an ordered
  tuple, while `Annotated[tuple[NodeType, ...], Many(...)]` applies inclusive size
  bounds. RuleCall/fingerprint/command contexts retain named tuple groups;
  `RuleCall.parents` is their declaration-order flattening for graph traversal.
- **Addresses are eager.** The Pipeline owns fingerprint/shell policy while its DAG owns the
  node-store root. A rule call returns Nodes with final fingerprints, relative paths, and absolute
  paths; there is no late resolution, DAG reindexing, or delayed deduplication.
- **Execution is DAG-only.** After a factory returns, call `P.finish()`, then
  `dag.require(P.sinks())` (or explicit label-selected Nodes), then `dag.execute()`.
  `DAG.add` and `execute(Pipeline)` do not exist.

## Scheduler protocol (current — 3 arguments)

```python
def my_scheduler(ready: list[Node], remaining: list[Node],
                 available_resources: dict[str, int]) -> list[Node]:
    """Return ready nodes in priority order; the executor submits from the front."""
```

- `ready` — nodes whose parents are all done, not yet running
- `remaining` — all not-yet-done, not-yet-running nodes (superset of ready)
- `available_resources` — remaining capacity for capped resources, e.g. `{"threads": 12}`
- The return value must be a `list` containing only currently ready nodes, with no duplicates;
  `execute()` rejects invalid selections before submission.
- Plain callables and callable objects both work; `execute()` rejects wrong-arity schedulers
  up front with a `TypeError` naming this protocol.
- Built-ins in `src/necroflow/schedulers.py`: `connected_component_scheduler` (default;
  incremental smallest-component-first) and `fifo_scheduler` (registration order).
  CLI: `--scheduler connected-components | fifo | file.py:callable`.

## `execute()` — check the docstring for details

`necroflow.executor.execute(dag, resource_caps=None, scheduler=..., keep_going=False,
autoclean=False, dry_run=False, node_runner=None, forced_stale_keys=None,
on_complete=None)
-> dict[str, ExecutionEvent]`

The dict is keyed by `node.relative_path.as_posix()`. `DAG.execute()` forwards
all kwargs and stores the same dict as `dag.last_execution_report`.
Full semantics: the `execute()` docstring and `docs/execution.md`.

## File map

```
src/necroflow/
  nodes.py           — Node, NodeState, NodeType/NodeTypeMeta, topo sort, connected components,
                       per-node state files
  rule_call.py       — concrete rule invocation, shared identity and command state
  contexts.py        — immutable NamedValues and CommandArgs public views
  fingerprints.py    — canonical v3 rule/provenance hashes and callable AST identity
  rules.py           — Rule internals plus command, text-file, and symlink-file declarations,
                       parse_resource with SI/binary suffixes
  schedulers.py      — Scheduler protocol, fifo_scheduler, ConnectedComponentScheduler
  dag.py             — path-length checks, resolve_command, write_dependencies,
                       classify_nodes, content hashing
  pipeline.py        — _GraphBase, Pipeline (prefixed views, labels, finish), DAG, ASCII rendering
  executor.py        — execute(), resource caps, lock, ExecutionEvent, autoclean, keep_going
  logger.py          — thread-safe job logging
  config.py          — job TOML loading and grid expansion (iter_job_configs, JobConfig)
  grid.py            — __grid TOML expansion and deterministic result labels
  gc.py              — provenance-aware node-store scanning, reporting, and deletion
  cli.py             — CLI argument parsing and command adapters, split nodes-dir/results-dir
                       layout, manifests, copied result trees
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
