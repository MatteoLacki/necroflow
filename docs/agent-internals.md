# Agent contributor reference

Detailed framework invariants and contributor conventions. Start with
[AGENTS.md](../AGENTS.md) for routing and pipeline usage. Code is authoritative.

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
  or any unrecognized state value, marks the RuleCall compromised and forces a re-run.
  The concurrency lock is `fcntl.flock` on
  `.rip/necroflow.lock` — one instance per node store.
- **Content-addressed, not time-addressed.** Each consumer records `consumed_hash` for
  every immutable parent output in `dependencies.toml`. Current hashes use an mtime-gated fast path
  through `.rip/{filename}.hash`; externally edited outputs are rehashed once, as node-store lock
  holders (real and dry runs) store the refreshed digest. `explain` never hashes them; it reports
  their consumers as undecided. By default, rebuilding a RuleCall reruns every active
  descendant, even with identical bytes. `--reuse-unchanged` / `reuse_unchanged=True`
  opts into preserving consumers whose recorded parent hashes still match.
- **Content hashers are pluggable.** Default BLAKE3; `sha256` built in; `--hasher file.py:Class`
  (a `name` plus `hash_path(path, threads)`). Every stored digest is tagged `<name>:<hex>`, and a
  consumed hash from another hasher counts as missing, so switching hashers reruns each consumer
  once and can never produce a false match. Output hashes use the call's own `threads`; planning
  uses the `-c` cap before anything runs and only the free threads mid-run.
- **Complete call hashes name directories.** Fingerprint v4 uses framed canonical values and paths
  `{rule}/{provenance_hash}/{filename}`. The stored rule hash covers recipe structure; the provenance hash includes
  it plus config, shell, parent lineage, and selected mixed-input plain values. Co-outputs share
  one canonical `RuleCall`, both hashes, workdir, and realized command. Constraints and
  `repeat` remain excluded. Fingerprinting is framework-owned.
- **Identity via `node.relative_path`, never `id()`.** It is a `Path` relative to
  `dag.nodes_dir` and is stable across node-store roots. Use it for adjacency, visited sets,
  requested outputs, and executor bookkeeping; serialize it with `.as_posix()` in JSON.
- **RuleCalls are atomic.** Requesting any output activates, caches, executes, reports, retains, and
  cleans the complete RuleCall. One submission produces and validates every declared co-output.
  Only explicitly requested Nodes are copied into `results/`.
- **`repeat` counts command attempts.** `repeat=N` makes one scheduler submission
  and runs the selected command runner at most `N` times, stopping at the first
  success. Only process failures are retried; the default `repeat=1` makes one
  attempt. Retry policy remains outside fingerprints.
- **Exit 0 with a missing declared output is a failure.** The executor checks `path.exists()`
  after every job.
- **`.rip/` per-RuleCall metadata**: `dependencies.toml` (lineage, mixed-input plain values,
  and consumed immutable parent hashes), `{filename}.hash`, `job.log`, `state`, `run.toml` (call timings/size), `graph.tgf`
  (ancestor DAG in Trivial Graph Format), `{filename}.invalidation` (Node invalidator token, when set).
- **Canonicalization is eager; labels are explicit.** Every `Pipeline(dag, ...)` references a
  shared DAG. A rule call fingerprints and interns its `RuleCall` immediately; equivalent calls
  return identical Node objects. Attribute/item assignment records qualified Pipeline-local labels.
  Several labels may alias one Node; Nodes do not carry a singular pipeline label.
- **Workflows scope ownership, labels stay explicit.** `@workflow` requires an open
  Pipeline as its first positional argument, activates it while a synchronous function
  runs, and restores the previous context on return or exception. Rules accept either
  that context or an explicit leading Pipeline; explicit ownership wins per call.
  Missing/wrong owners and deferred function kinds have explanatory errors. The
  decorator neither labels returned Nodes nor finishes the Pipeline.
- **Subpipelines are prefixed views.** `P.subpipeline(prefix)` shares its root Pipeline's DAG,
  shell policy, nodes, labels, and finished state while qualifying attribute/item access with a
  canonical non-empty request prefix. Prefixes remain outside fingerprints, and nested prefixes
  compose. Reusable subworkflows receive external input Nodes explicitly.
- **Pipeline construction has an explicit boundary.** `P.finish()` freezes the root and every
  subpipeline view. Later rules fail before fingerprinting/interning; later bindings and view
  creation also fail. Only the root may finish, `finish()` is idempotent, and `P.sinks()` requires
  finished construction. The CLI finishes each Pipeline after its workflow returns successfully.
- **Pipeline labels are safe visible paths.** Item labels may be canonical relative POSIX
  paths. Assignment rejects absolute, empty, dot-prefixed, `.`/`..`, repeated/trailing
  separator, Linux byte-limit, and file/directory-conflicting result paths. Labels select
  visible result paths and remain outside fingerprints. CLI result paths receive an exact
  destination-filesystem preflight before execution and again before result materialization.
- **CLI results are copies.** Only requested outputs are copied into `results/`; Linux uses
  GNU `cp -R -P --preserve=links,timestamps --reflink=auto`, macOS uses `cp -a -c`,
  and both fall back to physical copies. Linux copies use destination permission policy
  without preserving source ACLs, ownership, or extended attributes.
  `manifest.toml` records each visible path, canonical origin node key, and content hash.
- **Filename-less Node subclasses are input-only.** A `Node` with `filename = None` may be
  used as a fixed, union, or variadic input contract, but every Command output must resolve to an
  explicit filename. Command declaration rejects filename-less outputs; output names are not fallbacks.
- **Mixed fixed inputs select their branch per call.** A union containing Node and plain-value
  arms remains positional. A matching Node creates a parent edge; a matching plain value creates no
  edge and enters provenance as a named canonical value along with positional input order. Only
  matching plain values may be defaults, and positional defaults must be trailing. Mixed variadic
  element unions remain unsupported.
- **Persistent external state does not belong in a workdir.** A workdir is a function of its
  declared inputs, so any identity change gives a fresh empty one; state that must outlive that
  lives outside the node store and is passed in as a path. A `mutable=True` Command flag existed for
  this until 2026-09-16 and was removed: it stopped external byte edits from staling consumers but
  could not stop the workdir itself being discarded whenever identity moved.
- **Variadic Node inputs retain groups.** `tuple[Node, ...]` accepts an ordered
  tuple, while `Annotated[tuple[Node, ...], Many(...)]` applies inclusive size
  bounds. RuleCall/fingerprint/command contexts retain named tuple groups;
  `RuleCall.parents` is their declaration-order flattening for graph traversal.
- **Addresses are eager.** The Pipeline owns fingerprint/shell policy while its DAG owns the
  node-store root. A rule call returns Nodes with final fingerprints, relative paths, and absolute
  paths; there is no late resolution, DAG reindexing, or delayed deduplication.
- **Execution is DAG-only.** After a workflow returns, call `P.finish()`, then
  `dag.require(P.sinks())` (or explicit label-selected Nodes), then `dag.run()`.
  `DAG.add` and `run(Pipeline)` do not exist.

## Scheduler protocol (current — 3 arguments)

```python
def my_scheduler(ready: list[RuleCall], remaining: list[RuleCall],
                 available_resources: dict[str, int]) -> list[RuleCall]:
    """Return ready calls in priority order; executor submits from front."""
```

- `ready` — calls whose parent calls are all done, not yet running
- `remaining` — all not-yet-done, not-yet-running calls (superset of ready)
- `available_resources` — remaining capacity for capped resources, e.g. `{"threads": 12}`
- Return value must be a `list` containing only currently ready calls, without duplicates.
- Plain callables and callable objects work; wrong arity fails before node-store mutation.
- `fifo_scheduler` is sole built-in and default; order follows canonical RuleCall registration.
  CLI: `--scheduler fifo | file.py:callable`.

## `run()` — check docstring for details

`necroflow.executor.run(dag, resource_caps=None, scheduler=None, keep_going=False,
autoclean=False, dry_run=False, rule_call_runner=None, forced_stale_call_keys=None,
on_complete=None, hasher=None, reuse_unchanged=False)
-> dict[str, RuleCallExecution]`

Dict keyed by `call.relative_path.as_posix()`. `DAG.run()` forwards all kwargs and stores same
dict as `dag.last_execution_report`. Without `keep_going`, the first failure stops new submissions,
running calls finish and are recorded, and that failure is re-raised; with it, an
`ExecutionFailed` group carries the report. Full semantics: `run()` docstring and `docs/executor.md`.

## File map

```
src/necroflow/
  nodes.py           — Node and Node typed output concepts
  rule_call.py       — concrete invocation: identity, command realization, `.rip/` state and
                       dependency metadata
  contexts.py        — immutable NamedValues and CommandArgs public views
  containers.py      — Docker config value, `{env}:` command prefix, docker run argv
  fingerprints.py    — canonical v4 rule/provenance hashes and callable AST identity
  rules.py           — Command internals plus command, text-file, and symlink-file declarations,
                       parse_resource with SI/binary suffixes, threads="all" resolution
  schedulers.py      — RuleCall Scheduler protocol and fifo_scheduler
  fs.py              — node-store locking, path/shell validation, and output mtimes
  hashers.py         — pluggable output content hashers (BLAKE3 default, SHA-256), digest tags,
                       OutputHashes (stored-hash fast path per invocation)
  dag.py             — canonical registry, required-call closure, thread-cap binding, run()
  planning.py        — ExecutionPlan, Verdict, one-pass lazy consumed-hash classification
  pipeline.py        — Pipeline (prefixed views, labels, finish)
  workflow.py        — synchronous @workflow decorator and scoped rule ownership
  tgf.py             — TGF and Graphviz DOT text rendering, Node labels, per-call ancestor graph
  executor.py        — run(), Execution (per-run Phase state machine), ResourcePool, autoclean,
                       failure handling
  runners.py         — default runner run_call, attempt() with retries and timing, Outcome
  report.py          — RuleCallExecution report events, run.toml records, event counts
  logger.py          — job logging; the CLI configures output, the library never does
  config.py          — job TOML loading and grid expansion (iter_job_configs, JobConfig)
  grid.py            — __grid TOML expansion and deterministic result labels
  gc.py              — provenance-aware node-store scanning, reporting, and deletion
  cli.py             — CLI argument parsing and command adapters, split nodes-dir/results-dir
                       layout, manifests, copied result trees
  mcp_server.py      — optional MCP server (mcp extra): read-only graph/outputs/explain/
                       doctor/provenance tools, console script `necroflow-mcp
                       [--transport stdio|http] [--host H] [--port P]` (default stdio)
  templates/         — `necroflow init` project template (canonical pipeline + schema)
  skills/            — packaged pipeline usage skill printed by `necroflow guide`
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

## Executable shell construction

`declarations.py` analyses schemas/source; `resources.py` resolves typed resource policy.

Bare `@command` analyses a fixed prefix of `outputs(...)` assignments and the
input/config/resource signature. Its original function executes once per user
call during graph construction. `construction.py` provides a ContextVar-backed
Construction and public shell; no AST-generated closures remain.

Validate bindings and apply resource overrides before construction. Compute source
recipe identity and final artifact paths from the fixed schema/input lineage;
create an unregistered RuleCall. outputs() retrieves precreated typed artifacts.
shell(template) registers exactly one nonempty template. Validate its Docker prefix,
placeholders, and the exact returned artifacts before DAG interning. Always reset
the construction context in finally. Commands inside command bodies are rejected;
composition belongs in workflows. Repeated calls execute bodies again but intern
canonical artifacts. Failed bodies leave no DAG call or framework-created files.

Source body AST and Python identity define the versioned construction recipe.
Resource defaults/values and the constructed template are excluded from identity;
ordinary config remains hashed. Resource branches must preserve semantic results
and output schema. Resource[int/float/str] values resolve before body execution;
float requests/caps and memory_bytes are supported. Executable declarations reject
threads="all"; use a concrete integer. Caps cannot replay construction decisions.

Templates render once with shell quoting. F-string doubled braces preserve
placeholders; literal shell braces must be doubled in the resulting template.
Artifact arguments stay typed; .path is available without implying file existence.
Inspection also executes bodies: prints go to the Python process, not subprocess
logs. Source hashing does not discover arbitrary helper/global/environment state.

Existing dynamic factories, external CommandArgs callbacks, and text/symlink
materializers retain their separate lifecycle and identities. Old inline command
assignments/nested command functions are replaced by shell registration.
