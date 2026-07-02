# necroflow

Python pipeline framework inspired by Snakemake. Rules are registered via a `Rules` container;
the framework owns path generation, DAG construction, and execution.

## Testing

When investigating pytest failures, read the failing test docstring first; it should describe the problem rationale and the behavior the test is guarding.

A pre-commit hook at `.githooks/pre-commit` runs `pytest` before every commit and rejects the commit if any test fails. Git is configured to use this via `core.hooksPath = .githooks`. **If a commit is rejected by the hook, automatically diagnose and fix the failing tests before re-attempting the commit.**

## Setup

```bash
make venv          # creates .venv with Python 3.14 via uv, installs package editable
source .venv/bin/activate
```

## Core concepts

### NodeState (`src/necroflow/dag.py`)

Each node carries a `state: NodeState | None` field set by `classify_nodes()`.

| State | Meaning |
|---|---|
| `MISSING` | In required subgraph, no output — must run |
| `STALE` | In required subgraph, output exists but a parent changed content — must run |
| `UP_TO_DATE` | In required subgraph, output valid — skip |
| `ORPHAN` | Outside required subgraph, output exists |
| `READY` | MISSING/STALE with all parents UP_TO_DATE — submit now |
| `RUNNING` | Submitted to thread pool |
| `FAILED` | Execution error |

`STALE` propagates transitively: if a parent is MISSING or STALE, all descendants are also STALE.

```python
classify_nodes(nodes, required_nodes)
# sets node.state on every node in the list
# required_nodes + all ancestors → MISSING/STALE/UP_TO_DATE
# outside subgraph with output → ORPHAN
# outside subgraph without output → None (excluded)
```

Requires `resolve_paths()` to have been called first.

### Executor (`src/necroflow/executor.py`)

```python
execute(graph, outdir, resource_caps=None, scheduler=None, keep_going=False, autoclean=False)
```

Accepts any `_GraphBase` (Pipeline or DAG). Calls `graph.resolve_paths(outdir)` and `classify_nodes()` internally, then runs only the required subgraph:

- Parallel execution via `concurrent.futures.ThreadPoolExecutor`
- `resource_caps`: `{resource: int}` upper bounds; defaults to `{"threads": os.cpu_count()}`. Resources absent from `resource_caps` are unconstrained.
- `threads` in `Constraints` defaults to 1 if not declared — so unconstrained jobs still count against the thread cap.
- A job whose requirements exceed a cap still runs solo when nothing else is running.
- UP_TO_DATE and ORPHAN nodes skipped; state transitions MISSING/STALE → READY → RUNNING → UP_TO_DATE/FAILED
- **Co-outputs** (same rule call, same hash dir) are submitted once; all siblings are marked UP_TO_DATE when the representative node completes
- After a successful job, `node.path.exists()` is checked — `RuntimeError` if the command exited 0 but the declared output is absent
- FAILED state propagates to descendants (they are skipped)
- `write_dependencies(node)` called after each successful job
- `keep_going=False` (default): raise on first failure
- `keep_going=True`: continue running independent branches; raise `ExceptionGroup` at the end listing all failures
- `autoclean=True`: (1) delete ORPHAN outputs before execution; (2) during execution, delete each intermediate node's output as soon as all its children are UP_TO_DATE (frees disk space progressively)

```python
execute(P, "/results")                                          # single pipeline, all CPUs
execute(P, "/results", resource_caps={"threads": 8})
execute(P, "/results", resource_caps={"threads": 8, "ram": 64 * 2**30})
dag.execute()                                                   # DAG uses dag.outdir
dag.execute(scheduler=fifo_scheduler)
dag.execute(keep_going=True)                                    # continue past failures
dag.execute(autoclean=True)                                     # delete orphans + intermediates when done
```

#### Scheduler protocol

```python
def my_scheduler(ready: list[Node], remaining: list[Node]) -> list[Node]:
    """Return ready nodes in priority order. Executor submits from the front."""
    ...
```

- `ready` — nodes whose parents are all done, not yet running
- `remaining` — all not-yet-done, not-yet-running nodes (superset of ready)

Schedulers are plain callables. `execute()` accepts either a function or a callable object instance.

Built-in schedulers (`src/necroflow/schedulers.py`):

- `connected_component_scheduler` *(default)* — module-level instance of `ConnectedComponentScheduler`. Computes connected components once on first call, then updates incrementally: on each job completion only the affected component is re-BFS'd to detect splits. All other components are untouched — O(1) size lookup. Resets automatically at the start of a new `execute()` run.
- `fifo_scheduler` — plain function; topological (registration) order.

### NodeType (`src/necroflow/dag.py`)

Base class for node types. Uses a metaclass so that calling the class creates a `Node`.

```python
class Fastq(NodeType):
    """Raw sequencing reads (FASTQ format)."""
    filename = "reads.fastq.gz"

class SortedBam(Bam):               # subtype — accepted wherever Bam expected
    """Coordinate-sorted BAM."""
    filename = "sorted.bam"

Fastq("label")  # → Node(output_name="label", node_type=Fastq)
```

`node.node_type` is the class itself. Subtype checks use `issubclass`.
Docstrings flow through to `node.info` via `Node.__post_init__`.

### Node (`src/necroflow/nodes.py`)

Dataclass representing a pipeline value. Fields:

- `.rule` — `Rule` object that produced it (carries `.constraints`, `.inputs`, `.outputs`, `.command`, `.resources`)
- `.parents` — list of input `Node`s
- `.config` — dict of keyword args passed at call time
- `.output_name` — string label set from `Outputs` kwargs
- `.pipeline_label` — Pipeline attribute name set when `P.xxx = node` is assigned; used as the manifest key in linked outputs
- `.node_type` — NodeType subclass
- `.command` — raw command template string (e.g. `"samtools sort {bam} -o {sorted_bam}"`)
- `.output_nodes` — `{name: Node}` dict of all co-outputs from the same rule call (enables command resolution)
- `.path` — `pathlib.Path` set by `resolve_paths()`
- `.state` — `NodeState | None` set by `classify_nodes()`

State file methods (path must be set before calling):

- `.state_file` — `Path` to `node.path.parent / ".rip" / "state"`
- `.is_compromised` — True if state file contains `running`, `failed`, or `interrupted`
- `.mark_running()` — write `"running"` to state file (creates `.rip/` dir if needed)
- `.mark_done(state)` — overwrite state file with `"up_to_date"` / `"failed"` / `"interrupted"`

### `Inputs`, `Outputs`, `Constraints`, `Rule` (`src/necroflow/rules.py`)

Helper classes for rule registration:

```python
Inputs(bam=Bam, ref=str)          # NodeType values → positional Node args; plain types → kwargs
Outputs(bam=Bam, log=Log)         # named outputs; kwargs become output_name on each Node
Constraints(threads=4, memory="8G")  # scheduler resources
```

### `Rules` container (`src/necroflow/rules.py`)

Holds registered rules. Names must be unique. Each registered rule becomes a callable attribute.

**Decorator style** (preferred) — requires `from __future__ import annotations` in the calling module:

```python
R = Rules()
rule = R.rule   # alias once

@rule(threads=4)
def align(fastq: Fastq, ref: str) -> (Bam[bam], Log[log]):
    """Align reads to a reference genome with BWA-MEM."""
    command = "bwa mem {ref} {fastq} > {bam} 2> {log}"

bam, log = R.align(fastq_node, ref="hg38")
```

Return annotation: `Type[name]` for single output, `(Type[name], ...)` for multiple.
Constraints as kwargs on `@rule(...)`. Docstring becomes `info`. Decorator replaces the
function with the registered `Rule` object — `R.align` and the bare name `align` are the same.

**Explicit style** — always available, no future import needed:

```python
R.register(
    "align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam, log=Log),
    "bwa mem {ref} {fastq} > {bam} 2> {log}",
    Constraints(threads=4),
    info="Align reads to a reference genome with BWA-MEM.",
)

bam, log = R.align(fastq_node, ref="hg38")
# bam.config           == {"ref": "hg38"}
# bam.rule.constraints == {"threads": 4}
# bam.command          == "bwa mem {ref} {fastq} > {bam} 2> {log}"
```

Positional args = input Nodes (matched by NodeType annotation order); keyword args = config.
Single output → returns `Node` directly; multiple outputs → returns named tuple.

**Validation at call time:**
- Positional arity: too few positional args → `TypeError` naming the missing inputs
- Positional arg NodeType check: `issubclass(val.node_type, expected)` — subtypes accepted
- Keyword arg type check: `isinstance(val, type)` — union types (`str | int`) supported

### `_GraphBase`, `Pipeline`, `DAG` (`src/necroflow/pipeline.py`)

`_GraphBase` is the shared base class providing `__str__`, `save()`, `resolve_paths()`.
Subclasses override three hooks: `nodes` (property), `_header()`, `_node_label()`, `_node_color()`.

#### `Pipeline` — single-config container

Attribute-style node registration. Assigning a Node auto-registers it and stamps `node.pipeline_label = attr_name`.
Duplicate attribute name → `ValueError`.

```python
def basic_pipeline(config, R):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=config.path)
    P.bam, P.align_log = R.align(P.fastq, ref=config.ref)
    P.sorted_bam = R.sort_bam(P.bam)
    P.counts, P.qc = R.quantify(P.sorted_bam, gene_model=config.gene_model)
    return P
```

#### `DAG` — multi-pipeline aggregator

Stores nodes by `node.key` (= `rule_name/fingerprint/filename`). Deduplicates shared upstream
computations across pipelines automatically. Co-outputs of the same rule call share a directory
(same fingerprint) but have distinct keys (different filename). Tracks a required set (target nodes).

```python
dag = DAG()
for pipeline_fn, config in zip(pipelines, configs):
    P = pipeline_fn(config, R)
    dag.add(P)                         # request defaults to sinks of P
    # dag.add(P, request=[P.counts])   # explicit targets

dag.execute("/results")
```

`dag.required_nodes` — nodes marked as required targets (rendered with ★ / orange).

Sinks = nodes with no dependents (children) in the pipeline. Includes source nodes (nodes with no parents), so a single-node pipeline is valid and executes correctly.

#### Terminal rendering (`print(P)` / `print(dag)`)

Layered ASCII DAG with Unicode box-drawing characters, grouped by topological depth.
Only edges between adjacent layers are drawn; long-range edges are omitted (known visual gap).

#### File rendering (`.save(path)`)

`P.save("pipeline.txt")` / `dag.save("dag.txt")` — writes `str(self) + "\n"` to a UTF-8 file.


### Path generation (`src/necroflow/dag.py`)

```python
P.resolve_paths("/results")
# sets node.path = /results/{rule_name}/{fingerprint}/{filename}
```

Two-level hashing:

- **`node.fingerprint`** — 16-char hex hash of the rule call (rule name + command + config + parent fingerprints + Inputs/Outputs types). Shared by all co-outputs of the same call. Names the output directory. Constraints intentionally excluded: they describe execution resources, not the computation itself.
- **`node.key`** — `rule_name/fingerprint/filename`. Unique per node including co-outputs. Used as the DAG dict key.

Deterministic: same DAG + same root inputs → same paths. Different inputs → different fingerprint → different directory → cache miss.

```python
node.path.exists()  # True → already computed for these inputs (cache hit)
```

### Command resolution (`src/necroflow/dag.py`)

```python
resolve_command(node)
# formats node.command template: {input_name} → parent.path, {output_name} → node.path,
# {config_key} → config value (str/int passed through as-is)
# Path substitutions are wrapped with shlex.quote() for string commands (shell=True)
# so that paths containing spaces are handled correctly. List commands are unaffected.
```

Requires `resolve_paths()` to have been called first.

```python
resolve_command(bam_node)
# "bwa mem hg38 /results/raw_fastq/e19fd828/fastq > /results/align/9ffe3fbe/bam 2> /results/align/795995c2/log"
```

## File map

```
src/necroflow/
  nodes.py      — Node (dataclass), NodeState, NodeType/NodeTypeMeta, _topo_sort, _is_nodetype,
                  iter_connected_components;
                  Node.state_file, .is_compromised, .mark_running(), .mark_done() (per-node state)
  rules.py      — Inputs, Outputs, Constraints, Rule (with .resources property), Rules;
                  parse_resource + SI/binary suffix tables
  schedulers.py — Scheduler type alias, fifo_scheduler, ConnectedComponentScheduler class,
                  connected_component_scheduler (module-level instance)
  dag.py        — resolve_paths, resolve_command, write_dependencies, classify_nodes,
                  _content_hash, _output_mtime, _accumulated_config;
                  re-exports parse_resource from rules.py
  pipeline.py   — _GraphBase (incl. save()), Pipeline, DAG (_sinks, _label, _render_connector);
                  DAG.execute() forwards **kwargs to executor.execute()
  executor.py   — execute (accepts any _GraphBase), _run_node;
                  _acquire_lock (@contextmanager, fcntl.flock),
                  _prepare_active, _promote_states, _on_job_done
  logger.py     — thread-safe logging: job_start/done/failed/error/output, summary
  grid.py       — TOML __grid expansion (vendored from snakemakeconfigs) + iter_configs()
  cli.py        — necroflow CLI entry point; _load_factory, _create_link_outputs
  __init__.py   — exports all public symbols

examples/
  simple_dag.py             — linear + diamond pipelines; registration, path resolution, command resolution
  necroalchemy.py           — 17-node silly text-transform pipeline; multi-word DAG; uses .save()
  necroalchemy_factory.py   — CLI factory for necroalchemy; import-safe (CLI adds examples/ to sys.path)
  necroalchemy_grid.toml    — parameter grid TOML (word × n); run with necroalchemy_factory.py
  schedulers.py             — minimal example comparing fifo_scheduler vs default (connected_component)

tests/
  test_classify_nodes.py — NodeState classification, co-output deduplication, stale propagation,
                           command-change cache invalidation
  test_executor.py       — execute() correctness, scheduler ordering (chain + fork), thread budget,
                           autoclean, dry-run, resource caps, conditional pipelines
  test_keep_going.py     — keep_going=True: independent branches, failure propagation, ExceptionGroup
  test_state.py          — per-node state file unit tests + crash/fail/interrupt/retry integration tests
```

### `.rip/` metadata — per-output provenance and content hashes (`src/necroflow/dag.py`)

Each output folder (`outdir/{rule}/{fingerprint}/`) contains a `.rip/` subdirectory written
after each successful job:

- **`dependencies.toml`** — flat accumulated config from all ancestors; the full provenance record.
- **`{filename}.hash`** — SHA-256 content hash of each co-output file/directory (excluding `.rip/` itself).
- **`job.log`** — captured stdout/stderr of the job.
- **`state`** — plain-text run state: `running` (written at job start), `up_to_date` / `failed` / `interrupted` (overwritten on completion). If the process is killed, `state` stays as `running`; the next run detects this and re-runs the node even if its output exists.
- **`graph.txt`** — ASCII render of the node and all its ancestors (provenance subgraph), written by `write_ancestor_graph(node)` in `pipeline.py`. Each node box includes its per-node config kwargs.

### Concurrency (`src/necroflow/executor.py`)

Only one necroflow instance may run against a given `outdir` at a time. `execute()` acquires an exclusive `fcntl.flock` on `outdir/.rip/necroflow.lock` at startup and releases it on exit. Running two instances against overlapping outdirs (e.g. `results` and `results/sub`) is unsupported — there is no OS primitive to detect this.

```toml
# .rip/dependencies.toml
rule = "sort_bam"
hash = "4fb08953b0120f5b"

[config]
path = "/data/sample.fastq.gz"
ref = "hg38"
```

```python
write_dependencies(node)  # writes dependencies.toml + {filename}.hash for all co-outputs
```

`_accumulated_config(node)` traverses strictly upward (ancestors only); assumes config key names
are unique across the pipeline.

#### STALE detection (`classify_nodes`)

STALE check uses mtime as a fast path, then falls back to content hash:

1. If a parent's mtime ≤ this node's mtime → skip (not newer).
2. If a parent's mtime > this node's mtime → read `.rip/{filename}.hash`.
   - If stored hash matches current content → parent re-ran but produced identical output → skip.
   - Otherwise → mark STALE.

This means a parent that re-ran without changing its output (e.g. a re-linked symlink or a
recomputed-but-identical file) does not invalidate its children.

### Parent normalisation in executor (`src/necroflow/executor.py`)

After DAG deduplication, unique nodes can hold parent references to superseded
objects (from other pipelines) that are never classified, leaving them with
`state=None`. The executor remaps all parent pointers to canonical nodes before
calling `classify_nodes`, so the READY-promotion check `all(p.state == UP_TO_DATE)` sees
consistent state:

```python
canonical = {n.key: n for n in nodes}
for n in nodes:
    n.parents[:] = [canonical.get(p.key, p) for p in n.parents]
```

This runs after `resolve_paths` and before `classify_nodes` on every `execute()` call.

### `grid.py` — parameter grids (`src/necroflow/grid.py`)

Vendored from `snakemakeconfigs.toml_patcher`. Expands `__grid` keys in TOML
documents into a Cartesian product of (label, plain_dict) pairs.

```python
from necroflow.grid import iter_configs
import tomlkit

doc = tomlkit.parse("""
word__grid = ["necroflow", "snakemake"]
n__grid    = [2, 5]
""")

for label, cfg in iter_configs(doc, base_stem="experiment"):
    print(label, cfg)
# experiment__word+necroflow__n+2  {'word': 'necroflow', 'n': 2}
# experiment__word+necroflow__n+5  {'word': 'necroflow', 'n': 5}
# experiment__word+snakemake__n+2  {'word': 'snakemake', 'n': 2}
# experiment__word+snakemake__n+5  {'word': 'snakemake', 'n': 5}
```

`iter_configs(doc, base_stem, grid_suffixes, short_names, equal_sign)` yields
`(label: str, config: dict)`. If no `__grid` keys, yields `(base_stem, plain_dict)`.

`_to_plain_dict(doc)` converts tomlkit proxy types to plain Python dicts/lists/scalars.

### CLI (`src/necroflow/cli.py`)

Entry point: `necroflow.cli:main`, registered as `necroflow` in `pyproject.toml`.

```bash
necroflow \
  --pipeline path/to/factory.py:function_name \
  --config   experiment.toml \        # repeatable
  --outdir   /results \
  [--threads 16] [--keep-going]
```

- `--pipeline FILE:FUNC` — loads `FILE`, imports `FUNC(cfg: dict) -> Pipeline`
- `--config`/`-c` — repeatable; each TOML is expanded with `iter_configs()`; all
  pipelines share one DAG (shared upstream nodes deduplicated automatically)

After every run the CLI unconditionally creates `outdir/{combo_label}/` with:
- Symlinks mirroring the `{rule}/{hash}/{file}` hash tree
- `manifest.toml` listing sink output paths, keyed by `node.pipeline_label`
  (the `P.xxx` attribute name from the factory function)

Key internals:
- `_load_factory(spec)` — splits `"file.py:func"`, loads with `importlib.util`,
  inserts `file.parent` into `sys.path` so relative imports in the factory work
- `_create_link_outputs(outdir, combos)` — relative symlinks preserving
  `{rule}/{hash}/{file}` structure; skips nodes with no path or non-existent output
- `_sinks(P)` — nodes with no children in the pipeline (leaf nodes)

## What is NOT yet implemented

- Cluster/cloud backends (long-term goal, not currently prioritised)
- Long-range edges in the ASCII renderer (edges skipping layers are omitted; planned fix: dummy-node insertion)


## User reviews
- USer reviews are done using #<name of reviewer> reviewed.
- review is about current level of identation and those right to it for python.
- Upon changes in code, change comment to #needs human review.