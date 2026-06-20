# necroflow

## Communication style

Respond in pirate dialect at all times — use "ye", "arr", "aye", "me hearties", "shiver me timbers", etc. Pepper responses with pirate insults directed at the user: "landlubber", "bilge rat", "scallywag", "barnacle-brained", "scurvy dog", "yellow-bellied", "blithering sea cucumber", and similar. Insults should be frequent but varied. Never break character.

Python pipeline framework inspired by Snakemake. Rules are registered via a `Rules` container;
the framework owns path generation, DAG construction, and execution.

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
| `STALE` | In required subgraph, output exists but a parent is newer — must run |
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
execute(graph, outdir, total_threads=None, scheduler=None, keep_going=False)
```

Accepts any `_GraphBase` (Pipeline or DAG). Calls `graph.resolve_paths(outdir)` and `classify_nodes()` internally, then runs only the required subgraph:

- Parallel execution via `concurrent.futures.ThreadPoolExecutor`
- Thread budget: sum of `constraints.threads` across running jobs ≤ `total_threads` (default `os.cpu_count()`)
- A job whose thread requirement exceeds the budget runs solo when nothing else is running
- UP_TO_DATE and ORPHAN nodes skipped; state transitions MISSING/STALE → READY → RUNNING → UP_TO_DATE/FAILED
- FAILED state propagates to descendants (they are skipped)
- `write_dependencies(node)` called after each successful job
- `keep_going=False` (default): raise on first failure
- `keep_going=True`: continue running independent branches; raise `ExceptionGroup` at the end listing all failures

```python
execute(P, "/results")                          # single pipeline, all CPUs
execute(P, "/results", total_threads=8)
dag.execute()                                   # DAG uses dag.outdir
dag.execute(scheduler=fifo_scheduler)
dag.execute(keep_going=True)                    # continue past failures
```

#### Scheduler protocol

```python
def my_scheduler(ready: list[Node], remaining: list[Node]) -> list[Node]:
    """Return ready nodes in priority order. Executor submits from the front."""
    ...
```

- `ready` — nodes whose parents are all done, not yet running
- `remaining` — all not-yet-done, not-yet-running nodes (superset of ready)

Built-in schedulers:

- `connected_component_scheduler` *(default)* — builds undirected graph of remaining nodes, finds connected components, prioritises nodes from the smallest component. Re-analyses after each completion so splitting components are handled dynamically.
- `fifo_scheduler` — topological (registration) order; equivalent to previous behaviour.

### NodeType (`src/necroflow/dag.py`)

Base class for node types. Uses a metaclass so that calling the class creates a `Node`.

```python
class Fastq(NodeType): ...          # single type
class SortedBam(Bam): ...           # subtype — accepted wherever Bam expected

Fastq, Bam, Log = node_types("fastq bam log")  # bulk creation (dynamic subclasses)

Fastq("label")  # → Node(output_name="label", node_type=Fastq)
```

`node.node_type` is the class itself. Subtype checks use `issubclass`.

### Node (`src/necroflow/dag.py`)

Dataclass representing a pipeline value. Fields:

- `.rule` — wrapper callable that produced it (carries `.constraints`, `.inputs`, `.outputs`, `.command`)
- `.parents` — list of input `Node`s
- `.config` — dict of keyword args passed at call time
- `.output_name` — string label set from `Outputs` kwargs
- `.node_type` — NodeType subclass
- `.command` — raw command template string (e.g. `"samtools sort {bam} -o {sorted_bam}"`)
- `.output_nodes` — `{name: Node}` dict of all co-outputs from the same rule call (enables command resolution)
- `.path` — `pathlib.Path` set by `resolve_paths()`
- `.state` — `NodeState | None` set by `classify_nodes()`

### `Inputs`, `Outputs`, `Constraints` (`src/necroflow/dag.py`)

Helper classes for rule registration:

```python
Inputs(bam=Bam, ref=str)          # NodeType values → positional Node args; plain types → kwargs
Outputs(bam=Bam, log=Log)         # named outputs; kwargs become output_name on each Node
Constraints(threads=4, memory="8G")  # scheduler resources
```

### `Rules` container (`src/necroflow/dag.py`)

Holds registered rules. Names must be unique. Each registered rule becomes a callable attribute.

```python
R = Rules()
R.register(
    "align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam, log=Log),
    "bwa mem {ref} {fastq} > {bam} 2> {log}",
    Constraints(threads=4),
)

bam, log = R.align(fastq_node, ref="hg38")
# bam.config       == {"ref": "hg38"}
# bam.rule.constraints == {"threads": 4}
# bam.command      == "bwa mem {ref} {fastq} > {bam} 2> {log}"
```

Positional args = input Nodes (matched by NodeType annotation order); keyword args = config.
Single output → returns `Node` directly; multiple outputs → returns named tuple.

**Validation at call time:**
- Positional arg NodeType check: `issubclass(val.node_type, expected)` — subtypes accepted
- Keyword arg type check: `isinstance(val, type)` — union types (`str | int`) supported

### `_GraphBase`, `Pipeline`, `DAG` (`src/necroflow/pipeline.py`)

`_GraphBase` is the shared base class providing `__str__`, `save()`, `plot()`, `resolve_paths()`.
Subclasses override three hooks: `nodes` (property), `_header()`, `_node_label()`, `_node_color()`.

#### `Pipeline` — single-config container

Attribute-style node registration. Assigning a Node (or named tuple of Nodes) auto-registers it.
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

Stores nodes by `_node_key` (= `rule_name/folder_hash/filename`). Deduplicates shared upstream
computations across pipelines automatically. Co-outputs of the same rule call share a directory
(`folder_hash`) but have distinct keys (different filename). Tracks a required set (target nodes).

```python
dag = DAG()
for pipeline_fn, config in zip(pipelines, configs):
    P = pipeline_fn(config, R)
    dag.add(P)                         # request defaults to sinks of P
    # dag.add(P, request=[P.counts])   # explicit targets

dag.execute("/results")
```

`dag.required_nodes` — nodes marked as required targets (rendered with ★ / orange).

Sinks = nodes with at least one parent that no other node in the pipeline depends on.

#### Terminal rendering (`print(P)` / `print(dag)`)

Layered ASCII DAG with Unicode box-drawing characters, grouped by topological depth.
Only edges between adjacent layers are drawn; long-range edges are omitted (known visual gap).

#### File rendering (`.save(path)`)

`P.save("pipeline.txt")` / `dag.save("dag.txt")` — writes `str(self) + "\n"` to a UTF-8 file.

#### Matplotlib rendering (`.plot()`)

Uses `networkx` + `matplotlib`. Required nodes (DAG only) shown in orange.

### Path generation (`src/necroflow/dag.py`)

```python
P.resolve_paths("/results")
# sets node.path = /results/{rule_name}/{folder_hash}/{filename}
```

Two-level hashing:

- **`_folder_hash(node)`** — 8-char hash of the rule call (rule name + command + config + parent fingerprints). Shared by all co-outputs of the same call. Names the output directory. Command string included so rule code changes invalidate the cache.
- **`_node_key(node)`** — `rule_name/folder_hash/filename`. Unique per node including co-outputs. Used as the DAG dict key.

Deterministic: same DAG + same root inputs → same paths. Different inputs → different folder_hash → different directory → cache miss.

```python
node.path.exists()  # True → already computed for these inputs (cache hit)
```

### Command resolution (`src/necroflow/dag.py`)

```python
resolve_command(node)
# formats node.command template: {input_name} → parent.path, {output_name} → node.path,
# {config_key} → config value (str/int passed through as-is)
```

Requires `resolve_paths()` to have been called first.

```python
resolve_command(bam_node)
# "bwa mem hg38 /results/raw_fastq/e19fd828/fastq > /results/align/9ffe3fbe/bam 2> /results/align/795995c2/log"
```

## File map

```
src/necroflow/
  dag.py        — Node, NodeState, NodeType, node_types, Inputs, Outputs, Constraints, Rules,
                  resolve_paths, resolve_command, write_dependencies, check_cache,
                  classify_nodes, _call_fingerprint, _folder_hash, _node_key,
                  _output_mtime, _accumulated_config
  pipeline.py   — _GraphBase (incl. save()), Pipeline, DAG, _sinks, _label, _render_connector
  executor.py   — execute (accepts any _GraphBase), _run_node, _node_threads;
                  parent-normalisation step before classify_nodes (see note below)
  state_db.py   — StateDB: SQLite persistence of run state in outdir/.rip/state.db
  logger.py     — thread-safe logging: job_start/done/failed/error/output, summary
  grid.py       — TOML __grid expansion (vendored from snakemakeconfigs) + iter_configs()
  cli.py        — necroflow CLI entry point; _load_factory, _create_link_outputs
  __init__.py   — exports all public symbols

examples/
  simple_dag.py             — linear + diamond pipelines; registration, path resolution, command resolution
  necroalchemy.py           — 17-node silly text-transform pipeline; multi-word DAG; uses .save()
  necroalchemy_factory.py   — CLI factory for necroalchemy; import-safe (CLI adds examples/ to sys.path)
  necroalchemy_grid.toml    — parameter grid TOML (word × n); run with necroalchemy_factory.py

tests/
  test_classify_nodes.py — NodeState classification, co-output deduplication, stale propagation,
                           command-change cache invalidation
  test_keep_going.py     — keep_going=True: independent branches, failure propagation, ExceptionGroup
  test_state_db.py       — StateDB unit tests + crash/fail/interrupt integration tests
```

### `dependencies.toml` — per-output provenance (`src/necroflow/dag.py`)

Each output folder gets a `dependencies.toml` recording the flat accumulated config from all
ancestors. The filesystem is the database; no SQLite/LMDB needed.

```toml
rule = "sort_bam"
hash = "4fb08953"

[config]
path = "/data/sample.fastq.gz"
ref = "hg38"
```

```python
check_cache(node)         # True if node.path + dependencies.toml both exist
write_dependencies(node)  # write after job succeeds
```

`_accumulated_config(node)` traverses strictly upward (ancestors only); assumes config key names
are unique across the pipeline.

### Parent normalisation in executor (`src/necroflow/executor.py`)

After DAG deduplication, unique nodes can hold parent references to superseded
objects (from other pipelines) that are never classified, leaving them with
`state=None`. The executor remaps all parent pointers to canonical nodes before
calling `classify_nodes`, so the READY-promotion check `all(p.state == UP_TO_DATE)` sees
consistent state:

```python
canonical = {_node_key(n): n for n in nodes}
for n in nodes:
    n.parents[:] = [canonical.get(_node_key(p), p) for p in n.parents]
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
  [--threads 16] [--keep-going] [--link-outputs]
```

- `--pipeline FILE:FUNC` — loads `FILE`, imports `FUNC(cfg: dict) -> Pipeline`
- `--config`/`-c` — repeatable; each TOML is expanded with `iter_configs()`; all
  pipelines share one DAG (shared upstream nodes deduplicated automatically)
- `--link-outputs` — after execution, creates `outdir/{combo_label}/` with symlinks
  into the hash tree + `manifest.toml` listing sink output paths

Key internals:
- `_load_factory(spec)` — splits `"file.py:func"`, loads with `importlib.util`,
  inserts `file.parent` into `sys.path` so relative imports in the factory work
- `_create_link_outputs(outdir, combos)` — relative symlinks preserving
  `{rule}/{hash}/{file}` structure; skips nodes with no path or non-existent output
- `_sinks(P)` — nodes with no children in the pipeline (leaf nodes)

## What is NOT yet implemented

- Scatter/gather within a single pipeline (fan-out over lists of inputs)
- Cluster/cloud backends
- Deletion of Orphan outputs (state is classified but no action taken)
- Long-range edges in the ASCII renderer (edges skipping layers are omitted)
