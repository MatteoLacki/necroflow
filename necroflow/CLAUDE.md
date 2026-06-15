# necroflow

Python pipeline framework inspired by Snakemake. Rules are registered via a `Rules` container;
the framework owns path generation, DAG construction, and execution.

## Setup

```bash
make venv          # creates .venv with Python 3.14 via uv, installs package editable
source .venv/bin/activate
```

## Core concepts

### Executor (`src/necroflow/executor.py`)

```python
execute(pipeline, outdir, total_threads=None)
```

Calls `pipeline.resolve_paths(outdir)` internally, then runs all nodes:

- Nodes are scheduled in topological order (`pipeline.nodes` is topological by construction)
- Parallel execution via `concurrent.futures.ThreadPoolExecutor`
- Thread budget: sum of `constraints.threads` across running jobs ≤ `total_threads` (default `os.cpu_count()`)
- A job whose thread requirement exceeds the budget runs solo when nothing else is running
- Cache hits (`check_cache`) are skipped instantly before the loop starts
- `write_dependencies(node)` called after each successful job
- Raises `subprocess.CalledProcessError` on the first failure

```python
execute(P, "/results")               # use all CPUs
execute(P, "/results", total_threads=8)
```

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

### `Pipeline` (`src/necroflow/pipeline.py`)

Attribute-style node registration. Assigning a Node (or named tuple of Nodes) auto-registers it.

```python
def basic_pipeline(config, R):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=config.path)
    P.bam, P.align_log = R.align(P.fastq, ref=config.ref)
    P.sorted_bam = R.sort_bam(P.bam)
    P.counts, P.qc = R.quantify(P.sorted_bam, gene_model=config.gene_model)
    return P
```

`P.nodes` — all nodes in registration order. Duplicate attribute name → `ValueError`.

#### Terminal rendering (`print(P)`)

Layered ASCII DAG with Unicode box-drawing characters, grouped by topological depth.

#### Matplotlib rendering (`P.plot()`)

Uses `networkx` + `matplotlib`. Nodes laid out by topological layer.

### Path generation (`src/necroflow/dag.py`)

```python
P.resolve_paths("/results")
# sets node.path = /results/{rule_name}/{hash8}/{output_name}
```

The 8-char hash is derived from the full ancestor config chain (all rule names, output names,
configs, and parent fingerprints recursively). Deterministic: same DAG + same root inputs →
same paths. Different root inputs → different hash → different path → cache miss.

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
  dag.py        — Node, NodeType, node_types, Inputs, Outputs, Constraints, Rules,
                  resolve_paths, resolve_command, write_dependencies, check_cache,
                  _call_fingerprint, _node_hash, _accumulated_config
  pipeline.py   — Pipeline, _render_connector, _BOX junction map
  executor.py   — execute, _run_node, _node_threads
  __init__.py   — exports all public symbols

examples/
  simple_dag.py — linear pipeline + diamond pipeline; shows registration, path resolution,
                  command resolution, check_cache / write_dependencies usage
```

### `dependencies.toml` — per-output provenance (`src/necroflow/dag.py`)

Each output folder gets a `dependencies.toml` recording the flat accumulated config from all
ancestors. The filesystem is the database; no SQLite/LMDB needed.

```toml
rule = "sort_bam"
output_name = "sorted_bam"
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

## What is NOT yet implemented

- Scatter/gather (fan-out over lists of inputs)
- Smart cache invalidation: skip tasks when nothing upstream has changed (criterion TBD — mutable input files are the open question)
- Cluster/cloud backends
- Retry / failure handling
