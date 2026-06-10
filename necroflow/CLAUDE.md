# necroflow

Python pipeline framework inspired by Snakemake. Rules are plain Python functions; the framework owns path generation and DAG construction. Execution engine not yet implemented.

## Setup

```bash
make venv          # creates .venv with Python 3.14 via uv, installs package editable
source .venv/bin/activate
```

## Core concepts

### Node (`src/necroflow/dag.py`)

Single class representing a pipeline value — a promise of a future output path. Every node produced by a rule automatically gets:

- `.rule` — the wrapper callable that produced it (carries `.resources` and `.__name__`)
- `.parents` — list of input `Node`s
- `.config` — dict of keyword args passed at call time
- `.output_name` — optional string label (e.g. `"bam"`, `"log"`)

### `@rule` decorator (`src/necroflow/dag.py`)

Turns a Python function into a DAG rule. When called, it intercepts execution and returns `Node` objects instead of running the function body (execution engine is future work).

```python
@rule                        # single output
def sort_bam(bam: Node):
    return Node()

@rule(threads=4)             # with scheduler resources
def align(fastq: Node, *, ref):
    return Node("bam"), Node("log")   # multi-output
```

**Call style:** positional args = parent `Node`s; all kwargs = per-call config.

```python
bam, log = align(fastq, ref="hg38")
# bam.config  == {"ref": "hg38"}
# bam.rule.resources == {"threads": 4}
```

**Validation:** positional args annotated `Node` are checked at call time — passing a non-`Node` where `Node` is annotated (or vice versa) raises `TypeError`.

**Resources** (`@rule(threads=4, memory="8G")`) are fixed at decoration time and accessible via `node.rule.resources`. They are intended for future scheduler integration.

### `Pipeline` (`src/necroflow/pipeline.py`)

Context manager that auto-registers every node produced inside the `with` block via a `contextvars.ContextVar`. No manual registration needed.

```python
pipeline = Pipeline()
with pipeline:
    fastq = raw_fastq(path="/data/sample.fastq.gz")
    bam, log = align(fastq, ref="hg38")
    counts, qc = quantify(bam, gene_model="gencode_v44")

print(pipeline)        # terminal box-drawing DAG
pipeline.plot()        # matplotlib figure
```

`Pipeline.nodes` holds all nodes in creation order.

#### Terminal rendering (`print(pipeline)`)

Layered ASCII DAG using Unicode box-drawing characters. Nodes are rendered as labelled boxes grouped by topological depth. Edges are routed with proper junction chars (`┴ ┬ └ ┘ ┼`) handling fan-out, fan-in, and diamond patterns. Already-visited nodes in a diamond are shown with a back-reference (`↑ node (see above)`).

#### Matplotlib rendering (`pipeline.plot()`)

Uses `networkx` + `matplotlib`. Nodes laid out by topological layer.

## File map

```
src/necroflow/
  dag.py        — Node, @rule decorator
  pipeline.py   — Pipeline, _render_connector, _BOX junction map
  __init__.py   — exports: Node, rule, Pipeline

examples/
  simple_dag.py — linear pipeline + diamond pipeline examples
```

## What is NOT yet implemented

- Path/ID generation (content-addressed hashing, SQLite storage)
- Rule execution engine
- Scheduler integration (resources are stored but not used)
- Target reuse / auto-deletion of stale outputs
