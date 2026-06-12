# necroflow

Python pipeline framework inspired by Snakemake. Rules are plain Python functions; the framework owns path generation and DAG construction. Execution engine not yet implemented.

## Setup

```bash
make venv          # creates .venv with Python 3.14 via uv, installs package editable
source .venv/bin/activate
```

## Core concepts

### NodeType (`src/necroflow/dag.py`)

Lightweight base class for node types. Uses a metaclass so that calling the class creates a `Node`, not a NodeType instance.

```python
class Fastq(NodeType): ...          # single type
class SortedBam(Bam): ...           # subtype — accepted wherever Bam expected

Fastq, Bam, Log = node_types("fastq bam log")  # bulk creation (dynamic subclasses)

Fastq("output_name")   # → Node(output_name="output_name", node_type=Fastq)
```

`node.node_type` is the class itself (e.g. `Fastq`), not an instance.  
Inheritance is via normal Python class hierarchy; subtype check uses `issubclass`.

### Node (`src/necroflow/dag.py`)

Single dataclass representing a pipeline value — a promise of a future output path. Fields:

- `.rule` — wrapper callable that produced it (carries `.resources`, `.__name__`)
- `.parents` — list of input `Node`s
- `.config` — dict of keyword args passed at call time
- `.output_name` — optional string label (e.g. `"bam"`, `"log"`)
- `.node_type` — NodeType subclass (the class object, not an instance)

### `@rule` decorator (`src/necroflow/dag.py`)

Turns a Python function into a DAG rule. When called, intercepts execution and returns `Node`s instead of running the function body.

```python
@rule                                    # single output
def sort_bam(bam: Bam) -> Node:
    return SortedBam()

@rule(threads=4)                         # with scheduler resources
def align(fastq: Fastq, *, ref: str):
    return Bam("bam"), Log("log")        # multi-output
```

**Call style:** positional args = parent `Node`s; keyword-only args = per-call config.

```python
bam, log = align(fastq, ref="hg38")
# bam.config  == {"ref": "hg38"}
# bam.rule.resources == {"threads": 4}
```

**Validation — decoration time:**
- All parameters (positional and keyword-only) must have type annotations. Missing annotation → `TypeError`.

**Validation — call time:**
- Positional arg annotated with a NodeType subclass: value must be a `Node` whose `node_type` is that class or a subclass (`issubclass`).
- Positional arg annotated with non-NodeType, but value is a `Node` → `TypeError`.
- Positional `Node` value with non-NodeType annotation → `TypeError`.
- Keyword-only args: `isinstance(val, annotation)` check (supports `str | int` union types; complex generics skipped silently).

**Resources** (`@rule(threads=4, memory="8G")`) fixed at decoration time, accessible via `node.rule.resources`. Intended for future scheduler integration.

### `Pipeline` (`src/necroflow/pipeline.py`)

Attribute-style registration — assigning to a pipeline attribute auto-registers nodes.

```python
P = Pipeline()
P.fastq = raw_fastq(path="/data/sample.fastq.gz")
P.bam, P.align_log = align(P.fastq, ref="hg38")
P.sorted_bam = sort_bam(P.bam)

print(P)        # terminal box-drawing DAG
P.plot()        # matplotlib figure
```

`P.nodes` — all nodes in registration order.  
Duplicate attribute name → `ValueError`.

#### Terminal rendering (`print(pipeline)`)

Layered ASCII DAG using Unicode box-drawing characters. Nodes rendered as labelled boxes grouped by topological depth. Edges routed with proper junction chars (`┴ ┬ └ ┘ ┼`) handling fan-out, fan-in, and diamond patterns.

#### Matplotlib rendering (`pipeline.plot()`)

Uses `networkx` + `matplotlib`. Nodes laid out by topological layer.

## File map

```
src/necroflow/
  dag.py        — Node, NodeType, node_types, @rule decorator
  pipeline.py   — Pipeline, _render_connector, _BOX junction map
  __init__.py   — exports: Node, NodeType, node_types, rule, Pipeline

examples/
  simple_dag.py — linear pipeline + diamond pipeline (shows class inheritance)
```

## What is NOT yet implemented

- Path/ID generation (content-addressed hashing, SQLite storage)
- Rule execution engine
- Scheduler integration (resources are stored but not used)
- Target reuse / auto-deletion of stale outputs
