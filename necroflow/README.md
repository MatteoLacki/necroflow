# necroflow

<p align="center"><img src="images/logo.png" width="200" alt="necroflow logo"></p>

[![CI](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml/badge.svg)](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml)

Python pipeline framework inspired by Snakemake. Define rules, wire them into pipelines, run with automatic parallelism and caching.

## Core ideas

- **Rules** describe how to produce outputs from inputs — shell command templates with typed I/O.
- **Pipelines** wire rule calls together for a single config.
- **DAG** runs many pipelines at once, deduplicating shared upstream work across samples automatically.
- **Paths** are derived from a content-addressed hash of the full input chain — same inputs always produce the same path, different inputs produce different paths. The filesystem is the cache.

## Install

```bash
cd necroflow
make venv
source .venv/bin/activate
```

## Quick example

```python
from necroflow import NodeType, Inputs, Outputs, Constraints, Rules, Pipeline, DAG

# 1. Define types
class Fastq(NodeType):
    name = "reads.fastq.gz"

class Bam(NodeType):
    name = "aligned.bam"

class Counts(NodeType):
    name = "counts.txt"

# 2. Register rules
R = Rules()

R.register("raw_fastq",
    Inputs(path=str),
    Outputs(fastq=Fastq),
    "ln -s {path} {fastq}")

R.register("align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam),
    "bwa mem {ref} {fastq} > {bam}",
    Constraints(threads=4))

R.register("count",
    Inputs(bam=Bam, gene_model=str),
    Outputs(counts=Counts),
    "featureCounts -a {gene_model} {bam} -o {counts}")

# 3. Build a pipeline
def rna_pipeline(config, R):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=config.path)
    P.bam = R.align(P.fastq, ref=config.ref)
    P.counts = R.count(P.bam, gene_model=config.gene_model)
    return P
```

## Running one sample

```python
from types import SimpleNamespace

config = SimpleNamespace(path="/data/s1.fastq.gz", ref="hg38", gene_model="gencode_v44")
dag = DAG("/results")
dag.add(rna_pipeline(config, R))
dag.execute()
```

## Running many samples

```python
configs = [
    SimpleNamespace(path="/data/s1.fastq.gz", ref="hg38", gene_model="gencode_v44"),
    SimpleNamespace(path="/data/s2.fastq.gz", ref="hg38", gene_model="gencode_v44"),
    SimpleNamespace(path="/data/s3.fastq.gz", ref="hg38", gene_model="gencode_v44"),
]

dag = DAG("/results")
for config in configs:
    dag.add(rna_pipeline(config, R))

dag.execute()   # runs all samples in parallel, skips any already-computed outputs
```

Nodes with identical upstream configs (e.g. a shared reference index) are deduplicated across samples — recognised by hash, run once.

## Inspecting a pipeline

```python
from necroflow import resolve_command

P = rna_pipeline(config, R)
print(P)                    # layered ASCII DAG to stdout
P.save("pipeline.txt")      # same render to a file
P.plot()                    # matplotlib figure

dag.save("dag.txt")         # works on DAG too

P.resolve_paths("/results")
for node in P.nodes:
    print(resolve_command(node))   # fully-resolved shell command
```

## Caching

Each output lives at `outdir/{rule}/{hash8}/{filename}`. The hash captures the entire upstream config chain, so:

- Re-running with the same inputs is a no-op (cache hit).
- Changing any upstream parameter produces a new path — old results are never overwritten.
- A `dependencies.toml` next to each output records the full accumulated config for provenance.

## Parallelism and scheduling

`execute()` runs nodes in parallel up to the available CPU count (or `total_threads=N`). Thread budgets from `Constraints(threads=N)` are respected.

By default the scheduler prioritises nodes from the **smallest connected component** of remaining work — this tends to finish whole samples before starting new ones, keeping memory pressure low.

```python
from necroflow import fifo_scheduler

dag.execute(total_threads=16, scheduler=fifo_scheduler)  # topological order instead
```

Custom schedulers:

```python
def my_scheduler(ready, remaining):
    # return ready nodes in desired priority order
    return sorted(ready, key=lambda n: n.rule.constraints.get("threads", 1), reverse=True)

dag.execute(scheduler=my_scheduler)
```

## Types and subtypes

NodeTypes form an inheritance hierarchy — a rule accepting `Bam` also accepts `SortedBam`:

```python
class SortedBam(Bam):
    name = "sorted.bam"   # filename within the rule output directory

R.register("sort",
    Inputs(bam=Bam),
    Outputs(sorted_bam=SortedBam),
    "samtools sort {bam} -o {sorted_bam}")

R.register("quantify",
    Inputs(bam=SortedBam, gene_model=str),  # only accepts sorted bam
    ...)
```

## Failure handling

```python
dag.execute(keep_going=True)   # continue independent branches past failures
```

With `keep_going=False` (default) the first failure raises immediately. With `keep_going=True` independent branches keep running and all failures are collected into an `ExceptionGroup` at the end.

After each successful job, necroflow verifies that the declared output file exists. A command that exits 0 but writes no output is treated as a failure.

Run state is persisted to `outdir/.rip/state.db` (SQLite) between invocations. A node whose output exists on disk but whose previous run was interrupted by a signal or left in an unknown state is automatically re-executed next time.

Each job's stdout/stderr is captured to `outdir/{rule}/{hash}/{job.log}`. On failure the log is printed to the terminal.

## Multi-output rules

A rule with multiple declared outputs runs its command **once**; all co-outputs are marked complete when the command finishes:

```python
R.register("align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam, log=Log),      # one command, two outputs
    "bwa mem {ref} {fastq} > {bam} 2> {log}",
    Constraints(threads=4))

bam, log = R.align(fastq_node, ref="hg38")
```

## Cleaning orphan outputs

Outputs that existed from a previous run but are no longer in the required subgraph are classified as `ORPHAN`. Pass `autoclean=True` to delete them per-file (files via `unlink`, directories via `rmtree`):

```python
dag.execute(autoclean=True)
```

Or via CLI:

```bash
necroflow --pipeline factory.py:factory --config exp.toml --outdir /results --autoclean
```

## Command-line interface

necroflow ships a `necroflow` command that runs pipelines from TOML configs.

```bash
necroflow \
  --pipeline path/to/factory.py:function_name \
  --config   experiment.toml \
  --outdir   /results \
  [--threads 16] [--keep-going] [--autoclean]
```

`--pipeline` points to a Python file and names a factory function inside it.
The function receives a plain `dict` (the parsed config) and must return a `Pipeline`.

`--config` / `-c` may be repeated; all configs feed into the same DAG so shared
upstream nodes are deduplicated across configs automatically.

### Parameter grids

Any TOML key ending in `__grid` is expanded into a Cartesian product of all
combinations. The resulting output subfolders use the same naming scheme as
[snakemakeconfigs](https://github.com/MatteoLacki/snakemakeconfigs).

```toml
# experiment.toml
ref__grid    = ["hg38", "mm10"]
aligner__grid = ["bwa", "bowtie2"]
```

This produces four pipelines: `experiment__ref+hg38__aligner+bwa`,
`experiment__ref+hg38__aligner+bowtie2`, etc.

The factory function:

```python
# factory.py
from my_pipeline import rna_pipeline   # imported from the same directory

def factory(cfg: dict):
    return rna_pipeline(ref=cfg["ref"], aligner=cfg["aligner"])
```

### Linked outputs

After every run the CLI creates one subfolder per grid combo under `outdir/`:

```
/results/
  {rule}/{hash}/{file}           ← real outputs (content-addressed)
  experiment__ref+hg38__aligner+bwa/
    {rule}/{hash}/{file}         ← symlinks into the hash tree
    manifest.toml                ← sink output paths for this combo
  experiment__ref+hg38__aligner+bowtie2/
    ...
```

The symlink tree mirrors the hash structure; `manifest.toml` lists only the sink
(requested) outputs, keyed by the Pipeline attribute name assigned in the factory:

```toml
[outputs]
counts = "count/a3f1bc92/counts.txt"
```

The key (`counts`) matches `P.counts = R.count(...)` in the factory function.

See `examples/necroalchemy_grid.toml` and `examples/necroalchemy_factory.py`
for a runnable example.

## What is not yet implemented

- Scatter/gather within a single pipeline (fan-out over lists of inputs)
- Cluster / cloud backends
