# necroflow

<p align="center"><img src="images/logo.png" width="200" alt="necroflow logo"></p>

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
from necroflow import node_types, Inputs, Outputs, Constraints, Rules, Pipeline, DAG

# 1. Define types
Fastq, Bam, Counts = node_types("fastq=reads.fastq.gz bam=aligned.bam counts=counts.txt")

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
print(P)             # layered ASCII DAG
P.plot()             # matplotlib figure

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

## What is not yet implemented

- Scatter/gather (fan-out over lists of inputs)
- Smart cache invalidation based on upstream file modification times
- Cluster / cloud backends
- Retry and partial failure handling
