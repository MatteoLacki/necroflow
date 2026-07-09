[![CI](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/necroflow)](https://pypi.org/project/necroflow/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

# necroflow

<p align="center"><img src="images/logo.png" width="200" alt="necroflow logo"></p>

Python pipeline framework inspired by Snakemake. Define rules, wire them into pipelines, run with automatic parallelism and caching.

A local browser GUI for visualising pipelines and launching runs is available at [necroflow_gui](https://github.com/MatteoLacki/necroflow_gui).

See [COMPARISON.md](COMPARISON.md) for a detailed comparison with Snakemake, Nextflow, Luigi, CWL/WDL, and Prefect/Airflow across 20 axes.

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
from necroflow import NodeType, Rules, Pipeline, DAG, Inputs, Outputs

# 1. Define types
class Fastq(NodeType):
    """Raw sequencing reads."""
    filename = "reads.fastq.gz"

class Bam(NodeType):
    """Aligned reads."""
    filename = "aligned.bam"

class Counts(NodeType):
    """Per-gene read counts."""
    filename = "counts.txt"

# 2. Register rules
r = Rules()

@r.command("ln -s {path} {fastq}")
def raw_fastq(path: str):
    """Symlink a raw FASTQ file into the output tree."""
    return Fastq[fastq]

@r.command("bwa mem {ref} {fastq} > {bam}", threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads to a reference genome with BWA-MEM."""
    return Bam[bam]

@r.command("featureCounts -a {gene_model} {bam} -o {counts}")
def count(bam: Bam, gene_model: str):
    """Count reads per gene using featureCounts."""
    return Counts[counts]

# 3. Build a pipeline
def rna_pipeline(config, r):
    P = Pipeline()
    P.fastq = r.raw_fastq(path=config.path)
    P.bam = r.align(P.fastq, ref=config.ref)
    P.counts = r.count(P.bam, gene_model=config.gene_model)
    return P
```

The same rule can also be registered without decorators through the explicit API:

```python
r.register(
    "count",
    Inputs(bam=Bam, gene_model=str),
    Outputs(counts=Counts),
    "featureCounts -a {gene_model} {bam} -o {counts}",
)
```

## Running one sample

`DAG("results")` sets the output directory where all computed files will be written (you can use any path you like).

```python
from types import SimpleNamespace

config = SimpleNamespace(path="/data/s1.fastq.gz", ref="hg38", gene_model="gencode_v44")
dag = DAG("results")           # output directory — change to any writable path
dag.add(rna_pipeline(config, r))
dag.execute()
```

## Running many samples

```python
configs = [
    SimpleNamespace(path="/data/s1.fastq.gz", ref="hg38", gene_model="gencode_v44"),
    SimpleNamespace(path="/data/s2.fastq.gz", ref="hg38", gene_model="gencode_v44"),
    SimpleNamespace(path="/data/s3.fastq.gz", ref="hg38", gene_model="gencode_v44"),
]

dag = DAG("results")
for config in configs:
    dag.add(rna_pipeline(config, r))

dag.execute()   # runs all samples in parallel, skips any already-computed outputs
```

Nodes with identical upstream configs (e.g. a shared reference index) are deduplicated across samples — recognised by hash, run once.

## Where outputs live

`DAG("some-dir")` writes real content-addressed node outputs directly under that directory. The CLI defaults to a split layout: real cached outputs under `nodes/`, plus per-job symlink folders and `manifest.toml` files under `results/`. See [Where outputs live and caching](docs/caching.md) for the full layout.


## Manual

- [Where outputs live and caching](docs/caching.md)
- [Command-line interface](docs/cli.md)
- [Job TOML and parameter grids](docs/job-toml.md)
- [Config validation](docs/config-validation.md)
- [Rules and typed outputs](docs/rules.md)
- [Generated config files](docs/generated-config-files.md)
- [Execution, scheduling, and cleanup](docs/execution.md)

## What is not yet implemented

- Cluster / cloud backends
