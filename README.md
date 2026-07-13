[![CI](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml)
[![CI platforms](https://img.shields.io/badge/CI-Linux%20%7C%20macOS-blue)](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml)
[![CI Python](https://img.shields.io/badge/CI%20Python-3.10--3.15-blue)](https://github.com/MatteoLacki/necroflow/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/necroflow)](https://pypi.org/project/necroflow/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

# necroflow

<p align="center"><img src="images/logo.png" width="200" alt="necroflow logo"></p>

Python pipeline framework inspired by Snakemake. Define rules, wire them into pipelines, run with automatic parallelism and caching. All in Python. All safe. All readable.

For a compact overview of the current software surface, see [features.txt](features.txt).

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

## Define a pipeline

A command-line run points at a Python pipeline factory. Rules describe typed outputs and shell commands; the factory wires rule calls into a pipeline.

```python
# pipeline.py
from necroflow import NodeType, Rules, Pipeline

class Fastq(NodeType):
    filename = "reads.fastq.gz"

class Bam(NodeType):
    filename = "aligned.bam"

class Counts(NodeType):
    filename = "counts.txt"

r = Rules()

@r.command("ln -s {path} {fastq}")
def raw_fastq(path: str):
    return Fastq[fastq]

@r.command("bwa mem {ref} {fastq} > {bam}", threads=4)
def align(fastq: Fastq, ref: str):
    return Bam[bam]

@r.command("featureCounts -a {gene_model} {bam} -o {counts}")
def count(bam: Bam, gene_model: str):
    return Counts[counts]

def rna_pipeline(config):
    P = Pipeline()
    P.fastq = r.raw_fastq(path=config["path"])
    P.bam = r.align(P.fastq, ref=config["ref"])
    P.counts = r.count(P.bam, gene_model=config["gene_model"])
    return P
```

## Run from the CLI

Create a job TOML that references the factory and carries the concrete parameters for one run.

```toml
# job.toml
".pipeline" = "pipeline.py:rna_pipeline" # from pipeline import rna_pipeline

path = "/data/s1.fastq.gz"
ref = "hg38"
gene_model = "gencode_v44"
```

Run it with the `necroflow` command:

```bash
necroflow job.toml
```

By default, real cached node outputs go under `nodes/`, while user-facing results and `manifest.toml` go under `results/`; above, simply `results/job`. Use explicit roots when you want them elsewhere:

```bash
necroflow --nodes-dir nodes --results-dir results job.toml
```

For many runs, use multiple job TOMLs or `__grid` values inside one job TOML:

```toml
".pipeline" = "pipeline.py:rna_pipeline"

path__grid = ["/data/s1.fastq.gz", "/data/s2.fastq.gz"]
ref = "hg38"
gene_model = "gencode_v44"
```

The same pipeline can also be assembled and executed from Python directly; see [Rules and typed outputs](docs/rules.md) and [Execution, scheduling, and cleanup](docs/execution.md). See [Command-line interface](docs/cli.md) and [Job TOML and parameter grids](docs/job-toml.md) for the full CLI format.

## Where outputs live

`DAG("some-dir")` writes real content-addressed node outputs directly under that directory. The CLI defaults to a split layout: real cached outputs under `nodes/`, plus per-job symlink folders and `manifest.toml` files under `results/`. See [Where outputs live and caching](docs/caching.md) for the full layout.


## Manual

Start with the canonical workflow in [examples/canonical](examples/canonical/), or copy it with `necroflow init my-workflow`.

### CLI subcommands

The default command form is kept for convenience, but the same run can be written explicitly:

```bash
necroflow run job.toml
```

This executes the requested pipeline and creates cached outputs under `nodes/` plus job-facing links and a manifest under `results/job/`.

Create a starter workflow from the canonical template:

```bash
necroflow init my-workflow
```

Example output:

```text
created my-workflow
```

Render the requested DAG without executing commands:

```bash
necroflow graph job.toml
```

Example output, abridged:

```text
DAG  4 nodes  (1 required)

import_text[RawText:raw_text] (path='input.txt')
write_tool_config[ToolConfig:tool_config] (text='{\n  "mode": "uppercase"\n}\n')
process_text[ProcessedText:processed_text]
summarize[Summary:summary] *
```

List requested output paths without executing commands:

```bash
necroflow outputs job.toml
```

Example output:

```text
[job]
summary	node=nodes/summarize/d18e6af2070f14be/summary.txt	result=results/job/summary/summary.txt
```

Inspect stored metadata for an existing cached output:

```bash
necroflow provenance nodes/summarize/d18e6af2070f14be/summary.txt
```

Example output:

```text
path = nodes/summarize/d18e6af2070f14be/summary.txt
rule = summarize
hash = d18e6af2070f14be
[config]
path = 'input.txt'
text = '{\n  "mode": "uppercase"\n}\n'
```

- [Where outputs live and caching](docs/caching.md)
- [Command-line interface](docs/cli.md)
- [Job TOML and parameter grids](docs/job-toml.md)
- [Config validation](docs/config-validation.md)
- [Rules and typed outputs](docs/rules.md)
- [Generated config files](docs/generated-config-files.md)
- [Execution, scheduling, and cleanup](docs/execution.md)
- [Release checklist](docs/release.md)
- [Development](docs/development.md)

## What is not yet implemented

- Cluster / cloud backends
