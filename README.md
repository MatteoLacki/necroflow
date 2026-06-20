# necroflow

`necroflow` is a small Python pipeline framework inspired by Snakemake. It lets you define typed rules, wire them into reusable pipelines, aggregate many pipelines into one DAG, and execute only the requested target nodes with filesystem-backed caching.

This repository currently contains two Python packages:

- `necroflow/` - the core pipeline framework.
- `necroflow_gui/` - a local, Python-only browser GUI for visualizing pipelines, selecting target nodes, and launching necroflow runs.

## Core Ideas

- **Rules** declare typed inputs, typed outputs, shell commands, and optional resource constraints.
- **Pipelines** are Python functions that create a `Pipeline` and assign rule outputs to attributes.
- **DAGs** collect one or more pipelines and register the target nodes to calculate.
- **Caching** is based on content-addressed output paths. Re-running the same inputs skips completed outputs.
- **The GUI** renders pipeline graphs as clickable SVG and calls `DAG.add(pipeline, request=[...])` with the selected target nodes.

## Quick Example

```python
from types import SimpleNamespace
from necroflow import node_types, Inputs, Outputs, Rules, Pipeline, DAG

Fastq, Bam, Counts = node_types("fastq=reads.fastq.gz bam=aligned.bam counts=counts.txt")

R = Rules()
R.register("raw_fastq", Inputs(path=str), Outputs(fastq=Fastq), "ln -s {path} {fastq}")
R.register("align", Inputs(fastq=Fastq, ref=str), Outputs(bam=Bam), "bwa mem {ref} {fastq} > {bam}")
R.register("count", Inputs(bam=Bam), Outputs(counts=Counts), "wc -l {bam} > {counts}")


def pipeline(config, rules):
    p = Pipeline()
    p.fastq = rules.raw_fastq(path=config.path)
    p.bam = rules.align(p.fastq, ref=config.ref)
    p.counts = rules.count(p.bam)
    return p

config = SimpleNamespace(path="/data/sample.fastq.gz", ref="hg38")
dag = DAG("/results")
p = pipeline(config, R)
dag.add(p, request=[p.counts])
dag.execute()
```

## Running the Core Examples

```bash
cd necroflow
python examples/simple_dag.py
python examples/necroalchemy.py
```

`examples/necroalchemy.py` is a larger text-processing DAG that is useful for visual inspection and scheduler behavior.

## Running the GUI

The GUI is intentionally Python-only: it uses the standard library HTTP server and server-generated SVG, with no JavaScript requirement.

```bash
PYTHONPATH=necroflow/src:necroflow_gui/src python3 -m necroflow_gui.cli.main serve
```

Then open:

```text
http://127.0.0.1:8000/
```

The bundled GUI examples include:

- Basic RNA pipeline
- Diamond variant pipeline
- Necroalchemy text pipeline

By default, sink nodes in the graph are selected as requested outputs. Click nodes to toggle target selection, then run the selected targets.

## Project Registry for the GUI

A project can provide its own registry module exposing `PIPELINES`:

```bash
necroflow-gui serve path.to.registry:PIPELINES
necroflow-gui serve ./my_registry.py:PIPELINES
```

Each entry should be a `necroflow_gui.registry.PipelineSpec` with configs, rules, a pipeline builder, and an output directory.

## Repository Layout

```text
necroflow/
  src/necroflow/          core rules, DAG, executor, state DB, logging
  examples/               example pipelines
  tests/                  core tests

necroflow_gui/
  src/necroflow_gui/      local GUI, graph rendering, example registry
  tests/                  GUI tests
```

## Status

This is an early-stage project. The core behavior is intentionally small and Pythonic; the GUI is a local developer tool for inspecting and launching pipeline requests.
