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

`DAG("some-dir")` writes the real content-addressed node outputs directly under that directory. The CLI defaults to a split layout: hashed node outputs under `nodes/`, plus one user-facing subfolder per job/grid combo under `results/`:

```
nodes/
  {rule}/{hash16}/{file}           ← real node outputs (content-addressed)

results/
  experiment__ref+hg38__aligner+bwa/
    {rule}/{hash16}/{file}         ← symlinks to requested node outputs only
    manifest.toml                  ← requested output paths for this combo
  experiment__ref+hg38__aligner+bowtie2/
    ...
```

Only the **requested** outputs (defaults to pipeline sinks) get a symlink — intermediate ancestors are excluded. `manifest.toml` lists the same outputs keyed by the Pipeline attribute name assigned in the factory, with paths relative to the node store:

```toml
[outputs]
counts = "count/a3f1bc92/counts.txt"
```

The key (`counts`) matches `P.counts = R.count(...)` in the factory function.

See `examples/necroalchemy_grid.toml` and `examples/necroalchemy_factory.py`
for a runnable example.

## Command-line interface

necroflow ships a `necroflow` command. Each positional argument is a **job TOML** — a self-contained file that specifies the pipeline factory, optional requested outputs, and user config params.

```bash
necroflow [--nodes-dir nodes] [--results-dir results] [-c N|all] \
          [--constraint KEY=VALUE ...] [--keep-going] [--autoclean] [--dry-run] \
          [--invalidate LABEL ...] [--reap NAME ...] [--reap-file PATH] \
          [--validation PATH.py:FUNCTION ...] \
          JOB.toml [JOB2.toml ...]
```

| Flag | Meaning |
|---|---|
| `--nodes-dir DIR` | Hashed node output store (default: `nodes`). |
| `--results-dir DIR` | Per-job symlink and manifest directory (default: `results`). |
| `--outdir DIR` / `-o DIR` | Compatibility alias that uses one directory for both node outputs and job links. Cannot be combined with `--nodes-dir` or `--results-dir`. |
| `-c N` / `-call` | Thread cap — integer or `all` (default: all CPUs). |
| `--constraint KEY=VALUE` | Additional resource cap. Repeatable. Accepts SI/binary suffixes. |
| `--keep-going` / `-k` | Continue past failures; collect all errors at the end. |
| `--autoclean` | Delete orphan outputs and intermediate rule-call directories, including `{workdir}` side files. |
| `--dry-run` / `-n` | Show what would run without executing. |
| `--invalidate LABEL` | Force an already-requested pipeline label to rerun. Repeatable. |
| `--reap NAME` | Force labels listed under `NAME` in `reap.toml` to rerun. Repeatable. |
| `--reap-file PATH` | TOML file for named invalidation sets (default: `reap.toml`). |
| `--validation PATH.py:FUNCTION` | Validate each expanded job config with a Python callable. Repeatable. |

```bash
necroflow --invalidate counts job.toml
necroflow --reap quick --reap-file reap.toml job.toml
```

`--invalidate` and `--reap` do not override `.requests` and do not request extra outputs. They only mark matching labels stale when those labels are already in the active requested subgraph. A `reap.toml` file contains top-level named label lists:

```toml
quick = ["counts", "qc"]
```

### Job TOML format

```toml
# required — path resolved from the directory where necroflow is invoked
".pipeline" = "path/to/factory.py:function_name"

# optional — pipeline_label names to request (defaults to all sinks)
".requests" = ["counts", "qc"]

# user config — passed as a plain dict to the factory
ref    = "hg38"
sample = "NA12878"
```

Keys starting with `.` are necroflow metadata — stripped before the dict reaches the factory. They never appear in node configs or affect output hashes. User config can freely use any name, including `pipeline` or `request`.

### Config validation

Use `--validation path/to/schema.py:validate` to reject malformed job configs before pipeline construction. The callable receives the same plain config dict that the pipeline factory receives and should raise an exception on invalid input:

```python
def validate(config):
    if "sample" not in config:
        raise ValueError("missing required key: sample")
```

```bash
necroflow --validation schema.py:validate job.toml
```

`--validation` is repeatable and validators run in CLI order. Validation runs after `__grid` expansion and after stripping dot-prefixed necroflow metadata such as `.pipeline` and `.requests`. This callback mechanism is intentional: with `__grid`, the raw TOML file is not always the concrete config that a factory will receive, so validating the file ahead of time can miss or misreport errors in individual expanded combinations.

Python-only callers can use the same loader and validate in their own loop:

```python
from necroflow import iter_job_configs

def validate(config):
    if "sample" not in config:
        raise ValueError("missing required key: sample")

for job in iter_job_configs("job.toml"):
    validate(job.config)
    print(job.label, job.config)
```

Cerberus is available as an optional validation extra:

```bash
pip install "necroflow[validation]"
```

A validator can then load a Cerberus schema from TOML or JSON and apply it to the expanded config:

```python
import tomllib
from pathlib import Path
from cerberus import Validator

schema = tomllib.loads(Path("schema.toml").read_text())

def validate(config):
    validator = Validator(schema, allow_unknown=False)
    if not validator.validate(config):
        raise ValueError(validator.errors)
```

Cerberus handles structural checks well; branch-specific or cross-parameter domain rules can live in `check_with` hooks or in ordinary Python after the Cerberus check.

### Parameter grids

Any TOML key ending in `__grid` is expanded into a Cartesian product of all
combinations. The resulting output subfolders use the same naming scheme as
[snakemakeconfigs](https://github.com/MatteoLacki/snakemakeconfigs).

```toml
".pipeline"   = "factory.py:factory"
ref__grid     = ["hg38", "mm10"]
aligner__grid = ["bwa", "bowtie2"]
```

This produces four pipelines: `experiment__ref+hg38__aligner+bwa`,
`experiment__ref+hg38__aligner+bowtie2`, etc. Grid expansion also applies to
`pipeline` itself, so a single job TOML can fan out across different factory functions.

## Conditional pipelines

Pipeline factory functions are plain Python, so `if/else` branching on config values works naturally:

```python
def my_pipeline(config, R):
    P = Pipeline()
    P.a = R.align(path=config.path, ref=config.ref)
    if config.call_variants:
        P.result = R.call_snps(P.a)
    else:
        P.result = R.count_reads(P.a)
    return P
```

The branching config value (`config.call_variants`) does not need to be passed to any node. The rule name already encodes which branch was taken in the fingerprint, so `call_snps` and `count_reads` always produce distinct output paths regardless.

Two pipelines sharing the same upstream config (e.g. same `path` and `ref`) will reuse the `align` output — recognised as a cache hit — even if they take different branches downstream.

**Pipeline attribute names cannot be overwritten.** Assigning to the same name twice raises `ValueError`. If you want to build a pipeline in a loop, use distinct names:

```python
for i, step in enumerate(steps):
    setattr(P, f"result_{i}", R.process(step_node, mode=step))
```

The idiomatic pattern for multi-sample or multi-condition work is separate `Pipeline` objects added to a shared `DAG` — one pipeline per config, one `dag.add(P)` call per pipeline.

## Inspecting a pipeline

```python
from necroflow import resolve_command

P = rna_pipeline(config, R)
print(P)                    # layered ASCII DAG to stdout
P.save("pipeline.txt")      # same render to a file

dag.save("dag.txt")         # works on DAG too

P.resolve_paths("results")
for node in P.nodes:
    print(resolve_command(node))   # fully-resolved shell command
```

## Caching

Each hashed node output lives at `nodes/{rule}/{hash16}/{filename}` by default. The 16-character hash captures the entire upstream config chain, including rule name, command, config values, parent fingerprints, and declared `Inputs`/`Outputs` types (`Constraints` are excluded — execution resources don't affect output identity).

During path resolution, necroflow validates generated paths against the filesystem's `NAME_MAX` and `PATH_MAX` limits. If a rule name, filename, output directory, or complete generated path would exceed those limits, path resolution fails before execution starts.

### Rule work directories

Commands may use the built-in `{workdir}` placeholder to refer to the rule-call output directory: `nodes/{rule}/{hash16}` by default. Use it for tools that need to write a directory of side files or temporary computation products that should live next to the declared outputs:

```python
@r.command("dosomething --tmp {workdir}/scratch -o {result}")
def compute(input: Input):
    return Result[result]
```

The `{workdir}` directory is created before the command runs. Files written there are kept by default, just like declared outputs, because the directory is part of the cached rule-call result. The name `workdir` is reserved for this built-in placeholder and cannot be used as an input or output name.

Command template validation is intentionally strict about outputs: every declared output name must appear in the command template. Declared inputs and config values may be unused; if they are passed to the rule call, they still participate in the output fingerprint. Any placeholder that does appear must be a declared input, a declared output, or a built-in placeholder such as `{workdir}`.

### Generated config files

For tools that normally consume a large config file, register a built-in text-file rule and pass serialized config text from the pipeline factory. The text is written directly by Python, so it avoids shell quoting problems and command-line length limits from patterns such as `printf {config}`.

```python
import json
from necroflow import NodeType, Rules

class SageConfig(NodeType):
    filename = "sage.json"

R = Rules()
R.text_file("write_sage_config", SageConfig)

@R.command("necromerge2-run-sage {spectra} {fasta} {outdir} {run_info} --config {sage_config}")
def run_sage(spectra: SageInputStaged, fasta: Fasta, sage_config: SageConfig):
    return SageRawOutdir[outdir], SageRunInfo[run_info]
```

A job TOML table can then be passed through as ordinary factory config:

```toml
[sage]
deisotope = true
min_peaks = 15
```

```python
P.sage_config = R.write_sage_config(
    text=json.dumps(config["sage"], sort_keys=True, indent=2) + "\n"
)
P.sage_out, P.run_info = R.run_sage(P.spectra, P.fasta, P.sage_config)
```

`Rules.text_file(name, output, input_name="text")` creates a normal cached node. The text value participates in the node fingerprint, and the built-in writer recipe (`necroflow.text_file/v1`) is hashed in place of shell command text.

### Custom invalidation

A `NodeType` may define an optional `invalidator` callback next to `filename`. The callback receives the concrete `Node` and must return a stable string token. Necroflow stores that token under the node's `.rip/` metadata after a successful run and marks the node `STALE` if the token is missing or changes later.

```python
import hashlib
from pathlib import Path

def sha256_of_path(node):
    return hashlib.sha256(Path(node.config["path"]).read_bytes()).hexdigest()

class ToolBinary(NodeType):
    filename = "tool.ready"
    invalidator = sha256_of_path
```

Use this for external dependencies that should invalidate a cached node without becoming normal necroflow outputs, such as a binary, script, or selected source tree hash. Types without `invalidator` use the normal cache behavior. If the callback raises, execution fails fast instead of guessing whether the cache is valid.

Invalidators are evaluated during the initial node classification at the start of `execute()`. After a job succeeds, necroflow recomputes and stores the token for that node's outputs, but it does not re-run all invalidators between tasks in the same execution. If an external dependency changes while a pipeline is already running, that change is detected on the next `execute()` invocation.

- Re-running with the same inputs is a no-op (cache hit).
- Changing any upstream parameter, command, or declared type produces a new path — old results are never overwritten.
- A parent whose mtime is newer than a child triggers a content-hash check: if the parent's bytes are unchanged, the child is **not** re-run. Only a genuine content change marks children STALE.
- Each output folder contains a `.rip/` subdirectory with:
  - `dependencies.toml` — full accumulated config for provenance.
  - `{filename}.hash` — SHA-256 content hash, used for STALE detection on the next run.
  - `job.log` — captured stdout/stderr.
  - `state` — last recorded run state (`running` / `up_to_date` / `failed` / `interrupted`). If a process is killed mid-run the `state` file is left as `running`; on the next invocation necroflow detects this and re-runs the node even if its output exists on disk.

## Concurrency

**Only one necroflow instance may run against a given node store at a time.** `execute()` acquires an exclusive lock on `nodes/.rip/necroflow.lock` (via `fcntl.flock`) at startup and releases it on exit. A second instance targeting the same node store will fail immediately with a clear error. Running two instances against *overlapping* node stores (e.g. `nodes` and `nodes/sub`) is unsupported — there is no OS primitive to detect this, so avoid it.

## Parallelism and scheduling

`execute()` runs nodes in parallel subject to resource caps. By default the thread cap is all available CPUs. Declare per-job requirements with `Constraints`; set global caps via `resource_caps` (Python API) or CLI flags.

```python
@r.command("bwa mem {ref} {fastq} > {bam}", threads=4, ram="8Gi")
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM."""
    return Bam[bam]

dag.execute(resource_caps={"threads": 16, "ram": parse_resource("64Gi")})
```

Resource values accept SI (`K M G T P` = powers of 1000) and binary (`Ki Mi Gi Ti Pi` = powers of 1024) suffixes — e.g. `"8Gi"` is 8 GiB, `"8G"` is 8 GB. A job whose requirement exceeds the cap still runs solo when nothing else is running.

Rules also accept `repeat=N` in `R.register(...)`, `@r.command(...)`, and `@r.rule(...)` for Snakemake-style compatibility. Necroflow stores it as `rule.repeat` and validates that it is a positive integer, but it is currently metadata only: it does not make the executor run the command multiple times and it is not part of scheduling resources or output fingerprints.

By default the scheduler prioritises nodes from the **smallest connected component** of remaining work — this tends to finish whole samples before starting new ones, keeping memory pressure low.

```python
from necroflow import fifo_scheduler

dag.execute(resource_caps={"threads": 16}, scheduler=fifo_scheduler)  # topological order instead
```

Custom schedulers:

```python
def my_scheduler(ready, remaining):
    return sorted(ready, key=lambda n: n.rule.constraints.get("threads", 1), reverse=True)

dag.execute(scheduler=my_scheduler)
```

## Types and subtypes

NodeTypes form an inheritance hierarchy — a rule accepting `Bam` also accepts `SortedBam`:

```python
class SortedBam(Bam):
    """Coordinate-sorted BAM."""
    filename = "sorted.bam"

@r.command("samtools sort {bam} -o {sorted_bam}")
def sort(bam: Bam):
    """Sort BAM by coordinate with samtools."""
    return SortedBam[sorted_bam]

@r.command("featureCounts -a {gene_model} {bam} -o {counts}")
def quantify(bam: SortedBam, gene_model: str):  # only accepts sorted bam
    """Count reads per gene using featureCounts."""
    return Counts[counts]
```

## Failure handling

```python
dag.execute(keep_going=True)   # continue independent branches past failures
```

With `keep_going=False` (default) the first failure raises immediately. With `keep_going=True` independent branches keep running and all failures are collected into an `ExceptionGroup` at the end.

After each successful job, necroflow verifies that the declared output file exists. A command that exits 0 but writes no output is treated as a failure.

Run state is persisted to a plain-text `state` file inside each node's `.rip/` directory between invocations. A node whose output exists on disk but whose previous run was interrupted by a signal or left in an unknown state is automatically re-executed next time.

Each job's stdout/stderr is captured to the node store at `{rule}/{hash}/.rip/job.log`. On failure the log is printed to the terminal.

## Multi-output rules

A rule with multiple declared outputs runs its command **once**; all co-outputs are marked complete when the command finishes:

```python
@r.command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM, capturing the log."""
    return Bam[bam], Log[log]

P = Pipeline()
P.fastq = R.raw_fastq(path=config.path)
P.bam, P.log = R.align(P.fastq, ref="hg38")
```

## Cleaning orphan outputs

Outputs that existed from a previous run but are no longer in the required subgraph are classified as `ORPHAN`. Pass `autoclean=True` to delete them. Intermediate rule-call directories are removed as whole directories once all downstream work is complete, so side files written under `{workdir}` are cleaned together with the declared outputs:

```python
dag.execute(autoclean=True)
```

Or via CLI:

```bash
necroflow --nodes-dir nodes --results-dir results --autoclean job.toml
```


## What is not yet implemented

- Cluster / cloud backends
