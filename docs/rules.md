# Rules and Typed Outputs

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Rule-Call Lifecycle](rule-call-lifecycle.md)

The primary API is the explicit factory form. Factory rules require a stable `name=` and may include `doc=`; `Inputs` and `Outputs` preserve declaration order.

```python
run_sage = command(
    "sage {config} -f {fasta} -o {workdir} {spectra}",
    Inputs(spectra=SpectraFile, fasta=Fasta, config=SageConfig),
    Outputs(results_json=SageResultsJson, results_pin=SageResultsPin),
    Constraints(threads=4),
    name="run_sage",
    doc="Run Sage.",
)
```

The decorator form below remains supported as syntactic sugar.

## Python command callbacks

When a command must be assembled from resolved values, pass a module-level
Python function instead of a static template:

```python
import shlex
from necroflow import CommandArgs

def merge_command(args: CommandArgs) -> str:
    parts = [
        "samtools",
        "merge",
        "--threads",
        str(args.constraints.threads),
        str(args.outputs.merged),
        *(str(path) for path in args.inputs.values()),
    ]
    return shlex.join(parts)

merge = command(
    merge_command,
    Inputs(first=Bam, second=Bam),
    Outputs(merged=MergedBam),
    Constraints(threads=8),
    name="merge",
)
```

`CommandArgs` contains read-only named `inputs`, `config`, `outputs`, and
`constraints` collections plus `workdir`. Names support both attribute and
mapping access, such as `args.outputs.merged` and `args.outputs["merged"]`.

Callbacks return a complete shell string. Necroflow executes it unchanged and
does not attempt to infer or repair quoting; use `shlex.quote` or `shlex.join`
when interpolated values require shell escaping. Argv-list commands are not
supported in fingerprint v2.

Command callbacks must be module-level, closure-free functions or unambiguous
source-file lambdas accepting exactly one argument. Their canonical AST and
the running Python implementation/version participate in the default
fingerprint.

## Declaring rule outputs

Import `output` with the decorator and bind every output to a real local name:

```python
from necroflow import NodeType, command, output

class Bam(NodeType):
    filename = "aligned.bam"

class Log(NodeType):
    filename = "align.log"

@command("aligner {reads} > {bam} 2> {log}")
def align(reads: str):
    bam = output(Bam)
    log = output(Log)
    return bam, log
```

The decorated body is a declaration, not executable rule code. After an optional
docstring it contains one or more top-level `name = output(ConcreteNodeType)`
assignments and a final return of each declared name exactly once. The return order
defines the rule call's single-node or tuple shape. This is ordinary valid Python, so
linters understand that the names are bound and type checkers can preserve the return
shape through pipeline assignments.

## Conditional pipelines

Pipeline factory functions are plain Python, so `if/else` branching on config values works naturally:

```python
def my_pipeline(P: Pipeline, config) -> None:
    P.a = align(P, path=config.path, ref=config.ref)
    if config.call_variants:
        P.result = call_snps(P, P.a)
    else:
        P.result = count_reads(P, P.a)
```

The branching config value (`config.call_variants`) does not need to be passed to any node. The rule name already encodes which branch was taken in the fingerprint, so `call_snps` and `count_reads` always produce distinct output paths regardless.

Two pipelines sharing the same upstream config (e.g. same `path` and `ref`) will reuse the `align` output — recognised as a cache hit — even if they take different branches downstream.

**Pipeline attribute names cannot be overwritten.** Assigning to the same name twice raises `ValueError`. If you want to build a pipeline in a loop, use distinct names:

```python
for i, step in enumerate(steps):
    P[f"result_{i}"] = process(P, step_node, mode=step)
```

The idiomatic pattern for multi-sample or multi-condition work is one shared
`DAG` and a separate `Pipeline(dag)` per config. Equivalent rule calls are
interned immediately; after each factory, call `dag.require(P.sinks())` or
require explicitly selected labels.
Attribute and item labels (`P.result` and `P["result"]`) share one namespace.
Item labels may use non-identifier characters such as spaces or hyphens, but
remain one relative path component because the CLI uses them for result links.

## Pipeline sections

Use `P.section(name)` to mark the author-defined stage for all later node assignments:

```python
def my_pipeline(P: Pipeline, config) -> None:
    P.section("Read alignment")
    P.bam = align(P, path=config.path, ref=config.ref)
    P.section("Quantification")
    P.counts = count_reads(P, P.bam)
```

A section is presentation metadata, not computational input: it does not change node fingerprints, paths, cache hits, execution, or provenance. `necroflow graph --json` includes the section for each unambiguous node, and `necroflow graph --png` uses section clusters only when every displayed rule call has one unambiguous section. A shared node assigned to conflicting sections across pipelines falls back to the ordinary dependency-depth layout.

## Inspecting a pipeline

From the command line, render the requested job DAG without executing it:

```bash
necroflow graph job.toml
necroflow graph --output graph.txt job.toml
```

The same rendering is available from Python:

```python
from necroflow import DAG, Pipeline, resolve_command

dag = DAG("results")
P = Pipeline(dag)
rna_pipeline(P, config)
print(P)                    # layered ASCII DAG to stdout
P.save("pipeline.txt")      # same render to a file

dag.save("dag.txt")         # works on DAG too

for node in P.nodes:
    print(resolve_command(node))   # fully-resolved shell command
```

## Types and subtypes

NodeTypes form an inheritance hierarchy — a rule accepting `Bam` also accepts `SortedBam`:

```python
class SortedBam(Bam):
    """Coordinate-sorted BAM."""
    filename = "sorted.bam"

@command("samtools sort {bam} -o {sorted_bam}")
def sort(bam: Bam):
    """Sort BAM by coordinate with samtools."""
    sorted_bam = output(SortedBam)
    return sorted_bam
@command("featureCounts -a {gene_model} {bam} -o {counts}")
def quantify(bam: SortedBam, gene_model: str):  # only accepts sorted bam
    """Count reads per gene using featureCounts."""
    counts = output(Counts)
    return counts
```

The same pattern is useful for format families. Define a base `NodeType` for the
format contract, then make every concrete output subclass it. Downstream rules
can accept the base class when they only care that the input is a valid member
of that family:

```python
class MmappetDataset(NodeType):
    """Base type for mmappet directory outputs."""


class PrecursorTable(MmappetDataset):
    filename = "precursors.mmappet"


class FilteredPrecursors(MmappetDataset):
    filename = "filtered.mmappet"


class IndexedDataset(NodeType):
    """Base type for outputs with a ready-to-query index."""


class IndexedFilteredPrecursors(FilteredPrecursors, IndexedDataset):
    filename = "indexed-filtered.mmappet"


@command("filter-mmappet {precursors} > {filtered_precursors}")
def filter_precursors(precursors: PrecursorTable):
    filtered_precursors = output(FilteredPrecursors)
    return filtered_precursors
@command("index-mmappet {dataset} > {indexed_filtered_precursors}")
def index(dataset: FilteredPrecursors):
    indexed_filtered_precursors = output(IndexedFilteredPrecursors)
    return indexed_filtered_precursors
@command("score-mmappet {dataset} > {scores}")
def score(dataset: MmappetDataset):
    scores = output(Scores)
    return scores
@command("query-index {dataset} > {report}")
def query(dataset: IndexedDataset):
    report = output(Report)
    return report
@command("import-mmappet {dataset} > {precursor_table}")
def import_any_mmappet(dataset: PrecursorTable | FilteredPrecursors):
    precursor_table = output(PrecursorTable)
    return precursor_table
```

Here `score()` accepts `PrecursorTable`, `FilteredPrecursors`, or
`IndexedFilteredPrecursors`, because all are `MmappetDataset` subclasses.
`query()` accepts `IndexedFilteredPrecursors`, because it also inherits from
`IndexedDataset`. Use multiple inheritance for combined requirements: "must be
both filtered precursors and indexed". Use a union for alternatives:
`PrecursorTable | FilteredPrecursors` means "either concrete contract is fine".
Mixed unions such as `NodeType | str` are rejected because node inputs and config
inputs are different parts of the rule API.

Unions are for inputs only. A rule output should be a concrete `NodeType`, not a
union, because necroflow needs one exact artifact type to choose the filename,
node identity, downstream type, and provenance shape. If two rules can produce
alternative formats, give each producer a concrete output type and let downstream
consumers accept the alternatives with a union input.

Keep structural or semantic checks close to pipeline construction with a
validator when a config path is imported from outside the DAG:

```python
from pathlib import Path


def validate(config):
    path = Path(config["precursors"])
    if path.suffix != ".mmappet" or not path.is_dir():
        raise ValueError("precursors must be a .mmappet directory")
    if not (path / "precursors.parquet").exists():
        raise ValueError("invalid mmappet dataset: missing precursors.parquet")
```

## Multi-output rules

A rule with multiple declared outputs runs its command **once**; all co-outputs are marked complete when the command finishes:

```python
@symlink_file
def raw_fastq(path: str):
    fastq = output(Fastq)
    return fastq
@command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM, capturing the log."""
    bam = output(Bam)
    log = output(Log)
    return bam, log
dag = DAG("nodes")
P = Pipeline(dag)
P.fastq = raw_fastq(P, path=config.path)
P.bam, P.log = align(P, P.fastq, ref="hg38")
dag.require(P.sinks())
```

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Rule-Call Lifecycle](rule-call-lifecycle.md)
