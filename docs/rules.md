# Rules and Typed Outputs

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)

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

## Pipeline sections

Use `P.section(name)` to mark the author-defined stage for all later node assignments:

```python
def my_pipeline(config, R):
    P = Pipeline()
    P.section("Read alignment")
    P.bam = R.align(path=config.path, ref=config.ref)
    P.section("Quantification")
    P.counts = R.count_reads(P.bam)
    return P
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
from necroflow import resolve_command

P = rna_pipeline(config, R)
print(P)                    # layered ASCII DAG to stdout
P.save("pipeline.txt")      # same render to a file

dag.save("dag.txt")         # works on DAG too

P.resolve_paths("results")
for node in P.nodes:
    print(resolve_command(node))   # fully-resolved shell command
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


@r.command("filter-mmappet {precursors} > {filtered_precursors}")
def filter_precursors(precursors: PrecursorTable):
    return FilteredPrecursors[filtered_precursors]


@r.command("index-mmappet {dataset} > {indexed_filtered_precursors}")
def index(dataset: FilteredPrecursors):
    return IndexedFilteredPrecursors[indexed_filtered_precursors]


@r.command("score-mmappet {dataset} > {scores}")
def score(dataset: MmappetDataset):
    return Scores[scores]


@r.command("query-index {dataset} > {report}")
def query(dataset: IndexedDataset):
    return Report[report]


@r.command("import-mmappet {dataset} > {precursor_table}")
def import_any_mmappet(dataset: PrecursorTable | FilteredPrecursors):
    return PrecursorTable[precursor_table]
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
@r.command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM, capturing the log."""
    return Bam[bam], Log[log]

P = Pipeline()
P.fastq = R.raw_fastq(path=config.path)
P.bam, P.log = R.align(P.fastq, ref="hg38")
```

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)
