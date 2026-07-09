# Rules and Typed Outputs

[Back to README](../README.md)

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
