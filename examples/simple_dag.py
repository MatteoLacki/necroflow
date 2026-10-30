"""Typed bioinformatics pipeline example used by the Necroflow paper.

The workflows are safe to import and inspect without requiring BWA, samtools,
featureCounts, or bcftools. Commands run only when a DAG is executed.
"""

from necroflow import shell

from types import SimpleNamespace

from necroflow import (
    Resource,
    DAG,
    Node,
    Pipeline,
    command,
    outputs,
    symlink,
    workflow,
)


class Fastq(Node):
    filename = "reads.fastq.gz"


class Bam(Node):
    filename = "aligned.bam"


class SortedBam(Bam):
    filename = "sorted.bam"


class Log(Node):
    filename = "align.log"


class Counts(Node):
    filename = "counts.txt"


class Vcf(Node):
    filename = "variants.vcf.gz"


class AnnotatedVcf(Node):
    filename = "annotated.vcf.gz"


@command
def align(fastq: Fastq, reference: str, threads: Resource[int] = 4):
    """Align reads and write a BAM plus the BWA log."""
    bam, log = outputs(Bam, Log)
    shell(
        "bwa mem -t {threads} {reference} {fastq} 2> {log} | samtools view -b -o {bam}"
    )
    return bam, log


@command
def sort_bam(bam: Bam):
    """Sort a BAM by coordinate."""
    sorted_bam = outputs(SortedBam)
    shell("samtools sort {bam} -o {sorted_bam}")
    return sorted_bam


@command
def quantify(bam: SortedBam, gene_model: str):
    """Count reads per gene."""
    counts = outputs(Counts)
    shell("featureCounts -a {gene_model} {bam} -o {counts}")
    return counts


@command
def call_variants(bam: SortedBam, reference: str):
    """Call variants from a sorted BAM."""
    vcf = outputs(Vcf)
    shell(
        "bcftools mpileup -f {reference} -Ou {bam} | bcftools call -mv -Oz -o " "{vcf}"
    )
    return vcf


@command
def annotate(vcf: Vcf, database: str):
    """Annotate a VCF against a supplied database."""
    annotated_vcf = outputs(AnnotatedVcf)
    shell("bcftools annotate -a {database} {vcf} -Oz -o {annotated_vcf}")
    return annotated_vcf


@workflow
def aligned_reads(P: Pipeline, config) -> None:
    """Construct the import, alignment, and sorting prefix."""
    P.fastq = symlink(Fastq, config.path)
    P.bam, P.align_log = align(P.fastq, reference=config.reference)
    P.sorted_bam = sort_bam(P.bam)


@workflow
def quantification_pipeline(P: Pipeline, config) -> None:
    aligned_reads(P, config)
    P.counts = quantify(P.sorted_bam, gene_model=config.gene_model)


@workflow
def variant_pipeline(P: Pipeline, config) -> None:
    aligned_reads(P, config)
    P.vcf = call_variants(P.sorted_bam, reference=config.reference)


@workflow
def extended_pipeline(P: Pipeline, config) -> None:
    quantification_pipeline(P, config)
    if config.call_variants:
        P.vcf = call_variants(P.sorted_bam, reference=config.reference)
        P.annotated_vcf = annotate(P.vcf, database=config.variant_database)


def example_config(**overrides):
    values = {
        "path": "/data/sample.fastq.gz",
        "reference": "/refs/hg38.fa",
        "gene_model": "/refs/gencode.gtf",
        "variant_database": "/refs/dbsnp.vcf.gz",
        "call_variants": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def inspect_example():
    config = example_config()
    pipeline = Pipeline(DAG("results"))
    extended_pipeline(pipeline, config)
    return pipeline, [node.rule_call.resolve() for node in pipeline.nodes]


def shared_dag(outdir="results"):
    config = example_config()
    dag = DAG(outdir)
    quant = Pipeline(dag)
    quantification_pipeline(quant, config)
    variants = Pipeline(dag)
    variant_pipeline(variants, config)
    quant.finish()
    variants.finish()
    dag.require(quant.sinks())
    dag.require(variants.sinks())
    return dag, quant, variants


if __name__ == "__main__":
    pipeline, commands = inspect_example()
    print(pipeline)
    print("\n".join(command for command in commands if command is not None))
