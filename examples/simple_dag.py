"""Typed bioinformatics pipeline example used by the Necroflow paper.

The workflows are safe to import and inspect without requiring BWA, samtools,
featureCounts, or bcftools. Commands run only when a DAG is executed.
"""

import necroflow as nf

from types import SimpleNamespace


class Fastq(nf.Node):
    filename = "reads.fastq.gz"


class Bam(nf.Node):
    filename = "aligned.bam"


class SortedBam(Bam):
    filename = "sorted.bam"


class Log(nf.Node):
    filename = "align.log"


class Counts(nf.Node):
    filename = "counts.txt"


class Vcf(nf.Node):
    filename = "variants.vcf.gz"


class AnnotatedVcf(nf.Node):
    filename = "annotated.vcf.gz"


@nf.command
def align(fastq: Fastq, reference: str, threads: nf.Resource[int] = 4):
    """Align reads and write a BAM plus the BWA log."""
    bam, log = nf.outputs(Bam, Log)
    nf.shell(
        "bwa mem -t {threads} {reference} {fastq} 2> {log} | samtools view -b -o {bam}"
    )
    return bam, log


@nf.command
def sort_bam(bam: Bam):
    """Sort a BAM by coordinate."""
    sorted_bam = nf.outputs(SortedBam)
    nf.shell("samtools sort {bam} -o {sorted_bam}")
    return sorted_bam


@nf.command
def quantify(bam: SortedBam, gene_model: str):
    """Count reads per gene."""
    counts = nf.outputs(Counts)
    nf.shell("featureCounts -a {gene_model} {bam} -o {counts}")
    return counts


@nf.command
def call_variants(bam: SortedBam, reference: str):
    """Call variants from a sorted BAM."""
    vcf = nf.outputs(Vcf)
    nf.shell(
        "bcftools mpileup -f {reference} -Ou {bam} | bcftools call -mv -Oz -o " "{vcf}"
    )
    return vcf


@nf.command
def annotate(vcf: Vcf, database: str):
    """Annotate a VCF against a supplied database."""
    annotated_vcf = nf.outputs(AnnotatedVcf)
    nf.shell("bcftools annotate -a {database} {vcf} -Oz -o {annotated_vcf}")
    return annotated_vcf


@nf.workflow
def aligned_reads(P: nf.Pipeline, config) -> None:
    """Construct the import, alignment, and sorting prefix."""
    P.fastq = nf.symlink(Fastq, config.path)
    P.bam, P.align_log = align(P.fastq, reference=config.reference)
    P.sorted_bam = sort_bam(P.bam)


@nf.workflow
def quantification_pipeline(P: nf.Pipeline, config) -> None:
    aligned_reads(P, config)
    P.counts = quantify(P.sorted_bam, gene_model=config.gene_model)


@nf.workflow
def variant_pipeline(P: nf.Pipeline, config) -> None:
    aligned_reads(P, config)
    P.vcf = call_variants(P.sorted_bam, reference=config.reference)


@nf.workflow
def extended_pipeline(P: nf.Pipeline, config) -> None:
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
    pipeline = nf.Pipeline(nf.DAG("results"))
    extended_pipeline(pipeline, config)
    return pipeline, [node.rule_call.resolve() for node in pipeline.nodes]


def shared_dag(outdir="results"):
    config = example_config()
    dag = nf.DAG(outdir)
    quant = nf.Pipeline(dag)
    quantification_pipeline(quant, config)
    variants = nf.Pipeline(dag)
    variant_pipeline(variants, config)
    quant.finish()
    variants.finish()
    dag.require(quant.sinks())
    dag.require(variants.sinks())
    return dag, quant, variants


if __name__ == "__main__":
    pipeline, commands = inspect_example()
    print(pipeline)
    print("\n".join(nf.command for command in commands if nf.command is not None))
