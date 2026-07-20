"""Typed bioinformatics pipeline example used by the Necroflow paper.

The factories are safe to import and inspect without requiring BWA, samtools,
featureCounts, or bcftools. Commands run only when a DAG is executed.
"""

from types import SimpleNamespace

from necroflow import DAG, NodeType, Pipeline, resolve_command, command, output


class Fastq(NodeType):
    filename = "reads.fastq.gz"


class Bam(NodeType):
    filename = "aligned.bam"


class SortedBam(Bam):
    filename = "sorted.bam"


class Log(NodeType):
    filename = "align.log"


class Counts(NodeType):
    filename = "counts.txt"


class Vcf(NodeType):
    filename = "variants.vcf.gz"


class AnnotatedVcf(NodeType):
    filename = "annotated.vcf.gz"


@command("ln -s {path} {fastq}")
def raw_fastq(path: str):
    """Expose a source FASTQ as a typed artifact."""
    fastq = output(Fastq)
    return fastq


@command(
    "bwa mem {reference} {fastq} 2> {log} | samtools view -b -o {bam}",
    threads=4,
)
def align(fastq: Fastq, reference: str):
    """Align reads and write a BAM plus the BWA log."""
    bam = output(Bam)
    log = output(Log)
    return bam, log


@command("samtools sort {bam} -o {sorted_bam}")
def sort_bam(bam: Bam):
    """Sort a BAM by coordinate."""
    sorted_bam = output(SortedBam)
    return sorted_bam


@command("featureCounts -a {gene_model} {bam} -o {counts}")
def quantify(bam: SortedBam, gene_model: str):
    """Count reads per gene."""
    counts = output(Counts)
    return counts


@command(
    "bcftools mpileup -f {reference} -Ou {bam} | " "bcftools call -mv -Oz -o {vcf}"
)
def call_variants(bam: SortedBam, reference: str):
    """Call variants from a sorted BAM."""
    vcf = output(Vcf)
    return vcf


@command("bcftools annotate -a {database} {vcf} -Oz -o {annotated_vcf}")
def annotate(vcf: Vcf, database: str):
    """Annotate a VCF against a supplied database."""
    annotated_vcf = output(AnnotatedVcf)
    return annotated_vcf


def aligned_reads(config):
    """Construct the import, alignment, and sorting prefix."""
    P = Pipeline()
    P.fastq = raw_fastq(path=config.path)
    P.bam, P.align_log = align(P.fastq, reference=config.reference)
    P.sorted_bam = sort_bam(P.bam)
    return P


def quantification_pipeline(config):
    P = aligned_reads(config)
    P.counts = quantify(P.sorted_bam, gene_model=config.gene_model)
    return P


def variant_pipeline(config):
    P = aligned_reads(config)
    P.vcf = call_variants(P.sorted_bam, reference=config.reference)
    return P


def extended_pipeline(config):
    P = quantification_pipeline(config)
    if config.call_variants:
        P.vcf = call_variants(P.sorted_bam, reference=config.reference)
        P.annotated_vcf = annotate(P.vcf, database=config.variant_database)
    return P


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
    pipeline = extended_pipeline(config)
    pipeline.resolve_paths("results")
    return pipeline, [resolve_command(node) for node in pipeline.nodes]


def shared_dag(outdir="results"):
    config = example_config()
    quant = quantification_pipeline(config)
    variants = variant_pipeline(config)
    dag = DAG(outdir)
    dag.add(quant)
    dag.add(variants)
    return dag, quant, variants


if __name__ == "__main__":
    pipeline, commands = inspect_example()
    print(pipeline)
    print("\n".join(commands))
