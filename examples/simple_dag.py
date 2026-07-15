"""Typed bioinformatics pipeline example used by the Necroflow paper.

The factories are safe to import and inspect without requiring BWA, samtools,
featureCounts, or bcftools. Commands run only when a DAG is executed.
"""

from types import SimpleNamespace

from necroflow import DAG, NodeType, Pipeline, Rules, resolve_command


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


r = Rules()


@r.command("ln -s {path} {fastq}")
def raw_fastq(path: str):
    """Expose a source FASTQ as a typed artifact."""
    return Fastq[fastq]


@r.command(
    "bwa mem {reference} {fastq} 2> {log} | samtools view -b -o {bam}",
    threads=4,
)
def align(fastq: Fastq, reference: str):
    """Align reads and write a BAM plus the BWA log."""
    return Bam[bam], Log[log]


@r.command("samtools sort {bam} -o {sorted_bam}")
def sort_bam(bam: Bam):
    """Sort a BAM by coordinate."""
    return SortedBam[sorted_bam]


@r.command("featureCounts -a {gene_model} {bam} -o {counts}")
def quantify(bam: SortedBam, gene_model: str):
    """Count reads per gene."""
    return Counts[counts]


@r.command(
    "bcftools mpileup -f {reference} -Ou {bam} | " "bcftools call -mv -Oz -o {vcf}"
)
def call_variants(bam: SortedBam, reference: str):
    """Call variants from a sorted BAM."""
    return Vcf[vcf]


@r.command("bcftools annotate -a {database} {vcf} -Oz -o {annotated_vcf}")
def annotate(vcf: Vcf, database: str):
    """Annotate a VCF against a supplied database."""
    return AnnotatedVcf[annotated_vcf]


def aligned_reads(config, rules=r):
    """Construct the import, alignment, and sorting prefix."""
    P = Pipeline()
    P.fastq = rules.raw_fastq(path=config.path)
    P.bam, P.align_log = rules.align(P.fastq, reference=config.reference)
    P.sorted_bam = rules.sort_bam(P.bam)
    return P


def quantification_pipeline(config, rules=r):
    P = aligned_reads(config, rules)
    P.counts = rules.quantify(P.sorted_bam, gene_model=config.gene_model)
    return P


def variant_pipeline(config, rules=r):
    P = aligned_reads(config, rules)
    P.vcf = rules.call_variants(P.sorted_bam, reference=config.reference)
    return P


def extended_pipeline(config, rules=r):
    P = quantification_pipeline(config, rules)
    if config.call_variants:
        P.vcf = rules.call_variants(P.sorted_bam, reference=config.reference)
        P.annotated_vcf = rules.annotate(P.vcf, database=config.variant_database)
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
