try:
    _ip = get_ipython()
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from necroflow import (
    Node,
    NodeType,
    node_types,
    Inputs,
    Outputs,
    Constraints,
    Rules,
    Pipeline,
)

# --- node types ---

Fastq, Bam, Log, Counts, QcReport, Vcf, AnnotatedVcf, MergedVcf = node_types(
    "fastq bam log counts qc_report vcf annotated_vcf merged_vcf"
)


class SortedBam(Bam):
    """SortedBam IS-A Bam — accepted wherever Bam is expected."""


# --- rules ---

R = Rules()

R.register("raw_fastq", Inputs(path=str), Outputs(fastq=Fastq), "ln -s {path} {fastq}")

R.register(
    "align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam, log=Log),
    "bwa mem {ref} {fastq} > {bam}",
    Constraints(threads=4),
)

R.register(
    "sort_bam",
    Inputs(bam=Bam),
    Outputs(sorted_bam=SortedBam),
    "samtools sort {bam} -o {sorted_bam}",
)

R.register(
    "quantify",
    Inputs(bam=SortedBam, gene_model=str),
    Outputs(counts=Counts, qcreport=QcReport),
    "featureCounts -a {gene_model} {bam} -o {counts}",
)

# --- linear pipeline ---


def basic_pipeline(config, R):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=config.path)
    P.bam, P.align_log = R.align(P.fastq, ref=config.ref)
    P.sorted_bam = R.sort_bam(P.bam)
    P.counts, P.qc = R.quantify(P.sorted_bam, gene_model=config.gene_model)
    return P


from types import SimpleNamespace

config = SimpleNamespace(path="/data/sample.fastq.gz", ref="hg38", gene_model="gencode_v44")
P = basic_pipeline(config, R)

print(P)
P.plot()


# --- diamond pipeline ---

R.register(
    "call_variants",
    Inputs(bam=SortedBam, caller=str),
    Outputs(vcf=Vcf),
    "gatk HaplotypeCaller -I {bam} -O {vcf} --caller {caller}",
)

R.register(
    "annotate",
    Inputs(vcf=Vcf, db=str),
    Outputs(annotated_vcf=AnnotatedVcf),
    "bcftools annotate -a {db} {vcf} -o {annotated_vcf}",
)

R.register(
    "merge_annotations",
    Inputs(snp_ann=AnnotatedVcf, indel_ann=AnnotatedVcf),
    Outputs(merged_vcf=MergedVcf),
    "bcftools merge {snp_ann} {indel_ann} -o {merged_vcf}",
)

def diamond_pipeline(config, R):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=config.path)
    P.bam, P.align_log = R.align(P.fastq, ref=config.ref)
    P.sorted_bam = R.sort_bam(P.bam)
    P.snp_vcf = R.call_variants(P.sorted_bam, caller="haplotypecaller")
    P.indel_vcf = R.call_variants(P.sorted_bam, caller="mutect2")
    P.snp_ann = R.annotate(P.snp_vcf, db="dbsnp")
    P.indel_ann = R.annotate(P.indel_vcf, db="clinvar")
    P.merged = R.merge_annotations(P.snp_ann, P.indel_ann)
    return P


dconfig = SimpleNamespace(path="/data/sample2.fastq.gz", ref="hg38")
D = diamond_pipeline(dconfig, R)

print(D)
D.plot()
