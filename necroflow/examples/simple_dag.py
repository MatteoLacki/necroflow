try:
    _ip = get_ipython()
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from necroflow import Node, NodeType, node_types, Pipeline, rule

# --- node types ---

Fastq, Bam, Log, Counts, QcReport, Vcf, AnnotatedVcf, MergedVcf = node_types(
    "fastq bam log counts qc_report vcf annotated_vcf merged_vcf"
)


class SortedBam(Bam):
    """SortedBam IS-A Bam — accepted wherever Bam is expected"""


# --- rules ---
@rule
def raw_fastq(*, path: str) -> Node:
    return Fastq()


@rule(threads=4)
def align(fastq: Fastq, *, ref: str):
    return Bam("bam"), Log("log")


@rule
def sort_bam(bam: Bam):
    return SortedBam()


@rule
def quantify(bam: SortedBam, *, gene_model: str):
    return Counts("counts"), QcReport("qc_report")


# --- linear pipeline ---

P = Pipeline()
P.fastq = raw_fastq(path="/data/sample.fastq.gz")
P.bam, P.align_log = align(P.fastq, ref="hg38")
P.sorted_bam = sort_bam(P.bam)
P.counts, P.qc = quantify(P.sorted_bam, gene_model="gencode_v44")

print(P)
P.plot()

# --- diamond pipeline ---


@rule
def call_variants(bam: SortedBam, *, caller: str):
    return Vcf()


@rule
def annotate(vcf: Vcf, *, db: str):
    return AnnotatedVcf()


@rule
def merge_annotations(snp_ann: AnnotatedVcf, indel_ann: AnnotatedVcf):
    return MergedVcf()


D = Pipeline()
D.fastq = raw_fastq(path="/data/sample2.fastq.gz")
D.bam, D.align_log = align(D.fastq, ref="hg38")
D.sorted_bam = sort_bam(D.bam)
D.snp_vcf = call_variants(D.sorted_bam, caller="haplotypecaller")
D.indel_vcf = call_variants(D.sorted_bam, caller="mutect2")
D.snp_ann = annotate(D.snp_vcf, db="dbsnp")
D.indel_ann = annotate(D.indel_vcf, db="clinvar")
D.merged = merge_annotations(D.snp_ann, D.indel_ann)

print(D)
D.plot()
