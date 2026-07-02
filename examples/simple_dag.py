from __future__ import annotations

try:
    _ip = get_ipython()
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from types import SimpleNamespace
from necroflow import (
    NodeType,
    Inputs,
    Outputs,
    Constraints,
    Rules,
    resolve_command,
    Pipeline,
    DAG,
)

# --- node types ---

class Fastq(NodeType):
    """Raw sequencing reads (FASTQ format)."""
    filename = "reads.fastq.gz"

class Bam(NodeType):
    """Aligned reads in BAM format."""
    filename = "aligned.bam"

class SortedBam(Bam):
    """Coordinate-sorted BAM; required before quantification or variant calling."""
    filename = "sorted.bam"

class Log(NodeType):
    """Aligner log capturing mapping statistics."""
    filename = "align.log"

class Counts(NodeType):
    """Per-gene read counts produced by featureCounts."""
    filename = "counts.txt"

class QcReport(NodeType):
    """Alignment QC summary from featureCounts."""
    filename = "qc.txt"

class Vcf(NodeType):
    """Raw variant calls in VCF format."""
    filename = "variants.vcf.gz"

class AnnotatedVcf(NodeType):
    """Variant calls enriched with database annotations."""
    filename = "annotated.vcf.gz"

class MergedVcf(NodeType):
    """SNP and indel calls merged into a single VCF."""
    filename = "merged.vcf.gz"


# --- rules ---

R = Rules()
rule = R.rule

@rule
def raw_fastq(path: str) -> Fastq[fastq]:
    """Symlink a raw FASTQ file into the output tree."""
    command = "ln -s {path} {fastq}"

@rule(threads=4)
def align(fastq: Fastq, ref: str) -> (Bam[bam], Log[log]):
    """Align reads to a reference genome with BWA-MEM."""
    command = "bwa mem {ref} {fastq} > {bam}"

@rule
def sort_bam(bam: Bam) -> SortedBam[sorted_bam]:
    """Sort BAM by coordinate with samtools."""
    command = "samtools sort {bam} -o {sorted_bam}"

@rule
def quantify(bam: SortedBam, gene_model: str) -> (Counts[counts], QcReport[qcreport]):
    """Count reads per gene using featureCounts."""
    command = "featureCounts -a {gene_model} {bam} -o {counts}"

@rule
def call_variants(bam: SortedBam, caller: str) -> Vcf[vcf]:
    """Call germline SNPs and indels with GATK HaplotypeCaller."""
    command = "gatk HaplotypeCaller -I {bam} -O {vcf} --caller {caller}"

@rule
def annotate(vcf: Vcf, db: str) -> AnnotatedVcf[annotated_vcf]:
    """Annotate variants against a reference database with bcftools."""
    command = "bcftools annotate -a {db} {vcf} -o {annotated_vcf}"

@rule
def merge_annotations(snp_ann: AnnotatedVcf, indel_ann: AnnotatedVcf) -> MergedVcf[merged_vcf]:
    """Merge SNP and indel annotated VCFs into one file."""
    command = "bcftools merge {snp_ann} {indel_ann} -o {merged_vcf}"


# --- pipeline definitions ---

def basic_pipeline(config, R):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=config.path)
    P.bam, P.align_log = R.align(P.fastq, ref=config.ref)
    P.sorted_bam = R.sort_bam(P.bam)
    P.counts, P.qc = R.quantify(P.sorted_bam, gene_model=config.gene_model)
    return P


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


# --- single-pipeline inspection ---

config = SimpleNamespace(path="/data/sample1.fastq.gz", ref="hg38", gene_model="gencode_v44")
P = basic_pipeline(config, R)
print(P)
P.save("/tmp/simple_dag_pipeline.txt")

# inspect resolved commands before running
P.resolve_paths("results")
for node in P.nodes:
    print(resolve_command(node))

# --- multi-sample DAG: basic pipeline ---

basic_configs = [
    SimpleNamespace(path="/data/sample1.fastq.gz", ref="hg38", gene_model="gencode_v44"),
    SimpleNamespace(path="/data/sample2.fastq.gz", ref="hg38", gene_model="gencode_v44"),
]

dag = DAG("results")
for config in basic_configs:
    P = basic_pipeline(config, R)
    dag.add(P, request=[P.counts])   # only run up to counts, skip qc

print(dag)
dag.save("/tmp/simple_dag_basic.txt")
dag.execute()

# pipeline_label (the P.xxx name) is the handle for each output
for node in dag.nodes:
    if node.pipeline_label and node.path:
        print(f"{node.pipeline_label}: {node.path}")

# --- multi-sample DAG: diamond pipeline ---

diamond_configs = [
    SimpleNamespace(path="/data/sample1.fastq.gz", ref="hg38"),
    SimpleNamespace(path="/data/sample2.fastq.gz", ref="hg38"),
]

dag2 = DAG("results")
for config in diamond_configs:
    dag2.add(diamond_pipeline(config, R))  # sinks = [merged] per sample

print(dag2)
dag2.save("/tmp/simple_dag_diamond.txt")
dag2.execute()

for node in dag2.nodes:
    if node.pipeline_label and node.path:
        print(f"{node.pipeline_label}: {node.path}")
