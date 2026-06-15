try:
    _ip = get_ipython()
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from types import SimpleNamespace
from necroflow import (
    node_types,
    Inputs,
    Outputs,
    Constraints,
    Rules,
    resolve_command,
    Pipeline,
    DAG,
)

# --- node types ---

Fastq, Bam, Log, Counts, QcReport, Vcf, AnnotatedVcf, MergedVcf = node_types(
    "fastq=reads.fastq.gz"
    " bam=aligned.bam"
    " log=align.log"
    " counts=counts.txt"
    " qc_report=qc.txt"
    " vcf=variants.vcf.gz"
    " annotated_vcf=annotated.vcf.gz"
    " merged_vcf=merged.vcf.gz"
)


class SortedBam(Bam):
    name = "sorted.bam"


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
P.plot()

# inspect resolved commands before running
P.resolve_paths("/results")
for node in P.nodes:
    print(resolve_command(node))

# --- multi-sample DAG: basic pipeline ---

basic_configs = [
    SimpleNamespace(path="/data/sample1.fastq.gz", ref="hg38", gene_model="gencode_v44"),
    SimpleNamespace(path="/data/sample2.fastq.gz", ref="hg38", gene_model="gencode_v44"),
]

dag = DAG()
for config in basic_configs:
    dag.add(basic_pipeline(config, R))   # sinks = [counts, qc] per sample

print(dag)
dag.plot()
dag.execute("/results")

# --- multi-sample DAG: diamond pipeline ---

diamond_configs = [
    SimpleNamespace(path="/data/sample1.fastq.gz", ref="hg38"),
    SimpleNamespace(path="/data/sample2.fastq.gz", ref="hg38"),
]

dag2 = DAG()
for config in diamond_configs:
    dag2.add(diamond_pipeline(config, R))  # sinks = [merged] per sample

print(dag2)
dag2.plot()
dag2.execute("/results")
