try:
    _ip = get_ipython()
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from necroflow import Node, Pipeline, rule


@rule
def raw_fastq(*, path):
    return Node()


@rule(threads=4)
def align(fastq: Node, *, ref):
    return Node("bam"), Node("log")


@rule
def sort_bam(bam: Node):
    return Node()


@rule
def quantify(bam: Node, *, gene_model):
    return Node("counts"), Node("qc_report")


# --- build DAG ---

pipeline = Pipeline()
with pipeline:
    fastq = raw_fastq(path="/data/sample.fastq.gz")
    bam, align_log = align(fastq, ref="hg38")
    sorted_bam = sort_bam(bam)
    counts, qc = quantify(sorted_bam, gene_model="gencode_v44")

# --- inspect ---

print(f"fastq       rule={fastq.rule.__name__!r}  parents={fastq.parents}")
print(
    f"bam         output_name={bam.output_name!r}  parents=[{bam.parents[0].rule.__name__!r}]"
)
print(f"align_log   output_name={align_log.output_name!r}")
print(
    f"sorted_bam  parents=[{sorted_bam.parents[0].output_name!r} from {sorted_bam.parents[0].rule.__name__!r}]"
)
print(f"counts      output_name={counts.output_name!r}  config={counts.config}")
print(f"qc          output_name={qc.output_name!r}")

print(pipeline)
pipeline.plot()

# --- diamond DAG ---


@rule
def call_variants(bam: Node, *, caller):
    return Node()


@rule
def annotate(vcf: Node, *, db):
    return Node()


@rule
def merge_annotations(snp_ann: Node, indel_ann: Node):
    return Node()


diamond_pipeline = Pipeline()
with diamond_pipeline:
    fastq2 = raw_fastq(path="/data/sample2.fastq.gz")
    bam2, _ = align(fastq2, ref="hg38")
    snp_vcf = call_variants(bam2, caller="haplotypecaller")
    indel_vcf = call_variants(bam2, caller="mutect2")
    snp_ann = annotate(snp_vcf, db="dbsnp")
    indel_ann = annotate(indel_vcf, db="clinvar")
    merged = merge_annotations(snp_ann, indel_ann)

print(diamond_pipeline)
diamond_pipeline.plot()
