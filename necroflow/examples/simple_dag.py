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

pipeline.plot()
