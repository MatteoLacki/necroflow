from necroflow import rule, input_rule


@input_rule
def raw_fastq(*, config):
    pass


@rule(outputs=["bam", "log"])
def align(fastq, *, config):
    pass


@rule
def sort_bam(bam, *, config):
    pass


@rule(outputs=["counts", "qc_report"])
def quantify(bam, *, config):
    pass


# --- build DAG ---

fastq = raw_fastq(config={"path": "/data/sample.fastq.gz"})

bam, align_log = align(fastq, config={"ref": "hg38", "threads": 8})

sorted_bam = sort_bam(bam, config={})

counts, qc = quantify(sorted_bam, config={"gene_model": "gencode_v44"})

# --- inspect structure ---

print(f"fastq         rule={fastq.rule_name!r}  parents={fastq.parents}")
print(f"bam           rule={bam.rule_name!r}    output_name={bam.output_name!r}")
print(f"align_log     rule={align_log.rule_name!r}    output_name={align_log.output_name!r}")
print(f"sorted_bam    parents=[{sorted_bam.parents[0].rule_name!r} / {sorted_bam.parents[0].output_name!r}]")
print(f"counts        output_name={counts.output_name!r}")
print(f"qc            output_name={qc.output_name!r}")
print(f"qc parents:   {[p.rule_name for p in qc.parents]}")
