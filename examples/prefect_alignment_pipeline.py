"""Prefect equivalent of the Necroflow paper's Listing 1 (`alignment_pipeline`).

Compare against ``simple_dag.py`` / the paper's ``paper_examples.py``: the same
three steps (link a FASTQ, align + log with bwa/samtools, sort the BAM) are
expressed as Prefect tasks composed inside a flow. The flow itself reads a lot
like Necroflow's workflow function -- plain Python, real ``if`` statements --
but the output paths (``nodes/align/aligned.bam``, etc.) are constants chosen
by the developer rather than derived from each task's lineage. Running this
flow twice with a different ``reference`` would silently overwrite the first
run's outputs unless the paths were manually parameterized.

Run with, e.g.::

    python prefect_alignment_pipeline.py
"""

import subprocess
from pathlib import Path

from prefect import flow, task


@task
def raw_fastq(path: str) -> Path:
    out = Path("nodes/raw_fastq/reads.fastq.gz")
    out.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["ln", "-s", path, str(out)], check=True)
    return out


@task
def align(fastq: Path, reference: str, threads: int = 4) -> tuple[Path, Path]:
    bam = Path("nodes/align/aligned.bam")
    log = Path("nodes/align/align.log")
    bam.parent.mkdir(parents=True, exist_ok=True)
    cmd = (
        f"bwa mem -t {threads} {reference} {fastq} "
        f"2> {log} | samtools view -b -o {bam}"
    )
    subprocess.run(cmd, shell=True, check=True)
    return bam, log


@task
def sort_bam(bam: Path, threads: str = "all", ram: str = "16G") -> Path:
    sorted_bam = Path("nodes/sort_bam/sorted.bam")
    sorted_bam.parent.mkdir(parents=True, exist_ok=True)
    cmd = f"samtools sort -@ {threads} {bam} -o {sorted_bam} -m {ram}"
    subprocess.run(cmd, shell=True, check=True)
    return sorted_bam


@flow
def alignment_pipeline(path: str, reference: str, sort: bool = True):
    fastq = raw_fastq(path)
    bam, align_log = align(fastq, reference)
    if sort:
        sort_bam(bam)


if __name__ == "__main__":
    alignment_pipeline(path="/data/sample.fastq.gz", reference="/refs/hg38.fa", sort=True)
