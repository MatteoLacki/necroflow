"""Luigi equivalent of the Necroflow paper's Listing 1 (`alignment_pipeline`).

Compare against ``simple_dag.py`` / the paper's ``paper_examples.py``: the same
three steps (link a FASTQ, align + log with bwa/samtools, sort the BAM) are
expressed as Luigi tasks. Unlike Necroflow, output paths and task identity are
conventions the developer writes by hand (here, via ``task_id``), there is no
typed distinction between a ``Bam`` and a ``SortedBam`` product, and skipping
the final sort step means requesting a different task rather than toggling a
config flag inside one workflow function.

Run with, e.g.::

    python luigi_alignment_pipeline.py
"""

import subprocess
from pathlib import Path

import luigi


class RawFastq(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"nodes/raw_fastq/{self.task_id}/reads.fastq.gz")

    def run(self):
        self.output().makedirs()
        subprocess.run(["ln", "-s", self.path, self.output().path], check=True)


class Align(luigi.Task):
    path = luigi.Parameter()
    reference = luigi.Parameter()
    threads = luigi.IntParameter(default=4)

    def requires(self):
        return RawFastq(path=self.path)

    def output(self):
        base = f"nodes/align/{self.task_id}"
        return {
            "bam": luigi.LocalTarget(f"{base}/aligned.bam"),
            "log": luigi.LocalTarget(f"{base}/align.log"),
        }

    def run(self):
        out = self.output()
        Path(out["bam"].path).parent.mkdir(parents=True, exist_ok=True)
        cmd = (
            f"bwa mem -t {self.threads} {self.reference} {self.input().path} "
            f"2> {out['log'].path} | samtools view -b -o {out['bam'].path}"
        )
        subprocess.run(cmd, shell=True, check=True)


class SortBam(luigi.Task):
    path = luigi.Parameter()
    reference = luigi.Parameter()
    threads = luigi.Parameter(default="all")
    ram = luigi.Parameter(default="16G")

    def requires(self):
        return Align(path=self.path, reference=self.reference)

    def output(self):
        return luigi.LocalTarget(f"nodes/sort_bam/{self.task_id}/sorted.bam")

    def run(self):
        self.output().makedirs()
        bam = self.input()["bam"].path
        cmd = (
            f"samtools sort -@ {self.threads} {bam} "
            f"-o {self.output().path} -m {self.ram}"
        )
        subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    luigi.build(
        [SortBam(path="/data/sample.fastq.gz", reference="/refs/hg38.fa")],
        local_scheduler=True,
    )
