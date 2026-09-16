"""Host Python port of the pinned nextflow-io/rnaseq-nf workflow."""

import json
import re
import shlex
import shutil
import time
import tomllib
from dataclasses import asdict
from pathlib import Path

from necroflow import DAG, Docker, NodeType, Pipeline, command, output

ROOT = Path(__file__).resolve().parent


def load_job(root=ROOT):
    """Return image pins and shared Docker settings from the job TOML."""
    return tomllib.loads((root / "job.toml").read_text())


class Read1(NodeType):
    filename = "reads_1.fq"


class Read2(NodeType):
    filename = "reads_2.fq"


class Reference(NodeType):
    filename = "reference.fa"


class Index(NodeType):
    filename = "index"


class ReportInput(NodeType):
    filename = None


class FastQC(ReportInput):
    filename = "fastqc"


class Quant(ReportInput):
    filename = "quant"


class Report(NodeType):
    filename = "report"


class ReportConfig(NodeType):
    filename = "multiqc_config"


@command("ln -s -- {path} {reads}")
def read1(path: str):
    reads = output(Read1)
    return reads


@command("ln -s -- {path} {reads}")
def read2(path: str):
    reads = output(Read2)
    return reads


@command("ln -s -- {path} {fasta}")
def reference(path: str):
    fasta = output(Reference)
    return fasta


@command("ln -s -- {path} {config}")
def report_config(path: str):
    config = output(ReportConfig)
    return config


@command(
    "{env}:salmon index --threads {threads} -t {fasta} -i {index}",
    threads=1,
    ram="2Gi",
)
def index(fasta: Reference, env: Docker):
    index = output(Index)
    return index


def fastqc_command(args):
    sample = args.config.sample
    r1, r2 = f"{sample}_1.fq", f"{sample}_2.fq"
    return "{env}:" + "\n".join(
        [
            "set -eu",
            f"ln -sf {shlex.quote(str(args.inputs.r1))} {shlex.quote(r1)}",
            f"ln -sf {shlex.quote(str(args.inputs.r2))} {shlex.quote(r2)}",
            f"mkdir -p {shlex.quote(str(args.outputs.qc))}",
            f"fastqc -o {shlex.quote(str(args.outputs.qc))} -f fastq -q {shlex.quote(r1)} {shlex.quote(r2)}",
        ]
    )


@command(fastqc_command, threads=1, ram="2Gi")
def fastqc(r1: Read1, r2: Read2, sample: str, env: Docker):
    qc = output(FastQC)
    return qc


@command(
    "{env}:salmon quant --threads {threads} --libType=U "
    "-i {index} -1 {r1} -2 {r2} -o {quant}",
    threads=1,
    ram="2Gi",
)
def quantify(index: Index, r1: Read1, r2: Read2, sample: str, env: Docker):
    quant = output(Quant)
    return quant


def multiqc_command(args):
    commands = ["set -eu"]
    staged = []
    for sample, qc, quant in zip(
        args.config.samples,
        args.inputs.reports[::2],
        args.inputs.reports[1::2],
        strict=True,
    ):
        for label, path in [(f"fastqc_{sample}_logs", qc), (f"quant_{sample}", quant)]:
            commands.append(f"ln -sfn {shlex.quote(str(path))} {shlex.quote(label)}")
            staged.append(shlex.quote(label))
    commands += [
        f"cp {shlex.quote(str(args.inputs.config))}/* .",
        'echo "custom_logo: $PWD/nextflow_logo.png" >> multiqc_config.yaml',
        f"multiqc --force -n multiqc_report.html -o {shlex.quote(str(args.outputs.report))} {' '.join(staged)}",
    ]
    return "{env}:" + "\n".join(commands)


@command(multiqc_command, threads=1, ram="2Gi")
def multiqc(
    reports: tuple[ReportInput, ...],
    config: ReportConfig,
    samples: tuple[str, ...],
    env: Docker,
):
    report = output(Report)
    return report


def build(root=ROOT, job=None):
    """Discover pairs using Python and share a single canonical index."""
    job = load_job() if job is None else job
    data = root / "data" / "short"
    dag = DAG(root / "work" / "necroflow" / "nodes")
    p = Pipeline(dag)
    docker = job["docker"]
    env = lambda tool: {
        "env": Docker(
            job["images"][tool]["image"],
            platform=docker["platform"],
            run_args=docker["run_args"],
        )
    }
    fasta = reference(p, path=str(data / "reference.fa"))
    shared_index = index(p, fasta, **env("salmon"))
    reports, samples, selected = [], [], {}
    for path in sorted(data.glob("*_1.fq")):
        sample = path.name.removesuffix("_1.fq")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", sample):
            raise ValueError(f"Unsafe sample name: {sample!r}")
        mate = path.with_name(f"{sample}_2.fq")
        if not mate.is_file():
            raise ValueError(f"Missing mate: {mate}")
        a, b = read1(p, path=str(path)), read2(p, path=str(mate))
        qc = fastqc(p, a, b, sample=sample, **env("fastqc"))
        quant = quantify(p, shared_index, a, b, sample=sample, **env("salmon"))
        for label, node in [(f"fastqc/{sample}", qc), (f"quant/{sample}", quant)]:
            p[label] = node
            selected[label] = node
        samples.append(sample)
        reports.extend([qc, quant])
    if not samples:
        raise ValueError(f"No paired reads in {data}")
    config = report_config(p, path=str(root / "downloads" / "upstream" / "multiqc"))
    p.report = multiqc(
        p, tuple(reports), config, samples=tuple(samples), **env("multiqc")
    )
    selected["multiqc"] = p.report
    p.finish()
    dag.require(selected.values())
    return dag, selected


def main():
    start = time.monotonic()
    dag, selected = build()
    report = dag.run(resource_caps={"threads": 2, "ram": 4 * 1024**3})
    destination = ROOT / "results" / "necroflow"
    destination.mkdir(parents=True, exist_ok=True)
    for name, node in selected.items():
        target = destination / name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(node.path, target)
    evidence = ROOT / "reports"
    evidence.mkdir(exist_ok=True)
    payload = {
        "elapsed_seconds": time.monotonic() - start,
        "calls": [asdict(event) for event in report.values()],
    }
    (evidence / "necroflow.json").write_text(json.dumps(payload, indent=2) + "\n")
    print(
        f"Necroflow: {sum(not e.cached for e in report.values())} executed, "
        f"{sum(e.cached for e in report.values())} cached; {payload['elapsed_seconds']:.2f}s"
    )
