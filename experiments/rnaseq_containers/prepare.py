"""Download, subset, and run the pinned baseline without modifying upstream."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import urllib.request

from pipeline import ROOT, load_job

REFERENCE = "ggal_1_48850000_49020000.Ggal71.500bpflank.fa"
SAMPLES = ("ggal_gut", "ggal_liver")


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fetch(spec, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and sha256(destination) == spec["sha256"]:
        return
    partial = destination.with_suffix(destination.suffix + ".partial")
    print(f"Downloading {spec['url']}", flush=True)
    with (
        urllib.request.urlopen(spec["url"], timeout=120) as response,
        partial.open("wb") as output,
    ):
        shutil.copyfileobj(response, output)
    if sha256(partial) != spec["sha256"]:
        partial.unlink()
        raise ValueError(f"Checksum mismatch: {destination.name}")
    partial.replace(destination)


def download():
    spec = json.loads((ROOT / "sources.json").read_text())["upstream"]
    archive = ROOT / "downloads" / "upstream.tar.gz"
    fetch(spec, archive)
    upstream = ROOT / "downloads" / "upstream"
    marker = upstream / ".source-sha256"
    if marker.exists() and marker.read_text().strip() == spec["sha256"]:
        return
    with tarfile.open(archive) as source:
        source.extractall(ROOT / "downloads", filter="data")
    extracted = ROOT / "downloads" / f"rnaseq-nf-{spec['revision']}"
    if upstream.exists():
        shutil.rmtree(upstream)
    extracted.rename(upstream)
    marker.write_text(spec["sha256"] + "\n")


def fastq_record(stream):
    header = stream.readline()
    if not header:
        return None
    sequence, separator, quality = (stream.readline() for _ in range(3))
    if (
        not header.startswith(b"@")
        or not separator.startswith(b"+")
        or not sequence.endswith(b"\n")
        or not quality.endswith(b"\n")
        or len(sequence.rstrip(b"\r\n")) != len(quality.rstrip(b"\r\n"))
    ):
        raise ValueError("Malformed or incomplete FASTQ record")
    return header, sequence, separator, quality


def pair_id(header):
    value = header.split()[0]
    return value[:-2] if value.endswith((b"/1", b"/2")) else value


def subset_pair(first, second, out_first, out_second, count):
    """Retain complete corresponding pairs; never cut FASTQ records."""
    if count <= 0:
        raise ValueError("read-pairs must be positive")
    with (
        first.open("rb") as a,
        second.open("rb") as b,
        out_first.open("wb") as x,
        out_second.open("wb") as y,
    ):
        for _ in range(count):
            left, right = fastq_record(a), fastq_record(b)
            if left is None or right is None:
                raise ValueError(f"Input contains fewer than {count} complete pairs")
            if pair_id(left[0]) != pair_id(right[0]):
                raise ValueError("FASTQ mate identifiers differ")
            x.writelines(left)
            y.writelines(right)


def preprocess(count):
    source = ROOT / "downloads" / "upstream" / "data" / "ggal"
    files = [REFERENCE] + [
        f"{sample}_{mate}.fq" for sample in SAMPLES for mate in (1, 2)
    ]
    expected = {
        "read_pairs": count,
        "source_sha256": {name: sha256(source / name) for name in files},
    }
    destination = ROOT / "data" / "short"
    manifest = destination / "manifest.json"
    if manifest.exists():
        prior = json.loads(manifest.read_text())
        if all(prior.get(k) == v for k, v in expected.items()) and all(
            (destination / name).exists() and sha256(destination / name) == digest
            for name, digest in prior["subset_sha256"].items()
        ):
            return
    staging = ROOT / "data" / "preparing"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    try:
        for sample in SAMPLES:
            names = [f"{sample}_{mate}.fq" for mate in (1, 2)]
            subset_pair(
                *(source / n for n in names), *(staging / n for n in names), count
            )
        shutil.copyfile(source / REFERENCE, staging / "reference.fa")
        expected["subset_sha256"] = {
            p.name: sha256(p) for p in sorted(staging.iterdir())
        }
        (staging / "manifest.json").write_text(json.dumps(expected, indent=2) + "\n")
        if destination.exists():
            shutil.rmtree(destination)
        staging.rename(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    print(f"Prepared {count} pairs × {len(SAMPLES)} samples")


def setup():
    spec = json.loads((ROOT / "sources.json").read_text())["nextflow"]
    executable = ROOT / "tools" / "nextflow"
    fetch(spec, executable)
    executable.chmod(0o755)
    versions = {}
    job = load_job()
    platform = job["docker"]["platform"]
    for tool, spec in job["images"].items():
        image = spec["image"]
        found = subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True
        )
        if found.returncode:
            subprocess.run(
                ["docker", "pull", f"--platform={platform}", image], check=True
            )
        info = json.loads(
            subprocess.check_output(["docker", "image", "inspect", image])
        )[0]
        if info["Architecture"] != "amd64" or info["Os"] != "linux":
            raise ValueError(f"Unexpected image platform for {tool}")
        version = subprocess.check_output(
            [
                "docker",
                "run",
                "--rm",
                "--network=none",
                "--entrypoint",
                tool,
                image,
                "--version",
            ],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
        if spec["version"] not in version:
            raise ValueError(f"Unexpected {tool} version: {version}")
        versions[tool] = {
            "version": version,
            "image": image,
            "size_bytes": info["Size"],
        }
    reports = ROOT / "reports"
    reports.mkdir(exist_ok=True)
    (reports / "images.json").write_text(json.dumps(versions, indent=2) + "\n")


def nextflow():
    work = ROOT / "work" / "nextflow"
    work.mkdir(parents=True, exist_ok=True)
    pins = load_job()["images"]
    # JSON string quoting also produces valid Groovy double-quoted strings here.
    quote = json.dumps
    config = work / "experiment.config"
    text = """docker.enabled = true
process.cpus = 1
process.memory = '2 GB'
executor.queueSize = 2
docker.runOptions = '--cpus 1 --memory 2g --network none --user %s:%s -e HOME=/tmp'
trace.enabled = true
trace.overwrite = true
trace.fields = 'task_id,name,status,exit,realtime,workdir'
trace.file = %s
""" % (os.getuid(), os.getgid(), quote(str(ROOT / "reports" / "nextflow-trace.tsv")))
    for process, tool in [
        ("FASTQC", "fastqc"),
        ("INDEX", "salmon"),
        ("QUANT", "salmon"),
        ("MULTIQC", "multiqc"),
    ]:
        text += f"process {{ withName: {process} {{ container = {quote(pins[tool]['image'])} }} }}\n"
    config.write_text(text)
    env = dict(
        os.environ,
        NXF_HOME=str(ROOT / "tools" / "nxf-home"),
        NXF_VER="25.10.0",
        NXF_OPTS="-Xms128m -Xmx1g",
        NXF_ANSI_LOG="false",
    )
    reports = ROOT / "reports"
    reports.mkdir(exist_ok=True)
    start = time.monotonic()
    subprocess.run(
        [
            str(ROOT / "tools" / "nextflow"),
            "run",
            str(ROOT / "downloads" / "upstream" / "main.nf"),
            "-c",
            str(config),
            "-resume",
            "-work-dir",
            str(work / "tasks"),
            "-output-dir",
            str(ROOT / "results" / "nextflow"),
            "--reads",
            str(ROOT / "data" / "short" / "ggal_*_{1,2}.fq"),
            "--transcriptome",
            str(ROOT / "data" / "short" / "reference.fa"),
        ],
        cwd=work,
        env=env,
        check=True,
    )
    (reports / "nextflow.json").write_text(
        json.dumps({"elapsed_seconds": time.monotonic() - start}, indent=2) + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=["download", "preprocess", "setup", "nextflow"]
    )
    parser.add_argument("--read-pairs", type=int, default=1000)
    args = parser.parse_args()
    if args.action == "preprocess":
        preprocess(args.read_pairs)
    else:
        globals()[args.action]()
