"""Explicit Docker integration checks; run after `make compare`."""

import csv
import json
import os
from pathlib import Path
import subprocess
import tempfile

from necroflow import DAG, Docker, NodeType, Pipeline, command, output

from compare import main as compare_results
from pipeline import ROOT, build, load_job, main as run_pipeline
from prepare import nextflow


class Probe(NodeType):
    filename = "probe.txt"


@command("{env}:printf hello > {probe}", threads=1)
def success(env: Docker):
    probe = output(Probe)
    return probe


@command("{env}:exit 7", threads=1)
def failure(env: Docker):
    probe = output(Probe)
    return probe


@command("{env}:true", threads=1)
def missing(env: Docker):
    probe = output(Probe)
    return probe


def probe(rule, root):
    dag = DAG(root / rule.__name__)
    p = Pipeline(dag)
    job = load_job()
    env = Docker(
        job["images"]["fastqc"]["image"],
        platform=job["docker"]["platform"],
        run_args=job["docker"]["run_args"],
    )
    node = rule(p, env=env)
    p.finish()
    dag.require([node])
    return dag, node


def run_dag():
    dag, selected = build()
    report = dag.run(resource_caps={"threads": 2, "ram": 4 * 1024**3})
    return report, selected


def main():
    report, selected = run_dag()
    assert all(
        event.cached for event in report.values()
    ), "Necroflow resume reran tasks"
    nextflow()
    with (ROOT / "reports" / "nextflow-trace.tsv").open() as stream:
        trace = list(csv.DictReader(stream, delimiter="\t"))
    assert len(trace) == 6 and all(
        row["status"] == "CACHED" for row in trace
    ), "Nextflow resume reran tasks"

    first = ROOT / "data" / "short" / "ggal_gut_1.fq"
    original = first.read_bytes()
    original_stat = first.stat()
    changed = original.splitlines(keepends=True)
    changed[3] = (b"!" if changed[3][:1] != b"!" else b"I") + changed[3][1:]
    try:
        first.write_bytes(b"".join(changed))
        report, selected = run_dag()
        executed = {key for key, event in report.items() if not event.cached}
        expected = {
            selected[label].rule_call.relative_path.as_posix()
            for label in ("fastqc/ggal_gut", "quant/ggal_gut", "multiqc")
        }
        assert executed == expected, f"Unexpected invalidation: {executed ^ expected}"
    finally:
        first.write_bytes(original)
        os.utime(first, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
        run_pipeline()

    with tempfile.TemporaryDirectory(
        prefix="container probe ", dir=ROOT / "work"
    ) as temporary:
        root = Path(temporary)
        dag, node = probe(success, root)
        dag.run()
        assert node.path.read_text() == "hello"
        assert node.path.stat().st_uid == os.getuid()
        for rule, error in [
            (failure, subprocess.CalledProcessError),
            (missing, RuntimeError),
        ]:
            dag, node = probe(rule, root)
            try:
                dag.run()
            except error as exc:
                if rule is failure:
                    assert exc.returncode == 7
                else:
                    assert "output missing" in str(exc)
                assert node.rule_call.state_file.read_text() == "failed"
            else:
                raise AssertionError(f"{rule.__name__} unexpectedly succeeded")
    for path in (ROOT / "results" / "necroflow").rglob("*"):
        assert path.stat().st_uid == os.getuid(), f"Wrong output owner: {path}"
    compare_results()
    result = {
        "resume": "both engines cached all scientific tasks",
        "input_edit": "only gut QC, gut quantification, and MultiQC reran",
        "docker": "spaces, ownership, nonzero exit, missing output passed",
    }
    (ROOT / "reports" / "integration.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
