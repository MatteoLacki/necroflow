"""Fixed workload and independent byte checks, executed inside the test container."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path("/crash-test")
HERE = Path(__file__).resolve().parent
PYTHON = Path("/opt/necroflow/.venv/bin/python")
PAYLOADS = {"durable": b"necroflow durable\n", "completed": b"necroflow completed\n"}


def pipeline(P, config):
    from necroflow import NodeType, command, output

    class Text(NodeType):
        filename = "text.txt"

    @command("printf 'necroflow {phase}\\n' > {text}")
    def write_text(phase: str):
        text = output(Text)
        return text

    P.result = write_text(P, phase=config["phase"])


def job_args(phase):
    # Jobs, command logs, and observations live on the container's normal root.
    job = HERE / f"{phase}.toml"
    job.write_text(f'".pipeline" = "workload.py:pipeline"\nphase = "{phase}"\n')
    return [
        "--nodes-dir",
        str(ROOT / "nodes"),
        "--results-dir",
        str(ROOT / "results"),
        str(job),
    ]


def expected_files(phase):
    # Query the live CLI rather than guessing job/label/output path conventions.
    result = subprocess.run(
        [str(PYTHON), "-m", "necroflow.cli", "outputs", "--json", *job_args(phase)],
        cwd=HERE,
        capture_output=True,
        text=True,
        check=True,
    )
    requested = json.loads(result.stdout)["jobs"][0]["requested"]
    expected = hashlib.sha256(PAYLOADS[phase]).hexdigest()
    return {
        Path(item[key]).relative_to(ROOT).as_posix(): expected
        for item in requested
        for key in ("node_path", "result_path")
    }


def run_pipeline(phase):
    result = subprocess.run(
        [str(PYTHON), "-m", "necroflow.cli", *job_args(phase)],
        cwd=HERE,
        capture_output=True,
        text=True,
    )
    (HERE / f"{phase}-last-command.log").write_text(result.stdout + result.stderr)
    result.check_returncode()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def inspect():
    return {
        "files": {
            path.relative_to(ROOT).as_posix(): digest(path)
            for path in sorted(ROOT.rglob("*"))
            if path.is_file() and ".rip" not in path.parts
        },
        "states": {
            path.relative_to(ROOT).as_posix(): path.read_text()
            for path in sorted((ROOT / "nodes").rglob(".rip/state"))
        },
    }


def record(phase):
    expected = expected_files(phase)
    run_pipeline(phase)
    if phase == "durable":
        with (ROOT / "durable.txt").open("wb") as stream:
            stream.write(PAYLOADS[phase])
            stream.flush()
            os.fsync(stream.fileno())
        fd = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    else:
        (ROOT / "later.txt").write_bytes(PAYLOADS[phase])
    if any(digest(ROOT / name) != value for name, value in expected.items()):
        raise RuntimeError("completed pipeline produced unexpected result bytes")
    return {
        "phase": phase,
        "observation": inspect(),
        "expected_files": expected,
    }


def recover(phase):
    before = inspect()
    if phase == "durable":
        if (
            before["files"].get("durable.txt")
            != hashlib.sha256(PAYLOADS[phase]).hexdigest()
        ):
            raise RuntimeError("flushed file did not survive replay")
        if "later.txt" in before["files"]:
            raise RuntimeError("earlier crash state contains a later write")
    expected = expected_files(phase)
    if phase == "durable":
        if any(before["files"].get(name) != value for name, value in expected.items()):
            raise RuntimeError("flushed pipeline result did not survive replay")
    # A completion observation is deliberately not a durability assertion.
    run_pipeline(phase)
    after = inspect()
    if any(after["files"].get(name) != value for name, value in expected.items()):
        raise RuntimeError("recovery failed independent SHA-256 byte checks")
    return {"phase": phase, "before": before, "after": after, "verified": True}


if __name__ == "__main__":
    operation, phase = sys.argv[1:]
    if operation not in ("record", "recover") or phase not in PAYLOADS:
        raise SystemExit("expected record|recover durable|completed")
    print(json.dumps(record(phase) if operation == "record" else recover(phase)))
