"""
Custom invalidation example.

Run from the necroflow/ directory:
    source .venv/bin/activate
    python examples/custom_invalidation.py

The PreparedText NodeType hashes an external dependency file. The first
execute() writes the output. The example then edits the dependency file and
runs execute() again; the changed invalidation token marks the node STALE and
reruns the command at the same content-addressed output path.
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path

from necroflow import DAG, Inputs, NodeType, Outputs, Pipeline, Rules


WORK = Path("/tmp/necroflow_custom_invalidation")
OUTDIR = WORK / "results"
DEPENDENCY = WORK / "dependency.txt"


def sha256_of_dependency(node) -> str:
    return hashlib.sha256(Path(node.config["dependency"]).read_bytes()).hexdigest()


class PreparedText(NodeType):
    filename = "prepared.txt"
    invalidator = sha256_of_dependency


r = Rules()


@r.command("cat {dependency} > {prepared_text}")
def prepare_text(dependency: str):
    return PreparedText[prepared_text]


def build_pipeline() -> Pipeline:
    P = Pipeline()
    P.prepared = r.prepare_text(dependency=str(DEPENDENCY))
    return P


def run_once() -> Path:
    P = build_pipeline()
    dag = DAG(OUTDIR)
    dag.add(P)
    dag.execute()
    return P.prepared.path


if __name__ == "__main__":
    WORK.mkdir(parents=True, exist_ok=True)
    DEPENDENCY.write_text("version 1\n")

    first_path = run_once()
    first_mtime = first_path.stat().st_mtime
    print(f"first run:  {first_path} -> {first_path.read_text().strip()}")

    time.sleep(0.05)
    DEPENDENCY.write_text("version 2\n")

    second_path = run_once()
    second_mtime = second_path.stat().st_mtime
    print(f"second run: {second_path} -> {second_path.read_text().strip()}")
    print(f"same path:  {first_path == second_path}")
    print(f"reran:      {second_mtime > first_mtime}")
