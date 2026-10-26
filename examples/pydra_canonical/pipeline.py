"""Pydra 1.0a11 function-defined port of examples/canonical/pipeline.py.

Run ``make pydra-example`` from the repository root.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

from fileformats.generic import File
from pydra.compose import python, workflow
from pydra.engine.submitter import Submitter


@python.define
def import_text(source: File) -> File:
    """Copy a content-tracked external file into this task's working directory."""
    destination = Path.cwd() / "input.txt"
    shutil.copyfile(source.fspath, destination)
    return File(destination)


@python.define
def write_tool_config(text: str) -> File:
    """Materialize the same deterministic JSON as the Necroflow example."""
    destination = Path.cwd() / "tool_config.json"
    destination.write_text(text, encoding="utf-8")
    return File(destination)


@python.define
def process_text(raw_text: File, tool_config: File) -> File:
    """Uppercase text; retain the config dependency even though tr ignores it.

    The original canonical rule also declares this dependency without passing
    its contents to tr. Keeping it preserves the graph and cache contract.
    """
    destination = Path.cwd() / "processed.txt"
    with raw_text.fspath.open("rb") as source, destination.open("wb") as output:
        subprocess.run(
            ["tr", "[:lower:]", "[:upper:]"],
            stdin=source,
            stdout=output,
            env={**os.environ, "LC_ALL": "C"},
            check=True,
        )
    return File(destination)


@python.define
def summarize(processed_text: File) -> File:
    """Keep wc's count-plus-source-path format from the original example."""
    destination = Path.cwd() / "summary.txt"
    with destination.open("wb") as output:
        subprocess.run(
            ["wc", "-c", str(processed_text.fspath)], stdout=output, check=True
        )
    return File(destination)


@workflow.define(outputs=["raw", "tool_config", "processed", "summary"])
def canonical_pipeline(source: File, tool_text: str) -> tuple[File, File, File, File]:
    """Wire the four tasks with local variables and return the selected products."""
    raw = workflow.add(import_text(source=source)).out
    tool_config = workflow.add(write_tool_config(text=tool_text)).out
    processed = workflow.add(process_text(raw_text=raw, tool_config=tool_config)).out
    summary = workflow.add(summarize(processed_text=processed)).out
    return raw, tool_config, processed, summary


def run_pipeline(
    input_path: Path,
    cache_dir: Path,
    tool: dict | None = None,
    worker: str = "cf",
    n_procs: int = 2,
):
    """Execute locally; return Pydra's typed outputs with cached file paths."""
    tool_text = (
        json.dumps(
            tool if tool is not None else {"mode": "uppercase"},
            sort_keys=True,
            indent=2,
        )
        + "\n"
    )
    task = canonical_pipeline(
        source=File(input_path.expanduser().resolve()),
        tool_text=tool_text,
    )
    options = {"n_procs": n_procs} if worker == "cf" else {}
    with Submitter(
        worker=worker, cache_root=cache_dir.expanduser().resolve(), **options
    ) as submitter:
        return submitter(task, raise_errors=True).outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "canonical" / "input.txt",
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("build/pydra-canonical/cache")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("build/pydra-canonical/results")
    )
    parser.add_argument("--tool-json", default='{"mode": "uppercase"}')
    parser.add_argument("--worker", choices=("debug", "cf"), default="cf")
    parser.add_argument("--n-procs", type=int, default=2)
    args = parser.parse_args()
    if args.n_procs < 1:
        parser.error("--n-procs must be positive")
    try:
        tool = json.loads(args.tool_json)
    except json.JSONDecodeError as error:
        parser.error(f"invalid --tool-json: {error}")
    if not isinstance(tool, dict):
        parser.error("--tool-json must be a JSON object")

    outputs = run_pipeline(args.input, args.cache_dir, tool, args.worker, args.n_procs)
    output_dir = args.output_dir.expanduser().resolve()
    published = {}
    for label in ("raw", "tool_config", "processed", "summary"):
        source = getattr(outputs, label).fspath
        destination = output_dir / label / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        published[label] = {"path": str(destination), "cache_path": str(source)}
    byte_count = int(outputs.summary.fspath.read_text().split(maxsplit=1)[0])
    if byte_count != outputs.processed.fspath.stat().st_size:
        raise RuntimeError("summary byte count disagrees with processed file size")
    report = {"byte_count": byte_count, "outputs": published}
    (output_dir / "outputs.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
