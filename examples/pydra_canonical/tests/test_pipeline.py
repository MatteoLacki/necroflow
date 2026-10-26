"""Cross-engine behavior checks, run in the example's isolated environment."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

EXAMPLE = Path(__file__).resolve().parents[1]
CANONICAL = EXAMPLE.parent / "canonical"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pydra_example = load_module("pydra_canonical_pipeline", EXAMPLE / "pipeline.py")


@pytest.mark.parametrize("worker", ["debug", "cf"])
@pytest.mark.parametrize("filename", ["input.txt", "input_alt.txt"])
def test_matches_necroflow_canonical_pipeline(tmp_path, monkeypatch, worker, filename):
    """Both engines must produce the same data and byte count from the same DAG."""
    from necroflow import DAG, Pipeline

    monkeypatch.setenv("LC_ALL", "C")
    original = load_module("necroflow_canonical_pipeline", CANONICAL / "pipeline.py")
    source = tmp_path / "input with spaces.txt"
    source.write_bytes((CANONICAL / filename).read_bytes())
    tool = {"mode": "uppercase", "nested": {"z": 2, "a": 1}}

    dag = DAG(tmp_path / "necroflow nodes")
    pipeline = Pipeline(dag)
    original.canonical_pipeline(pipeline, {"input": str(source), "tool": tool})
    pipeline.finish()
    dag.require([pipeline.summary])
    dag.run(resource_caps={"threads": 2})

    result = pydra_example.run_pipeline(source, tmp_path / "pydra cache", tool, worker)
    for label in ("raw", "tool_config", "processed"):
        assert (
            getattr(result, label).fspath.read_bytes()
            == getattr(pipeline, label).path.read_bytes()
        )
    # wc embeds the engine-specific source path; compare its numeric value.
    pydra_count = int(result.summary.fspath.read_text().split(maxsplit=1)[0])
    necroflow_count = int(pipeline.summary.path.read_text().split(maxsplit=1)[0])
    assert pydra_count == necroflow_count == len(source.read_bytes())
    assert result.processed.fspath.read_bytes() == source.read_bytes().upper()


def result_files(cache):
    return {path: path.stat().st_mtime_ns for path in cache.rglob("_result.pklz")}


def test_cli_reuses_cache_across_processes(tmp_path):
    """A fresh interpreter must reuse completed tasks without rewriting results."""
    cache = tmp_path / "cache"
    results = tmp_path / "results"
    command = [
        sys.executable,
        str(EXAMPLE / "pipeline.py"),
        "--cache-dir",
        str(cache),
        "--output-dir",
        str(results),
    ]
    first = subprocess.run(command, check=True, text=True, capture_output=True)
    before = result_files(cache)
    assert len(before) == 5  # four tasks plus the workflow
    second = subprocess.run(command, check=True, text=True, capture_output=True)
    assert result_files(cache) == before
    assert json.loads(first.stdout) == json.loads(second.stdout)
    report = json.loads((results / "outputs.json").read_text())
    assert report["byte_count"] == len((CANONICAL / "input.txt").read_bytes())
    assert (
        results / "processed" / "processed.txt"
    ).read_bytes() == b"HELLO NECROFLOW\n"


def test_editing_source_at_same_path_invalidates_cached_work(tmp_path):
    """File content changes must select new results even when the path is stable."""
    source = tmp_path / "source.txt"
    source.write_bytes(b"first\n")
    cache = tmp_path / "cache"
    first = pydra_example.run_pipeline(source, cache, worker="debug")
    before = result_files(cache)
    source.write_bytes(b"second version\n")
    second = pydra_example.run_pipeline(source, cache, worker="debug")
    assert first.processed.fspath.read_bytes() == b"FIRST\n"
    assert second.processed.fspath.read_bytes() == b"SECOND VERSION\n"
    assert first.processed.fspath != second.processed.fspath
    after = result_files(cache)
    assert len(after) == len(before) + 4  # config task alone is reusable
    assert all(after[path] == mtime for path, mtime in before.items())


def test_config_dependency_is_retained_when_tr_ignores_it(tmp_path):
    """Changing config must rerun processing while reusing the imported input."""
    cache = tmp_path / "cache"
    first = pydra_example.run_pipeline(
        CANONICAL / "input.txt", cache, {"mode": "uppercase"}, "debug"
    )
    before = result_files(cache)
    second = pydra_example.run_pipeline(
        CANONICAL / "input.txt", cache, {"mode": "changed"}, "debug"
    )
    assert json.loads(second.tool_config.fspath.read_text()) == {"mode": "changed"}
    assert first.processed.fspath.read_bytes() == second.processed.fspath.read_bytes()
    after = result_files(cache)
    new_tasks = [
        path.parent
        for path in after.keys() - before.keys()
        if path.parent.name.startswith("python-")
    ]
    new_files = {path.name for directory in new_tasks for path in directory.iterdir()}
    assert {"tool_config.json", "processed.txt"} <= new_files
    assert not any((directory / "input.txt").exists() for directory in new_tasks)
    assert all(after[path] == mtime for path, mtime in before.items())


@pytest.mark.parametrize("worker", ["debug", "cf"])
def test_paper_splitter_examples_map_group_and_reduce(tmp_path, worker):
    """Splitters must match product/zip semantics and combine must only group.

    The sine workflow also maps over an upstream task's list, then performs a
    separate numerical reduction after combining the per-index terms.
    """
    example = load_module("pydra_splitter_example", EXAMPLE / "splitter_example.py")
    result = example.run_demo(tmp_path / "cache", worker=worker)
    assert result["outer_product"] == [11, 101, 12, 102]
    assert result["paired"] == [11, 102]
    assert result["grouped_by_x"] == [[11, 101], [12, 102]]
    assert result["sine"]["indices"] == [0, 1, 2]
    assert len(result["sine"]["terms"]) == 3
    assert result["sine"]["approximation"] == pytest.approx(1.0045248555348174)
    assert sum(result["sine"]["terms"]) == result["sine"]["approximation"]
