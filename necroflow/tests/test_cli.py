"""Tests for CLI internals: _create_link_outputs, manifest keys."""
import tomlkit
import pytest
from pathlib import Path
from necroflow import NodeType, Inputs, Outputs, Rules, Pipeline
from necroflow.dag import resolve_paths
from necroflow.cli import _create_link_outputs
from necroflow.pipeline import _sinks


class Out(NodeType): name = "out.txt"
class Log(NodeType): name = "run.log"


R = Rules()
R.register("step1", Inputs(v=str),     Outputs(out=Out),      "echo {v} > {out}")
R.register("step2", Inputs(out=Out),   Outputs(log=Log),      "cat {out} > {log}")


def _make_pipeline_with_outputs(tmp_path) -> tuple[Pipeline, Path]:
    """Build a pipeline, resolve paths, and create real output files."""
    P = Pipeline()
    P.out = R.step1(v="hello")
    P.log = R.step2(P.out)
    resolve_paths(P.nodes, tmp_path)
    for node in P.nodes:
        node.path.parent.mkdir(parents=True, exist_ok=True)
        node.path.touch()
    return P, tmp_path


# ── symlink creation ──────────────────────────────────────────────────────────

def test_combo_dir_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    assert (outdir / "run1").is_dir()


def test_symlinks_created_for_existing_outputs(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    combo_dir = outdir / "run1"
    symlinks = list(combo_dir.rglob("*.txt")) + list(combo_dir.rglob("*.log"))
    assert len(symlinks) > 0
    assert all(f.is_symlink() for f in symlinks)


def test_symlinks_point_to_real_files(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    combo_dir = outdir / "run1"
    for link in combo_dir.rglob("*"):
        if link.is_symlink():
            assert link.resolve().exists()


def test_skips_missing_outputs(tmp_path):
    P = Pipeline()
    P.out = R.step1(v="hello")
    resolve_paths(P.nodes, tmp_path)
    # do NOT create the output file
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir=tmp_path, combos=combos)
    combo_dir = tmp_path / "run1"
    assert combo_dir.is_dir()  # dir still created
    assert not any(combo_dir.rglob("*.txt"))  # but no symlinks for missing output


# ── manifest ─────────────────────────────────────────────────────────────────

def test_manifest_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    assert (outdir / "run1" / "manifest.toml").exists()


def test_manifest_keys_are_pipeline_labels(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _sinks(P)
    combos = [("run1", P, sinks)]
    _create_link_outputs(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # sink has pipeline_label "log" (the last node P.log = R.step2(...))
    assert "log" in keys


def test_manifest_only_sinks(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _sinks(P)
    combos = [("run1", P, sinks)]
    _create_link_outputs(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # "out" is intermediate (P.out), "log" is the sink (P.log)
    assert "out" not in keys
    assert "log" in keys


# ── multiple combos ───────────────────────────────────────────────────────────

def test_multiple_combos(tmp_path):
    P1 = Pipeline()
    P1.out = R.step1(v="alpha")
    resolve_paths(P1.nodes, tmp_path)
    for n in P1.nodes:
        n.path.parent.mkdir(parents=True, exist_ok=True)
        n.path.touch()

    P2 = Pipeline()
    P2.out = R.step1(v="beta")
    resolve_paths(P2.nodes, tmp_path)
    for n in P2.nodes:
        n.path.parent.mkdir(parents=True, exist_ok=True)
        n.path.touch()

    combos = [("combo_alpha", P1, _sinks(P1)), ("combo_beta", P2, _sinks(P2))]
    _create_link_outputs(tmp_path, combos)
    assert (tmp_path / "combo_alpha").is_dir()
    assert (tmp_path / "combo_beta").is_dir()
