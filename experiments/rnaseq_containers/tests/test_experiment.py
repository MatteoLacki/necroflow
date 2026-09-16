"""Offline tests for experiment boundaries and provenance."""

import copy
import subprocess

import pytest

from necroflow.containers import docker_argv

from compare import compare_quant
from pipeline import build, load_job
from prepare import subset_pair


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "space in path"
    data = root / "data" / "short"
    data.mkdir(parents=True)
    (data / "reference.fa").write_text(">transcript\nACGT\n")
    for sample in ("ggal_gut", "ggal_liver"):
        for mate in (1, 2):
            (data / f"{sample}_{mate}.fq").write_text(f"@read/{mate}\nACGT\n+\nIIII\n")
    return root


def test_subset_preserves_pairs_and_records(tmp_path):
    """Reduction retains complete matching mates, in original order."""
    paths = [tmp_path / name for name in ("r1", "r2", "out1", "out2")]
    for mate, path in enumerate(paths[:2], 1):
        path.write_text("".join(f"@r{i}/{mate}\nAC\n+\nII\n" for i in range(3)))
    subset_pair(*paths, 2)
    for source, reduced in zip(paths[:2], paths[2:]):
        assert reduced.read_bytes() == b"".join(
            source.read_bytes().splitlines(keepends=True)[:8]
        )


@pytest.mark.parametrize("bad", ["@different/2\nAC\n+\nII\n", "@r0/2\nAC\n+\nI\n", ""])
def test_subset_rejects_bad_pair(tmp_path, bad):
    """Invalid or insufficient mates must not masquerade as usable paired reads."""
    paths = [tmp_path / name for name in ("r1", "r2", "out1", "out2")]
    paths[0].write_text("@r0/1\nAC\n+\nII\n")
    paths[1].write_text(bad)
    with pytest.raises(ValueError):
        subset_pair(*paths, 1)


def test_shared_index_and_image_provenance(workspace):
    """A tool image changes its consumers' identity, without invalidating other branches."""
    before, selected = build(workspace)
    assert len([c for c in before.calls.values() if c.rule.__name__ == "index"]) == 1
    job = copy.deepcopy(load_job())
    job["images"]["fastqc"]["image"] = "example/fastqc@sha256:" + "0" * 64
    after, changed = build(workspace, job)
    for sample in ("ggal_gut", "ggal_liver"):
        assert (
            selected[f"fastqc/{sample}"].relative_path
            != changed[f"fastqc/{sample}"].relative_path
        )
        assert (
            selected[f"quant/{sample}"].relative_path
            == changed[f"quant/{sample}"].relative_path
        )
    assert selected["multiqc"].relative_path != changed["multiqc"].relative_path


def test_docker_path_arguments_and_failure(workspace, monkeypatch):
    """Host paths survive spaces and a failed container remains a failed task."""
    dag, selected = build(workspace)
    call = selected["fastqc/ggal_gut"].rule_call
    argv = docker_argv(call, call.container, call.resolve())
    assert f"--workdir={call.workdir}" in argv
    reads = workspace / "data" / "short" / "ggal_gut_1.fq"
    read_node = call.inputs["r1"]
    read_node.path.parent.mkdir(parents=True)
    read_node.path.symlink_to(reads)
    argv = docker_argv(call, call.container, call.resolve())
    assert f"type=bind,src={reads},dst={read_node.path},readonly" in argv
    assert "'" in argv[-1]  # Paths are quoted inside the container's shell too.
    assert "--network=none" in argv

    def fail(command, **kwargs):
        raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError) as error:
        call.run(call.log_path())
    assert error.value.returncode == 7


def test_quant_comparison_detects_scientific_change(tmp_path):
    """Comparison rejects changed abundance, regardless of output packaging."""
    a, b = tmp_path / "a.sf", tmp_path / "b.sf"
    text = "Name\tLength\tEffectiveLength\tTPM\tNumReads\nt\t100\t80\t1000000\t5\n"
    a.write_text(text)
    b.write_text(text)
    assert compare_quant(a, b) == 1
    b.write_text(text.replace("\t5\n", "\t6\n"))
    with pytest.raises(AssertionError, match="NumReads"):
        compare_quant(a, b)


def test_input_import_stays_on_host(workspace):
    """Input symlink rules must work without Docker, including paths with spaces."""
    dag, _ = build(workspace)
    call = next(c for c in dag.calls.values() if c.rule.__name__ == "reference")
    assert call.container is None
    call.run(call.log_path())
    assert call.outputs[0].path.is_symlink()
    assert call.outputs[0].path.read_text() == ">transcript\nACGT\n"


def test_preprocess_is_repeatable_and_count_sensitive(tmp_path, monkeypatch):
    """Unchanged preparation preserves mtimes; a new pair count rebuilds complete pairs."""
    import prepare

    monkeypatch.setattr(prepare, "ROOT", tmp_path)
    source = tmp_path / "downloads" / "upstream" / "data" / "ggal"
    source.mkdir(parents=True)
    (source / prepare.REFERENCE).write_text(">t\nACGT\n")
    for sample in prepare.SAMPLES:
        for mate in (1, 2):
            (source / f"{sample}_{mate}.fq").write_text(
                "".join(f"@r{i}/{mate}\nAC\n+\nII\n" for i in range(3))
            )
    prepare.preprocess(2)
    output = tmp_path / "data" / "short" / "ggal_gut_1.fq"
    before = output.stat().st_mtime_ns
    prepare.preprocess(2)
    assert output.stat().st_mtime_ns == before
    prepare.preprocess(1)
    assert len(output.read_text().splitlines()) == 4
    with pytest.raises(ValueError, match="fewer"):
        prepare.preprocess(4)
    assert len(output.read_text().splitlines()) == 4
