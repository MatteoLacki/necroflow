"""Result publication preserves output data without requiring source ACLs."""

import errno
import os
from pathlib import Path
import struct
import subprocess
import sys

import pytest

from necroflow import DAG, Node, Pipeline
import necroflow.cli as cli
from necroflow.rules import Inputs, Outputs, Command


class Result(Node):
    filename = "result.txt"


SOURCE = Command(
    "source", Inputs(value=str), Outputs(out=Result), "echo {value} > {out}"
)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux POSIX ACL encoding")
def test_copy_does_not_preserve_source_acl_or_xattrs(tmp_path):
    """Linux results must not require source ACLs or extended attributes."""
    source = tmp_path / "source"
    source.write_text("result")
    # Linux POSIX ACL xattr: version 2, then (tag, permissions, user/group ID).
    undefined_id = 0xFFFFFFFF
    entries = [
        (0x01, 7, undefined_id),  # owner: rwx
        (0x02, 4, os.getuid() + 1),  # named user: r--
        (0x04, 5, undefined_id),  # group: r-x
        (0x10, 5, undefined_id),  # mask: r-x
        (0x20, 0, undefined_id),  # other: ---
    ]
    acl = struct.pack("<I", 2) + b"".join(
        struct.pack("<HHI", *entry) for entry in entries
    )
    try:
        os.setxattr(source, "system.posix_acl_access", acl)
        os.setxattr(source, "user.necroflow_test", b"source metadata")
    except OSError as exc:
        if exc.errno in (errno.ENOTSUP, errno.EPERM, errno.EACCES):
            pytest.skip(f"test filesystem cannot set ACLs/xattrs: {exc}")
        raise
    destination = tmp_path / "destination"

    cli._copy_result(source, destination)

    assert destination.read_bytes() == source.read_bytes()
    assert "system.posix_acl_access" not in os.listxattr(destination)
    assert "user.necroflow_test" not in os.listxattr(destination)
    assert os.getxattr(source, "system.posix_acl_access") == acl


def test_copy_preserves_directory_contents_links_executable_and_mtime(tmp_path):
    """Result copies retain useful file semantics and remain independent copies."""
    source = tmp_path / "source"
    source.mkdir()
    executable = source / "run"
    executable.write_text("#!/bin/sh\necho result\n")
    executable.chmod(0o755)
    os.utime(executable, ns=(1_700_000_000_000_000_000,) * 2)
    (source / "hardlink").hardlink_to(executable)
    (source / "symlink").symlink_to("run")
    (source / "dangling").symlink_to("missing")
    (source / "empty").mkdir()
    destination = tmp_path / "destination"

    cli._copy_result(source, destination)

    copied = destination / "run"
    assert copied.read_bytes() == executable.read_bytes()
    assert copied.stat().st_mode & 0o100
    assert copied.stat().st_mtime_ns == executable.stat().st_mtime_ns
    assert not copied.samefile(executable)
    assert (destination / "hardlink").samefile(copied)
    assert (destination / "symlink").readlink() == Path("run")
    assert (destination / "dangling").readlink() == Path("missing")
    assert (destination / "empty").is_dir()


@pytest.mark.parametrize("partial_kind", ["file", "directory", "symlink"])
def test_failed_copy_cleans_staged_outputs_and_preserves_previous_results(
    tmp_path, monkeypatch, partial_kind
):
    """A failed copy leaves neither staged debris nor changes to published results."""
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    pipeline.first = SOURCE(pipeline, value="first")
    pipeline.second = SOURCE(pipeline, value="second")
    pipeline.finish()
    for node in pipeline.nodes:
        node.path.parent.mkdir(parents=True)
        node.path.write_text("old result")
    results = tmp_path / "results"
    combos = [("job", pipeline, cli._resolve_request(pipeline, None))]
    cli._materialize_results(results, combos)
    before = {
        p.relative_to(results): p.read_bytes()
        for p in results.rglob("*")
        if p.is_file()
    }
    for node in pipeline.nodes:
        node.path.write_text("new result")
    copy = cli._copy_result

    def fail_second_copy(source, destination):
        if source == pipeline.first.path:
            return copy(source, destination)
        if partial_kind == "directory":
            destination.mkdir()
            (destination / "partial").write_text("partial")
        elif partial_kind == "symlink":
            destination.symlink_to("missing")
        else:
            destination.write_text("partial")
        raise subprocess.CalledProcessError(1, ["cp", str(source), str(destination)])

    monkeypatch.setattr(cli, "_copy_result", fail_second_copy)
    with pytest.raises(subprocess.CalledProcessError) as failure:
        cli._materialize_results(results, combos)

    assert not list(results.rglob("*.necroflow-*"))
    after = {
        p.relative_to(results): p.read_bytes()
        for p in results.rglob("*")
        if p.is_file()
    }
    assert after == before
    assert pipeline.second.path.read_text() == "new result"
    notes = " ".join(failure.value.__notes__)
    assert "Result publication failed" in notes
    assert str(results / "job" / "second" / "result.txt") in notes
    assert "Completed nodes remain cached" in notes


def test_publication_failure_leaves_completed_nodes_cached(tmp_path, monkeypatch):
    """Retrying publication reuses successful nodes after a copy error."""
    factory = tmp_path / "pipeline.py"
    factory.write_text(
        "from necroflow import Node, command, outputs\n"
        "class Result(Node): filename = 'result.txt'\n"
        "@command('echo result > {out}')\n"
        "def source():\n"
        "    out = outputs(Result)\n"
        "    return out\n"
        "def build(pipeline, config):\n"
        "    pipeline.result = source(pipeline)\n"
    )
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory}:build"\n')
    nodes = tmp_path / "nodes"
    results = tmp_path / "results"
    args = ["--nodes-dir", str(nodes), "--results-dir", str(results), str(job)]

    def fail_copy(source, destination):
        raise subprocess.CalledProcessError(1, ["cp", str(source), str(destination)])

    with monkeypatch.context() as patch:
        patch.setattr(cli, "_copy_result", fail_copy)
        with pytest.raises(subprocess.CalledProcessError):
            cli.main(args)

    state = next(nodes.glob("*/*/.rip/state"))
    assert state.read_text().strip() == "up_to_date"
    output = state.parent.parent / "result.txt"
    original_mtime = output.stat().st_mtime_ns
    cli.main(args)
    assert output.stat().st_mtime_ns == original_mtime
    assert (results / "job/result/result.txt").read_text() == "result\n"
