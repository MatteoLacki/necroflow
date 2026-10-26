"""Unprivileged boundary tests. Real storage replay is verified by setup's SSH smoke test."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tarfile
from types import SimpleNamespace

import pytest

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(
        f"powerloss_{name}", HERE / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "request_text",
    [
        "",
        "bash",
        "smoke",
        "smoke ../disk",
        "cleanup /dev/sda",
        "smoke a;id",
        "smoke $(id)",
        "smoke a extra",
        "report --help",
        "smoke " + "a" * 33,
        "preflight extra",
        "smoke 'a\nb'",
    ],
)
def test_reject_untrusted_requests(request_text):
    """SSH callers cannot supply shell syntax, arbitrary operations, paths, or extra arguments."""
    with pytest.raises(ValueError):
        load("runner").parse_request(request_text)


@pytest.mark.parametrize(
    "request_text", ["preflight", "smoke trial-1", "report trial-1", "cleanup trial-1"]
)
def test_named_operations(request_text):
    """The documented SSH interface accepts only fixed operations and bounded run IDs."""
    assert load("runner").parse_request(request_text) == request_text.split()


def test_run_symlinks_rejected(tmp_path, monkeypatch):
    """A run cannot escape the runner-owned directory through a symlink."""
    runner = load("runner")
    monkeypatch.setattr(runner, "BASE", tmp_path)
    (tmp_path / "runs").mkdir()
    (tmp_path / "runs" / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlinks"):
        runner.run_path("escape")


def test_images_never_overwrite_existing_files(tmp_path):
    """Formatting inputs are newly created exclusive files; existing bytes remain untouched."""
    runner = load("runner")
    path = tmp_path / "image"
    path.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        runner.create_image(path, 1024)
    assert path.read_bytes() == b"keep"


def test_attached_image_never_replayed(tmp_path, monkeypatch):
    """Replay cannot target an image already in use through any loop device."""
    runner = load("runner")
    path = tmp_path / "image"
    path.write_bytes(b"data")
    monkeypatch.setattr(runner, "loops_for", lambda _: ["/dev/loop123"])
    with pytest.raises(RuntimeError, match="already attached"):
        runner.require_unused(path)


def test_hardlinked_image_rejected(tmp_path):
    """A backing file with another name cannot become a destructive operation's target."""
    runner = load("runner")
    path = tmp_path / "image"
    path.write_bytes(b"data")
    os.link(path, tmp_path / "other")
    with pytest.raises(RuntimeError, match="exclusively owned"):
        runner.require_unused(path)


def test_cleanup_does_not_touch_another_runs_mount(tmp_path, monkeypatch):
    """Cleanup refuses a mount that belongs to a different run."""
    runner = load("runner")
    monkeypatch.setattr(
        runner,
        "owned_container",
        lambda: {"devices": {"crash-test": {"source": "/some/other/run/mount"}}},
    )
    with pytest.raises(RuntimeError, match="another run"):
        runner.cleanup_devices(tmp_path)


def test_unrelated_mount_never_unmounted(tmp_path, monkeypatch):
    """A coincidentally occupied mount directory cannot cause an unrelated unmount."""
    runner = load("runner")
    monkeypatch.setattr(runner, "owned_container", lambda: {"devices": {}})
    monkeypatch.setattr(runner, "loops_for", lambda _: [])
    monkeypatch.setattr(runner, "mount_source", lambda _: "/dev/sda1")
    with pytest.raises(RuntimeError, match="unrelated device"):
        runner.cleanup_devices(tmp_path)


def test_ssh_process_arguments_rejected(monkeypatch):
    """An SSH command cannot override forced-command parsing with process arguments."""
    runner = load("runner")
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "preflight")
    monkeypatch.setattr(sys, "argv", ["runner.py", "cleanup", "run"])
    with pytest.raises(ValueError, match="no process arguments"):
        runner.main()


def test_snapshot_preserves_dirty_worktree(tmp_path):
    """Transfer preserves edited and untracked code, omits deleted and ignored files, and retains symlinks."""
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "tracked").write_text("old")
    (tmp_path / "deleted").write_text("old")
    subprocess.run(
        ["git", "-C", str(tmp_path), "add", "tracked", "deleted"], check=True
    )
    (tmp_path / "tracked").write_text("uncommitted edit")
    (tmp_path / "deleted").unlink()
    (tmp_path / "new").write_text("untracked code")
    (tmp_path / ".gitignore").write_text("ignored\n")
    (tmp_path / "ignored").write_text("excluded")
    (tmp_path / "alias").symlink_to("tracked")
    snapshot = subprocess.run(
        [sys.executable, str(HERE / "snapshot.py"), str(tmp_path)],
        check=True,
        capture_output=True,
    )
    with tarfile.open(fileobj=io.BytesIO(snapshot.stdout)) as archive:
        assert set(archive.getnames()) == {"tracked", "new", ".gitignore", "alias"}
        assert archive.extractfile("tracked").read() == b"uncommitted edit"
        assert archive.extractfile("new").read() == b"untracked code"
        assert archive.getmember("alias").issym()


def test_setup_records_command_before_launch(tmp_path, monkeypatch):
    """An interrupted host command leaves its identity in the readable setup receipt."""
    setup = load("setup")
    monkeypatch.setattr(setup, "ETC", tmp_path)
    monkeypatch.setattr(
        setup, "ACTIVE_CONFIG", {"marker": setup.MARKER, "stage": "installing"}
    )

    def interrupted(*args, **kwargs):
        receipt = json.loads((tmp_path / "config.json").read_text())
        assert receipt["current_command"] == "incus exec test-instance"
        assert receipt["last_exit_code"] is None
        raise OSError("interrupted external command")

    monkeypatch.setattr(setup.subprocess, "run", interrupted)
    with pytest.raises(OSError):
        setup.run("incus", "exec", "test-instance")
    assert "incus exec test-instance" in (tmp_path / "setup.log").read_text()


def test_setup_failure_retains_both_output_streams(tmp_path, monkeypatch):
    """A failing command cannot hide its real error behind a warning on the other stream."""
    setup = load("setup")
    monkeypatch.setattr(setup, "ETC", tmp_path)
    monkeypatch.setattr(
        setup, "ACTIVE_CONFIG", {"marker": setup.MARKER, "stage": "installing"}
    )
    monkeypatch.setattr(
        setup.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=17,
            stdout="actual package error\n",
            stderr="secondary warning\n",
        ),
    )
    with pytest.raises(RuntimeError) as failure:
        setup.run("apt-get", "install", "example")
    assert "exited 17" in str(failure.value)
    assert "actual package error" in str(failure.value)
    assert "secondary warning" in str(failure.value)
    receipt = json.loads((tmp_path / "config.json").read_text())
    assert receipt["last_exit_code"] == 17
    log = (tmp_path / "setup.log").read_text()
    assert "actual package error" in log and "secondary warning" in log


def test_resume_rejects_unowned_receipt(tmp_path, monkeypatch):
    """Resume cannot adopt existing host resources lacking this environment's ownership receipt."""
    setup = load("setup")
    monkeypatch.setattr(setup, "ETC", tmp_path)
    (tmp_path / "config.json").write_text('{"marker": "another-environment"}')
    with pytest.raises(RuntimeError, match="ownership marker"):
        setup.resume(SimpleNamespace())


def test_resume_rejects_unowned_instance(tmp_path, monkeypatch):
    """A valid receipt cannot authorize changes to an instance whose ownership marker changed."""
    setup = load("setup")
    monkeypatch.setattr(setup, "ETC", tmp_path)
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "marker": setup.MARKER,
                "project": setup.PROJECT,
                "container": setup.INSTANCE,
                "project_created": True,
                "instance_created": True,
            }
        )
    )
    monkeypatch.setattr(setup, "incus", lambda *a, **kw: '{"config": {}}')
    with pytest.raises(RuntimeError, match="instance ownership marker"):
        setup.resume(SimpleNamespace())


def test_workload_independent_bytes_and_recovery(tmp_path, monkeypatch):
    """The real current CLI produces both phases and recovers missing outputs, with SHA-256 checks."""
    workload = load("workload")
    source = tmp_path / "source"
    source.mkdir()
    shutil.copy2(HERE / "workload.py", source / "workload.py")
    root = tmp_path / "crash-test"
    root.mkdir()
    monkeypatch.setattr(workload, "HERE", source)
    monkeypatch.setattr(workload, "ROOT", root)
    monkeypatch.setattr(workload, "PYTHON", Path(sys.executable))
    first = workload.record("durable")
    expected = hashlib.sha256(b"necroflow durable\n").hexdigest()
    assert set(first["expected_files"].values()) == {expected}
    assert workload.recover("durable")["verified"]
    second = workload.record("completed")
    # Missing immutable outputs must be rebuilt when the same pipeline resumes.
    for path in (root / "nodes").rglob("text.txt"):
        if path.read_bytes() == b"necroflow completed\n":
            path.unlink()
    result_name = next(
        name for name in second["expected_files"] if name.startswith("results/")
    )
    (root / result_name).unlink()
    recovered = workload.recover("completed")
    assert recovered["verified"]
    assert (
        recovered["after"]["files"][result_name]
        == hashlib.sha256(b"necroflow completed\n").hexdigest()
    )


def test_early_recovery_rejects_later_changes(tmp_path, monkeypatch):
    """The earlier crash assertion detects a late write rather than silently accepting it."""
    workload = load("workload")
    monkeypatch.setattr(workload, "ROOT", tmp_path)
    (tmp_path / "durable.txt").write_bytes(workload.PAYLOADS["durable"])
    (tmp_path / "later.txt").write_bytes(b"later")
    with pytest.raises(RuntimeError, match="later write"):
        workload.recover("durable")


@pytest.mark.skipif(
    not os.environ.get("NF_REPLAY_LOG"),
    reason="set NF_REPLAY_LOG to built upstream replay-log",
)
def test_upstream_replay_flush_fua_and_marks(tmp_path):
    """Pinned upstream replay honors marks and flush/FUA boundaries on a synthetic v1 log."""
    replay = os.environ["NF_REPLAY_LOG"]
    sector = 512
    entries = []

    def entry(target, sectors, flags, payload=b"", mark=b""):
        header = struct.pack("<QQQQ", target, sectors, flags, len(mark)) + mark
        entries.append(header.ljust(sector, b"\0") + payload)

    entry(0, 1, 0, b"a" * sector)
    entry(0, 0, 1)  # completed flush
    entry(0, 0, 8, mark=b"durable\0")
    entry(0, 1, 2, b"b" * sector)  # completed FUA write
    entry(0, 0, 8, mark=b"completed\0")
    log = tmp_path / "writes.img"
    log.write_bytes(
        struct.pack("<QQQI", 0x6A736677736872, 1, len(entries), sector).ljust(
            sector, b"\0"
        )
        + b"".join(entries)
    )
    for flag, index in (("--next-flush", 1), ("--next-fua", 3)):
        found = subprocess.run(
            [replay, "--log", str(log), "--find", flag],
            check=True,
            capture_output=True,
            text=True,
        )
        assert int(found.stdout.strip()) == index
    for mark, content in (("durable", b"a"), ("completed", b"b")):
        destination = tmp_path / f"{mark}.img"
        destination.write_bytes(b"\0" * sector)
        subprocess.run(
            [replay, "--log", str(log), "--find", "--end-mark", mark],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                replay,
                "--log",
                str(log),
                "--replay",
                str(destination),
                "--end-mark",
                mark,
            ],
            check=True,
        )
        assert destination.read_bytes() == content * sector
    absent = subprocess.run(
        [replay, "--log", str(log), "--find", "--end-mark", "missing"],
        capture_output=True,
    )
    assert absent.returncode != 0
