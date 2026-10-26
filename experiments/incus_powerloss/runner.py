#!/usr/bin/python3 -I
"""Root-owned, fixed-command host runner. No shell commands or device arguments."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import time

BASE = Path("/var/lib/necroflow-powerloss")
CONFIG = Path("/etc/necroflow-powerloss/config.json")
REPLAY = Path("/usr/local/libexec/necroflow-powerloss/replay-log")
PROJECT = "necroflow-powerloss"
CONTAINER = "necroflow-crash-test"
MARKER = "necroflow-powerloss-v1"
IMAGE_SIZE = 256 * 1024**2
LOG_SIZE = 1024**3
MAX_RUNS = 4
IMAGE_NAMES = (
    "baseline.img",
    "record.img",
    "writes.img",
    "durable.img",
    "completed.img",
    "durable-recovery.img",
    "completed-recovery.img",
)


def command(*args, accepted=(0,)):
    result = subprocess.run([str(arg) for arg in args], capture_output=True, text=True)
    if result.returncode not in accepted:
        raise RuntimeError(
            f"{shlex.join(map(str, args))}: {result.stderr or result.stdout}"
        )
    return result.stdout


def incus(*args):
    return command("incus", "--force-local", "--project", PROJECT, *args)


def parse_request(value):
    args = shlex.split(value)
    if args == ["preflight"]:
        return args
    if (
        len(args) != 2
        or args[0] not in ("smoke", "report", "cleanup")
        or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", args[1]) is None
    ):
        raise ValueError(
            "expected preflight | smoke RUN_ID | report RUN_ID | cleanup RUN_ID"
        )
    return args


def owned_container():
    config = json.loads(incus("query", f"/1.0/instances/{CONTAINER}?project={PROJECT}"))
    if config["config"].get("user.necroflow-powerloss") != MARKER:
        raise RuntimeError(
            "refusing to use an instance without this environment's marker"
        )
    return config


def preflight():
    result = {
        "root": os.geteuid() == 0,
        "kernel": os.uname().release,
        "mapper_access": os.access("/dev/mapper/control", os.R_OK | os.W_OK),
        "loop_access": os.access("/dev/loop-control", os.R_OK | os.W_OK),
        "replay_tool": REPLAY.is_file(),
        "configured": CONFIG.is_file(),
    }
    if result["mapper_access"]:
        result["dm_targets"] = command("dmsetup", "targets")
        result["log_writes"] = "log-writes " in result["dm_targets"]
    result["ready"] = all(
        result.get(key)
        for key in (
            "root",
            "mapper_access",
            "loop_access",
            "replay_tool",
            "configured",
            "log_writes",
        )
    )
    return result


def run_path(run_id):
    path = BASE / "runs" / run_id
    if path.is_symlink() or path.resolve() != path:
        raise RuntimeError("run path must not contain symlinks")
    return path


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def create_image(path, size):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.ftruncate(fd, size)
    finally:
        os.close(fd)


def loops_for(path):
    payload = json.loads(
        command("losetup", "--json", "--list", "--output", "NAME,BACK-FILE")
    )
    return [
        entry["name"]
        for entry in payload.get("loopdevices", [])
        if entry["back-file"] == str(path)
    ]


def require_unused(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise RuntimeError(f"not an exclusively owned regular image: {path}")
    if loops_for(path):
        raise RuntimeError(f"image already attached: {path}")


def attach(path):
    require_unused(path)
    return command("losetup", "--find", "--show", "--nooverlap", path).strip()


def copy_image(source, destination):
    require_unused(source)
    # Reserve the destination ourselves; cp may only overwrite this new inode.
    create_image(destination, 0)
    command("cp", "--reflink=auto", "--sparse=always", "--", source, destination)


def mount_source(path):
    output = command("findmnt", "--json", "--mountpoint", path, accepted=(0, 1))
    return json.loads(output)["filesystems"][0]["source"] if output.strip() else None


def mount_test(run, device):
    target = run / "mount"
    if mount_source(target):
        raise RuntimeError("test mount already in use")
    command("mount", "-t", "ext4", "-o", "nodiscard", device, target)
    incus(
        "config",
        "device",
        "add",
        CONTAINER,
        "crash-test",
        "disk",
        f"source={target}",
        "path=/crash-test",
    )


def cleanup_devices(run):
    config = owned_container()
    device = config["devices"].get("crash-test")
    if device:
        if device.get("source") != str(run / "mount"):
            raise RuntimeError("another run owns the container mount")
        incus("config", "device", "remove", CONTAINER, "crash-test")
    loops = {loop for name in IMAGE_NAMES for loop in loops_for(run / name)}
    mapper = f"nfcrash-{run.name}"
    mapper_path = Path("/dev/mapper") / mapper
    source = mount_source(run / "mount")
    if source:
        if source != str(mapper_path) and source not in loops:
            raise RuntimeError("refusing to unmount an unrelated device")
        command("umount", run / "mount")
    if mapper_path.exists():
        table = command("dmsetup", "table", mapper).split()
        expected = {
            f"{os.major(os.stat(loop).st_rdev)}:{os.minor(os.stat(loop).st_rdev)}"
            for loop in loops_for(run / "record.img") + loops_for(run / "writes.img")
        }
        if len(table) != 5 or table[2] != "log-writes" or set(table[3:]) != expected:
            raise RuntimeError("refusing to remove an unrelated device mapper")
        command("dmsetup", "remove", mapper)
    for loop in loops:
        if command(
            "findmnt", "--noheadings", "--source", loop, accepted=(0, 1)
        ).strip():
            raise RuntimeError(f"refusing to detach mounted device {loop}")
        command("losetup", "--detach", loop)
    if any(loops_for(run / name) for name in IMAGE_NAMES):
        raise RuntimeError(
            "loop detach still pending; retain backing files and retry cleanup"
        )


def workload(operation, phase):
    return json.loads(
        incus(
            "exec",
            CONTAINER,
            "--",
            "/opt/necroflow/.venv/bin/python",
            "/opt/necroflow/experiments/incus_powerloss/workload.py",
            operation,
            phase,
        )
    )


def smoke(run_id):
    checks = preflight()
    if not checks["ready"]:
        raise RuntimeError(f"preflight failed: {json.dumps(checks)}")
    config = owned_container()
    if config["devices"].get("crash-test"):
        raise RuntimeError("container already has a crash-test mount")
    runs = BASE / "runs"
    if len(list(runs.iterdir())) >= MAX_RUNS:
        raise RuntimeError(
            f"at most {MAX_RUNS} retained runs; cleanup an old run first"
        )
    run = run_path(run_id)
    if (Path("/dev/mapper") / f"nfcrash-{run_id}").exists():
        raise RuntimeError("device-mapper name already exists")
    run.mkdir(mode=0o700)  # Existing run IDs must never be overwritten.
    (run / "mount").mkdir()
    report = {
        "run_id": run_id,
        "started": time.time(),
        "preflight": checks,
        "setup": json.loads(CONFIG.read_text()),
        "recoveries": {},
    }
    write_json(run / "report.json", report)
    try:
        create_image(run / "baseline.img", IMAGE_SIZE)
        baseline = attach(run / "baseline.img")
        command(
            "mkfs.ext4",
            "-q",
            "-E",
            "nodiscard,lazy_itable_init=0,lazy_journal_init=0",
            baseline,
        )
        command("mount", "-t", "ext4", "-o", "nodiscard", baseline, run / "mount")
        mappings = json.loads(config["config"]["volatile.idmap.current"])
        uid = next(
            item["Hostid"] for item in mappings if item["Isuid"] and item["Nsid"] == 0
        )
        gid = next(
            item["Hostid"] for item in mappings if item["Isgid"] and item["Nsid"] == 0
        )
        os.chown(run / "mount", uid, gid)
        command("umount", run / "mount")
        command("losetup", "--detach", baseline)
        copy_image(run / "baseline.img", run / "record.img")
        create_image(run / "writes.img", LOG_SIZE)
        data, log = attach(run / "record.img"), attach(run / "writes.img")
        mapper = f"nfcrash-{run_id}"
        command(
            "dmsetup",
            "create",
            mapper,
            "--table",
            f"0 {IMAGE_SIZE // 512} log-writes {data} {log}",
        )
        mount_test(run, f"/dev/mapper/{mapper}")
        report["observations"] = {"durable": workload("record", "durable")}
        # syncfs completes before this mark; later unmount flushes are outside it.
        command("sync", "-f", run / "mount")
        command("dmsetup", "message", mapper, "0", "mark durable")
        report["observations"]["completed"] = workload("record", "completed")
        command("dmsetup", "message", mapper, "0", "mark completed")
        report["dm_status"] = command("dmsetup", "status", mapper)
        write_json(run / "report.json", report)
        cleanup_devices(run)  # Finalizes the log. No use of record.img for recovery.
        report["log_info"] = command(REPLAY, "--log", run / "writes.img")
        for phase in ("durable", "completed"):
            # --find must succeed: upstream replay alone does not reject a missing end mark.
            report.setdefault("mark_entries", {})[phase] = int(
                command(
                    REPLAY,
                    "--log",
                    run / "writes.img",
                    "--find",
                    "--end-mark",
                    phase,
                ).strip()
            )
            raw = run / f"{phase}.img"
            copy_image(run / "baseline.img", raw)
            require_unused(raw)
            command(
                REPLAY,
                "--log",
                run / "writes.img",
                "--replay",
                raw,
                "--end-mark",
                phase,
            )
            # Preserve raw crash image; journal recovery and Necroflow mutate a copy.
            recovered = run / f"{phase}-recovery.img"
            copy_image(raw, recovered)
            recovered_loop = attach(recovered)
            report.setdefault("fsck", {})[phase] = command(
                "e2fsck",
                "-p",
                recovered_loop,
                accepted=(0, 1),
            )
            mount_test(run, recovered_loop)
            report["recoveries"][phase] = workload("recover", phase)
            cleanup_devices(run)
            write_json(run / "report.json", report)
        report["verified"] = True
    except Exception as exc:
        report["error"] = str(exc)
        raise
    finally:
        try:
            cleanup_devices(run)
        except Exception as exc:
            report["cleanup_error"] = str(exc)
            report["verified"] = False
            raise
        finally:
            report["finished"] = time.time()
            write_json(run / "report.json", report)
    return report


def main():
    original = os.environ.get("SSH_ORIGINAL_COMMAND")
    if original is not None and len(sys.argv) != 1:
        raise ValueError("SSH runner takes no process arguments")
    args = parse_request(original if original is not None else shlex.join(sys.argv[1:]))
    if args == ["preflight"]:
        result = preflight()
        print(json.dumps(result, indent=2))
        return 0 if result["ready"] else 1
    if os.geteuid() != 0:
        raise PermissionError("host storage operations require root")
    with (BASE / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        operation, run_id = args
        path = run_path(run_id)
        if operation == "smoke":
            result = smoke(run_id)
        elif operation == "report":
            result = json.loads((path / "report.json").read_text())
        else:
            if not path.is_dir():
                raise FileNotFoundError(path)
            cleanup_devices(path)
            shutil.rmtree(path)
            result = {"cleaned": run_id}
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
