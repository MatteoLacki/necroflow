#!/usr/bin/env python3
"""Install this isolated host environment. Run locally with sudo, never over its SSH key."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import shlex
import socket
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
BASE = Path("/var/lib/necroflow-powerloss")
ETC = Path("/etc/necroflow-powerloss")
LIB = Path("/usr/local/libexec/necroflow-powerloss")
PROJECT = "necroflow-powerloss"
INSTANCE = "necroflow-crash-test"
ACCOUNT = "nf-powerloss"
UNIT = Path("/etc/systemd/system/necroflow-powerloss-sshd.service")
SUDOERS = Path("/etc/sudoers.d/necroflow-powerloss")
MARKER = "necroflow-powerloss-v1"
REPLAY_REVISION = "7b70d8a6863c5de30933d42a7672d35d01d2dc6c"
CLIENT_KEY = "/home/matteo/.ssh/necroflow-powerloss_ed25519"
KNOWN_HOSTS = "/home/matteo/.ssh/necroflow-powerloss_known_hosts"
ACTIVE_CONFIG = None


def run(*args):
    display = shlex.join(map(str, args))
    print("+", display, file=sys.stderr, flush=True)
    if ACTIVE_CONFIG is not None:
        ACTIVE_CONFIG["current_command"] = display
        ACTIVE_CONFIG["last_exit_code"] = None
        save(ACTIVE_CONFIG)
        with (ETC / "setup.log").open("a") as log:
            (ETC / "setup.log").chmod(0o644)
            log.write(f"\n+ {display}\n")
    result = subprocess.run(list(map(str, args)), capture_output=True, text=True)
    if ACTIVE_CONFIG is not None:
        ACTIVE_CONFIG["last_exit_code"] = result.returncode
        save(ACTIVE_CONFIG)
        with (ETC / "setup.log").open("a") as log:
            log.write(result.stdout + result.stderr + f"\nexit: {result.returncode}\n")
    if result.returncode:
        raise RuntimeError(
            f"{display} exited {result.returncode}:\n{result.stdout}{result.stderr}"
        )
    return result.stdout


def incus(*args, project=PROJECT):
    return run("incus", "--force-local", "--project", project, *args)


def save(config):
    temporary = ETC / "config.json.tmp"
    temporary.write_text(json.dumps(config, indent=2) + "\n")
    temporary.chmod(0o644)
    temporary.replace(ETC / "config.json")


def install(args, existing=None):
    global ACTIVE_CONFIG
    # Refuse collisions before changing anything. Root owns every installed script.
    for path in (BASE, ETC, LIB, UNIT, SUDOERS):
        if existing is None and (path.exists() or path.is_symlink()):
            raise RuntimeError(
                f"already exists; inspect or teardown this environment first: {path}"
            )
    try:
        pwd.getpwnam(ACCOUNT)
    except KeyError:
        pass
    else:
        if existing is None or not existing.get("account_created"):
            raise RuntimeError(f"account already exists: {ACCOUNT}")
    projects = json.loads(
        incus("project", "list", "--format", "json", project="default")
    )
    if existing is None and any(item["name"] == PROJECT for item in projects):
        raise RuntimeError(f"project already exists: {PROJECT}")
    pools = json.loads(incus("storage", "list", "--format", "json", project="default"))
    pool = next((pool for pool in pools if pool["name"] == args.pool), None)
    if pool is None:
        raise RuntimeError(f"existing pool not found: {args.pool}")
    # No changes to existing networks, pools, profiles, or the main SSH service.
    incus("network", "show", args.network, project="default")
    incus(
        "exec",
        args.dev_container,
        "--",
        "test",
        "-d",
        args.repo,
        project=args.dev_project,
    )
    dev_uid = incus(
        "exec", args.dev_container, "--", "id", "-u", "matteo", project=args.dev_project
    ).strip()
    dev_gid = incus(
        "exec", args.dev_container, "--", "id", "-g", "matteo", project=args.dev_project
    ).strip()

    def dev(*command):
        return incus(
            "exec",
            args.dev_container,
            "--user",
            dev_uid,
            "--group",
            dev_gid,
            "--",
            *command,
            project=args.dev_project,
        )

    missing = [
        name
        for name in (
            "gcc",
            "make",
            "git",
            "sshd",
            "mkfs.ext4",
            "dmsetup",
            "losetup",
            "visudo",
        )
        if shutil.which(name) is None
    ]
    if missing:
        run("apt-get", "update")
        run(
            "apt-get",
            "install",
            "--yes",
            "build-essential",
            "git",
            "openssh-server",
            "e2fsprogs",
            "dmsetup",
            "util-linux",
            "sudo",
        )
    run("modprobe", "dm-log-writes")
    if "log-writes " not in run("dmsetup", "targets"):
        raise RuntimeError("running kernel does not provide the log-writes target")
    if existing is None:
        for path in (BASE, ETC, LIB):
            path.mkdir(mode=0o700 if path == BASE else 0o755)
        (BASE / "runs").mkdir(mode=0o700)
    config = {
        "marker": MARKER,
        "project": PROJECT,
        "container": INSTANCE,
        "image_requested": args.image,
        "pool": args.pool,
        "network": args.network,
        "host": args.host,
        "ssh_port": 22222,
        "dev_container": args.dev_container,
        "dev_project": args.dev_project,
        "source_repo": args.repo,
        "limits": {
            "cpu": 2,
            "memory": "4GiB",
            "crash_filesystem_bytes": 256 * 1024**2,
            "write_log_bytes": 1024**3,
            "retained_runs": 4,
        },
        "replay_revision": REPLAY_REVISION,
        "stage": "installing",
    }
    if existing is not None:
        config = existing
        config["stage"] = "installing"
        config.pop("error", None)
    with (
        (BASE / "setup.lock").open("a") as setup_lock,
        tempfile.TemporaryDirectory(prefix="nf-powerloss-setup-") as temp,
    ):
        fcntl.flock(setup_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ACTIVE_CONFIG = config
        save(config)
        ETC.chmod(0o755)
        LIB.chmod(0o755)
        # Refuse to change files beneath an active smoke run. Stop only our listener.
        with (BASE / "runner.lock").open("a") as runner_lock:
            fcntl.flock(runner_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if UNIT.exists():
                run("systemctl", "stop", UNIT.name)
        with socket.socket() as listener:
            listener.bind((args.host, 22222))
        temp = Path(temp)
        source = temp / "log-writes"
        run("git", "clone", "https://github.com/josefbacik/log-writes.git", source)
        run("git", "-C", source, "checkout", "--detach", REPLAY_REVISION)
        run("make", "-C", source)
        shutil.copy2(source / "replay-log", LIB / "replay-log")
        shutil.copy2(HERE / "runner.py", LIB / "runner.py")
        (LIB / "runner.py").chmod(0o755)
        config["replay_sha256"] = hashlib.sha256(
            (LIB / "replay-log").read_bytes()
        ).hexdigest()
        if not config.get("project_created"):
            incus(
                "project",
                "create",
                PROJECT,
                "-c",
                "features.images=false",
                "-c",
                "features.networks=false",
                "-c",
                "features.storage.volumes=false",
                project="default",
            )
            config["project_created"] = True
            save(config)
        create = [
            "init",
            args.image,
            INSTANCE,
            "--no-profiles",
            "--storage",
            args.pool,
            "-c",
            "limits.cpu=2",
            "-c",
            "limits.memory=4GiB",
            "-c",
            "security.privileged=false",
            "-c",
            f"user.necroflow-powerloss={MARKER}",
        ]
        if pool["driver"] in ("btrfs", "zfs", "lvm", "ceph"):
            create += ["-d", "root,size=12GiB"]
            config["limits"]["root_disk"] = "12GiB"
        else:
            config["limits"][
                "root_disk"
            ] = "existing pool has no enforced per-instance quota"
        if not config.get("instance_created"):
            incus(*create)
            config["instance_created"] = True
            save(config)
        details = json.loads(
            incus("query", f"/1.0/instances/{INSTANCE}?project={PROJECT}")
        )
        network = details["devices"].get("eth0")
        if network is None:
            incus(
                "config",
                "device",
                "add",
                INSTANCE,
                "eth0",
                "nic",
                "name=eth0",
                f"network={args.network}",
            )
        elif network.get("type") != "nic" or network.get("network") != args.network:
            raise RuntimeError("existing instance has an unexpected eth0 device")
        if details["status"] != "Running":
            incus("start", INSTANCE)
        incus("exec", INSTANCE, "--", "apt-get", "update")
        incus(
            "exec",
            INSTANCE,
            "--",
            "env",
            "DEBIAN_FRONTEND=noninteractive",
            "apt-get",
            "install",
            "--yes",
            "git",
            "build-essential",
            "python3-venv",
            "ca-certificates",
        )
        # Include tracked modifications and untracked nonignored files from development.
        snapshot_program = (HERE / "snapshot.py").read_text()
        archive = temp / "source.tar"
        config["current_command"] = "snapshot development working tree"
        config["last_exit_code"] = None
        save(config)
        with archive.open("wb") as stream:
            result = subprocess.run(
                [
                    "incus",
                    "--force-local",
                    "--project",
                    args.dev_project,
                    "exec",
                    args.dev_container,
                    "--user",
                    dev_uid,
                    "--group",
                    dev_gid,
                    "--",
                    "python3",
                    "-c",
                    snapshot_program,
                    args.repo,
                ],
                stdout=stream,
                stderr=subprocess.PIPE,
            )
        config["last_exit_code"] = result.returncode
        save(config)
        if result.returncode:
            raise RuntimeError(result.stderr.decode())
        with archive.open("rb") as stream:
            config["source_archive_sha256"] = hashlib.file_digest(
                stream, "sha256"
            ).hexdigest()
        incus("file", "push", archive, f"{INSTANCE}/opt/necroflow-source.tar")
        incus("exec", INSTANCE, "--", "mkdir", "-p", "/opt/necroflow")
        incus(
            "exec",
            INSTANCE,
            "--",
            "tar",
            "-xf",
            "/opt/necroflow-source.tar",
            "-C",
            "/opt/necroflow",
        )
        incus("exec", INSTANCE, "--", "python3", "-m", "venv", "/opt/uv")
        incus("exec", INSTANCE, "--", "/opt/uv/bin/pip", "install", "uv")
        incus(
            "exec",
            INSTANCE,
            "--",
            "/opt/uv/bin/uv",
            "venv",
            "/opt/necroflow/.venv",
            "--allow-existing",
            "--python",
            "3.14",
        )
        incus(
            "exec",
            INSTANCE,
            "--",
            "/opt/uv/bin/uv",
            "pip",
            "install",
            "--python",
            "/opt/necroflow/.venv/bin/python",
            "-e",
            "/opt/necroflow[dev]",
        )
        config["python"] = incus(
            "exec", INSTANCE, "--", "/opt/necroflow/.venv/bin/python", "-V"
        ).strip()
        config["uv"] = incus(
            "exec", INSTANCE, "--", "/opt/uv/bin/uv", "--version"
        ).strip()
        details = json.loads(
            incus("query", f"/1.0/instances/{INSTANCE}?project={PROJECT}")
        )
        config["instance_config"] = details["config"]
        if not config.get("account_created"):
            run(
                "useradd",
                "--system",
                "--no-create-home",
                "--home-dir",
                "/nonexistent",
                "--shell",
                "/bin/sh",
                ACCOUNT,
            )
        config["account_created"] = True
        save(config)
        dev("mkdir", "-p", "-m", "700", "/home/matteo/.ssh")
        # Dedicated key is generated once, only inside development container.
        probe = incus(
            "exec",
            args.dev_container,
            "--",
            "sh",
            "-c",
            f"test -e {CLIENT_KEY} && echo exists || echo absent",
            project=args.dev_project,
        ).strip()
        if probe == "absent":
            dev(
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                MARKER,
                "-f",
                CLIENT_KEY,
            )
        public = dev("cat", CLIENT_KEY + ".pub").strip()
        if not public.startswith("ssh-ed25519 ") or "\n" in public:
            raise RuntimeError("unexpected client public key")
        (ETC / "authorized_keys").write_text("restrict " + public + "\n")
        (ETC / "authorized_keys").chmod(0o644)
        if not (ETC / "ssh_host_ed25519_key").exists():
            run(
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                ETC / "ssh_host_ed25519_key",
            )
        host_public = (ETC / "ssh_host_ed25519_key.pub").read_text().strip()
        pinned = temp / "known_hosts"
        pinned.write_text(f"[{args.host}]:22222 {host_public}\n")
        incus(
            "file",
            "push",
            pinned,
            f"{args.dev_container}{KNOWN_HOSTS}",
            project=args.dev_project,
        )
        incus(
            "exec",
            args.dev_container,
            "--",
            "chown",
            f"{dev_uid}:{dev_gid}",
            KNOWN_HOSTS,
            project=args.dev_project,
        )
        SUDOERS.write_text(
            f'Defaults:{ACCOUNT} env_keep += "SSH_ORIGINAL_COMMAND"\n'
            f'{ACCOUNT} ALL=(root) NOPASSWD: {LIB}/runner.py ""\n'
        )
        SUDOERS.chmod(0o440)
        run("visudo", "-cf", SUDOERS)
        sshd = ETC / "sshd_config"
        sshd.write_text(
            f"Port 22222\nListenAddress {args.host}\nHostKey {ETC}/ssh_host_ed25519_key\n"
            f"PidFile /run/necroflow-powerloss-sshd.pid\nAllowUsers {ACCOUNT}\n"
            f"AuthorizedKeysFile {ETC}/authorized_keys\n"
            "AuthenticationMethods publickey\nPasswordAuthentication no\n"
            "KbdInteractiveAuthentication no\nUsePAM yes\nPermitRootLogin no\n"
            "PermitTTY no\nDisableForwarding yes\nPermitUserEnvironment no\n"
            f"ForceCommand /usr/bin/sudo -n {LIB}/runner.py\n"
        )
        run("/usr/sbin/sshd", "-t", "-f", sshd)
        UNIT.write_text(
            "[Unit]\nDescription=Necroflow isolated power-loss runner SSH\nAfter=network.target incus.service\n"
            f"[Service]\nExecStart=/usr/sbin/sshd -D -f {sshd}\nRestart=on-failure\n"
            "[Install]\nWantedBy=multi-user.target\n"
        )
        run("systemctl", "daemon-reload")
        run("systemctl", "enable", "--now", UNIT.name)
        config["ssh_command"] = (
            f"ssh -i {CLIENT_KEY} -o IdentitiesOnly=yes -o BatchMode=yes "
            f"-o StrictHostKeyChecking=yes -o UserKnownHostsFile={KNOWN_HOSTS} "
            f"-p 22222 {ACCOUNT}@{args.host}"
        )
        config["stage"] = "installed"
        save(config)
        print(json.dumps(config, indent=2), flush=True)
        # Verify the actual restricted SSH path from development, including key pinning.
        dev(*shlex.split(config["ssh_command"]), "preflight")
        smoke_id = (
            "setup-smoke"
            if not (BASE / "runs/setup-smoke").exists()
            else f"setup-{time.time_ns():x}"
        )
        config["smoke_run_id"] = smoke_id
        save(config)
        smoke_result = dev(*shlex.split(config["ssh_command"]), f"smoke {smoke_id}")
        (BASE / "setup-smoke.json").write_text(smoke_result)
        config["stage"] = "verified"
        config.pop("current_command", None)
        save(config)
        print(smoke_result)


def resume(args):
    config = json.loads((ETC / "config.json").read_text())
    if (
        config.get("marker") != MARKER
        or config.get("project") != PROJECT
        or config.get("container") != INSTANCE
    ):
        raise RuntimeError("cannot resume: environment ownership marker missing")
    if not config.get("project_created") or not config.get("instance_created"):
        raise RuntimeError("resume requires a previously created test instance")
    details = json.loads(incus("query", f"/1.0/instances/{INSTANCE}?project={PROJECT}"))
    if details["config"].get("user.necroflow-powerloss") != MARKER:
        raise RuntimeError("cannot resume: instance ownership marker missing")
    if config.get("stage") == "verified":
        print(json.dumps(config, indent=2))
        return
    for name in ("pool", "network", "host", "dev_container", "dev_project"):
        setattr(args, name, config[name])
    args.repo = config["source_repo"]
    args.image = config["image_requested"]
    install(args, existing=config)


def teardown():
    config = json.loads((ETC / "config.json").read_text())
    if config.get("marker") != MARKER:
        raise RuntimeError("environment ownership marker missing")
    if UNIT.exists():
        run("systemctl", "disable", "--now", UNIT.name)
    # Refuse teardown while any run operation is active.
    with (BASE / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # runner cleanup takes the same lock, so release before invoking it. SSH is stopped.
    for path in sorted((BASE / "runs").iterdir()):
        run(LIB / "runner.py", "cleanup", path.name)
    if config.get("instance_created"):
        details = json.loads(
            incus("query", f"/1.0/instances/{INSTANCE}?project={PROJECT}")
        )
        if details["config"].get("user.necroflow-powerloss") != MARKER:
            raise RuntimeError("instance ownership marker missing")
        incus("delete", INSTANCE, "--force")
    if config.get("project_created"):
        incus("project", "delete", PROJECT, project="default")
    if config.get("account_created"):
        run("userdel", ACCOUNT)
    for path in (SUDOERS, UNIT):
        path.unlink(missing_ok=True)
    run("systemctl", "daemon-reload")
    for path in (BASE, LIB, ETC):
        shutil.rmtree(path)
    print(
        "Removed only the Necroflow power-loss environment. Development client key retained."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("install", "resume", "teardown"))
    parser.add_argument("--pool", default="default")
    parser.add_argument("--network", default="incusbr-1000")
    parser.add_argument("--image", default="images:ubuntu/24.04")
    parser.add_argument("--host", default="10.64.249.1")
    parser.add_argument("--dev-container", default="aiwork")
    parser.add_argument("--dev-project", default="user-1000")
    parser.add_argument("--repo", default="/home/matteo/Projects/nectroflow/necroflow")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise PermissionError("run this installer on the Incus host with sudo")
    if args.operation == "install":
        install(args)
    elif args.operation == "resume":
        resume(args)
    else:
        with (BASE / "setup.lock").open("a") as setup_lock:
            fcntl.flock(setup_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            teardown()


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        if ACTIVE_CONFIG is not None:
            ACTIVE_CONFIG["stage"] = "failed"
            ACTIVE_CONFIG["error"] = str(exc)
            save(ACTIVE_CONFIG)
        print(f"Setup stopped: {exc}", file=sys.stderr)
        sys.exit(1)
