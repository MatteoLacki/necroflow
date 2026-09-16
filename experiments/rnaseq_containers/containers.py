"""Experiment-local Docker runner; Necroflow itself stays on the host."""

import json
import os
import subprocess
from pathlib import Path

from necroflow import RuleCall

ROOT = Path(__file__).resolve().parent
# Bump when changing execution semantics, since runner code is outside rule hashes.
CONTAINER_POLICY = 1


def images():
    return json.loads((ROOT / "images.json").read_text())


def docker_argv(call, root=ROOT):
    """Use identical host/container paths, including symlink targets."""
    image = call.config["image"]
    if "@sha256:" not in image:
        raise ValueError("Container rules require a digest-pinned image")
    if call.config["container_policy"] != CONTAINER_POLICY:
        raise ValueError("Unsupported container execution policy")
    workdir = call.workdir.resolve()
    workdir.relative_to(root.resolve())
    return [
        "docker",
        "run",
        "--rm",
        "--init",
        "--pull=never",
        "--platform=linux/amd64",
        "--network=none",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--cpus",
        "1",
        "--memory",
        "2g",
        "--mount",
        f"type=bind,src={root.resolve()},dst={root.resolve()},readonly",
        "--mount",
        f"type=bind,src={workdir},dst={workdir}",
        "--workdir",
        str(workdir),
        "--env",
        "HOME=/tmp",
        "--entrypoint",
        "/bin/bash",
        image,
        "-euo",
        "pipefail",
        "-c",
        call.resolve(),
    ]


def run_container(call, log_path, *, root=ROOT):
    """Host input rules run normally; image-configured commands run in Docker."""
    if "image" not in call.config:
        return RuleCall.run(call, log_path)
    call.workdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    argv = docker_argv(call, root)
    with log_path.open("w") as log:
        subprocess.run(argv, check=True, stdout=log, stderr=subprocess.STDOUT)
