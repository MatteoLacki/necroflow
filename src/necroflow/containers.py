"""Docker execution settings, ``{name}:`` command prefixes, and ``docker run`` argv."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from necroflow.rule_call import RuleCall

# Bump when framework-owned launch semantics change (fixed argv, mounts, shell):
# that code sits outside rule hashes, so container-capable calls hash this instead.
CONTAINER_POLICY = 1

_PREFIX = re.compile(r"\s*\{([A-Za-z_][A-Za-z0-9_]*)\}:")
# Docker parses --mount values as CSV.
_MOUNT_UNSAFE = (",", '"', "\n")


class Docker(dict):
    """Read-only Docker settings passed to a rule as ordinary config.

    Being a mapping, the value fingerprints and serializes like any other
    config, so image, platform, and run args all enter provenance.
    ``{uid}`` and ``{gid}`` in ``run_args`` are expanded only at launch.
    """

    DEFAULT_RUN_ARGS = (
        "--rm",
        "--init",
        "--pull=missing",
        "--user={uid}:{gid}",
        "--env=HOME=/tmp",
    )

    def __init__(
        self,
        image: str,
        platform: str,
        run_args: Sequence[str] = DEFAULT_RUN_ARGS,
    ):
        if not isinstance(image, str) or "@sha256:" not in image:
            raise ValueError(f"Docker image must be digest-pinned, got {image!r}")
        if not isinstance(platform, str) or not platform:
            raise ValueError(f"Docker platform must be non-empty, got {platform!r}")
        if isinstance(run_args, str) or not all(
            isinstance(arg, str) for arg in run_args
        ):
            raise TypeError("Docker run_args must be a sequence of strings")
        # Plain str/tuple: TOML strings are str subclasses and TOML arrays are
        # lists, which fingerprint differently from the Python tuple spelling.
        super().__init__(
            image=str(image),
            platform=str(platform),
            run_args=tuple(str(arg) for arg in run_args),
        )

    def __reduce__(self):
        return (type(self), (self["image"], self["platform"], self["run_args"]))

    def _read_only(self, *args, **kwargs):
        raise TypeError("Docker values are read-only")

    __setitem__ = __delitem__ = clear = pop = popitem = _read_only
    setdefault = update = __ior__ = _read_only


def docker_input_names(input_specs: Mapping[str, Any]) -> frozenset[str]:
    """Return inputs annotated exactly ``Docker``."""
    return frozenset(name for name, ann in input_specs.items() if ann is Docker)


def split_prefix(command: str, docker_inputs: frozenset[str]) -> tuple[str | None, str]:
    """Split a leading ``{name}:`` selecting a Docker input from the command body.

    Commands whose leading placeholder is not a Docker input are returned
    unchanged, so existing ``{x}:`` templates keep their meaning.
    """
    match = _PREFIX.match(command)
    if match is None or match.group(1) not in docker_inputs:
        return None, command
    return match.group(1), command[match.end() :]


def _mount(src, dst, *, readonly: bool) -> str:
    for path in (src, dst):
        if any(char in str(path) for char in _MOUNT_UNSAFE):
            raise ValueError(
                f"Docker bind mount path must not contain ',', '\"', or newlines: "
                f"{str(path)!r}"
            )
    spec = f"type=bind,src={src},dst={dst}"
    return spec + ",readonly" if readonly else spec


def docker_argv(call: RuleCall, docker: Docker, body: str) -> list[str]:
    """Build ``docker run`` argv: user run_args, then framework-owned arguments.

    Inputs are bound read-only at their node-store paths from their resolved
    sources; the call workdir is bound writable and used as cwd.
    """
    ids = {"{uid}": str(os.getuid()), "{gid}": str(os.getgid())}
    run_args = []
    for arg in docker["run_args"]:
        for placeholder, value in ids.items():
            arg = arg.replace(placeholder, value)
        run_args.append(arg)
    mounts = {
        parent.path: _mount(parent.path.resolve(), parent.path, readonly=True)
        for parent in call.parents
    }
    workdir = call.workdir
    return [
        "docker",
        "run",
        *run_args,
        f"--platform={docker['platform']}",
        *(arg for spec in mounts.values() for arg in ("--mount", spec)),
        "--mount",
        _mount(workdir.resolve(), workdir, readonly=False),
        f"--workdir={workdir}",
        "--entrypoint=/bin/sh",
        docker["image"],
        "-c",
        body,
    ]
