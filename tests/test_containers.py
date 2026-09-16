"""Tests for Docker config values, `{env}:` command prefixes, and native launch."""

import copy
import hashlib
import json
import os
import pickle
import shlex
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import tomlkit

from necroflow import (
    DAG,
    Docker,
    Inputs,
    NodeType,
    Outputs,
    Pipeline,
    command,
    output,
)
from necroflow.containers import docker_argv
from necroflow.fingerprints import PROVENANCE_HASH_DOMAIN, canonical_bytes

IMAGE = "example.org/tool@sha256:" + "0" * 64
OTHER_IMAGE = "example.org/tool@sha256:" + "1" * 64


class Text(NodeType):
    filename = "text.txt"


class Copy(NodeType):
    filename = "copy.txt"


class Tree(NodeType):
    filename = "tree"


@command("printf hello > {text}")
def host_text():
    text = output(Text)
    return text


@command("{env}:cat {source} > {copy}")
def docker_copy(source: Text, env: Docker):
    copy = output(Copy)
    return copy


@command("cat {source} > {copy}")
def host_copy_with_env(source: Text, env: Docker):
    copy = output(Copy)
    return copy


@command("{label}:cat {source} > {copy}")
def label_prefix(source: Text, label: str):
    copy = output(Copy)
    return copy


@command("cat {source} > {copy}")
def plain_copy(source: Copy):
    copy = output(Text)
    return copy


@command("{env}:cat {sources} > {copy}")
def docker_concat(sources: tuple[Text, ...], env: Docker):
    copy = output(Copy)
    return copy


@command("{env}:exit 7")
def docker_failure(env: Docker):
    text = output(Text)
    return text


def callback_command(args):
    return "{env}:" + f"cat {shlex.quote(str(args.inputs.source))} > copy.txt"


@command(callback_command)
def docker_callback(source: Text, env: Docker):
    copy = output(Copy)
    return copy


def host_callback_command(args):
    return f"cat {shlex.quote(str(args.inputs.source))} > copy.txt"


@command(host_callback_command)
def host_callback(source: Text, env: Docker):
    copy = output(Copy)
    return copy


@command(host_callback_command)
def plain_callback(source: Text):
    copy = output(Copy)
    return copy


def empty_callback_command(args):
    return "{env}:  "


@command(empty_callback_command)
def empty_callback(env: Docker):
    text = output(Text)
    return text


FAKE_DOCKER = """#!{python}
import json, os, subprocess, sys
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
workdir = next(a for a in sys.argv if a.startswith("--workdir=")).split("=", 1)[1]
sys.exit(subprocess.run(["/bin/sh", "-c", sys.argv[-1]], cwd=workdir).returncode)
"""


@pytest.fixture
def fake_docker(tmp_path, monkeypatch):
    """Put a `docker` on PATH that logs argv and runs the body on the host."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "docker"
    script.write_text(FAKE_DOCKER.format(python=sys.executable))
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "docker.log"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_LOG", str(log))

    def calls():
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    return calls


def build(tmp_path, rule, *, docker=None, name="nodes"):
    dag = DAG(tmp_path / name)
    p = Pipeline(dag)
    p.text = host_text(p)
    kwargs = {} if docker is None else {"env": docker}
    p.out = rule(p, p.text, **kwargs)
    return dag, p


def test_docker_requires_digest_and_platform():
    """Mutable tags and implicit platforms would make cached identities lie.

    The value is validated when constructed, before any rule call interns it.
    """
    with pytest.raises(ValueError, match="digest-pinned"):
        Docker("example.org/tool:latest", platform="linux/amd64")
    with pytest.raises(ValueError, match="platform"):
        Docker(IMAGE, platform="")
    with pytest.raises(TypeError, match="run_args"):
        Docker(IMAGE, platform="linux/amd64", run_args="--rm")


def test_docker_defaults_and_toml_spellings_hash_identically(tmp_path):
    """Job-TOML and Python spellings of one Docker value share call identity.

    TOML yields str subclasses and list arrays, which the fingerprinter frames
    differently from str/tuple unless the value normalises them.
    """
    doc = tomlkit.parse(f"""
[env]
image = "{IMAGE}"
platform = "linux/amd64"
run_args = {json.dumps(list(Docker.DEFAULT_RUN_ARGS))}
""")
    from_toml = Docker(**doc["env"])
    from_python = Docker(IMAGE, platform="linux/amd64")
    assert from_python["run_args"] == Docker.DEFAULT_RUN_ARGS
    _, first = build(tmp_path, docker_copy, docker=from_toml)
    _, second = build(tmp_path, docker_copy, docker=from_python)
    assert first.out.relative_path == second.out.relative_path


def test_docker_is_read_only_but_copyable():
    """Mutating a Docker value after interning would desynchronise its paths."""
    docker = Docker(IMAGE, platform="linux/amd64")
    for mutate in (
        lambda: docker.__setitem__("image", OTHER_IMAGE),
        lambda: docker.update(image=OTHER_IMAGE),
        lambda: docker.pop("image"),
        docker.clear,
    ):
        with pytest.raises(TypeError, match="read-only"):
            mutate()
    assert copy.deepcopy(docker) == docker
    assert pickle.loads(pickle.dumps(docker)) == docker
    assert type(copy.copy(docker)) is Docker


def test_prefix_only_selects_docker_inputs(tmp_path):
    """Only a leading placeholder naming a Docker input switches execution.

    Existing templates that happen to start with `{x}:` keep their meaning, and
    a Docker input without a prefix is ordinary config.
    """
    docker = Docker(IMAGE, platform="linux/amd64")
    assert docker_copy.container_capable
    assert not label_prefix.container_capable
    assert not host_copy_with_env.container_capable
    p = Pipeline(DAG(tmp_path / "label"))
    call = label_prefix(p, host_text(p), label="x").rule_call
    assert call.container is None
    assert call.resolve().startswith("x:cat ")
    _, p = build(tmp_path, host_copy_with_env, docker=docker, name="host")
    assert p.out.rule_call.container is None


def test_empty_container_body_is_rejected():
    """A prefix alone is not a command."""
    with pytest.raises(ValueError, match="body is empty"):
        command("{env}:   ", Inputs(env=Docker), Outputs(text=Text), name="empty")


def test_empty_container_callback_body_is_rejected(tmp_path):
    """Callbacks may only select Docker with a non-empty remaining command."""
    dag = DAG(tmp_path / "nodes")
    p = Pipeline(dag)
    node = empty_callback(p, env=Docker(IMAGE, platform="linux/amd64"))
    with pytest.raises(TypeError, match="empty container command body"):
        node.rule_call.resolve()


def test_docker_settings_change_consumer_and_descendant_paths(tmp_path):
    """Image, platform, and run args are lineage for consumers and descendants.

    Switching back to an earlier value reuses the earlier canonical Nodes.
    """
    base = Docker(IMAGE, platform="linux/amd64")
    variants = [
        Docker(OTHER_IMAGE, platform="linux/amd64"),
        Docker(IMAGE, platform="linux/arm64"),
        Docker(IMAGE, platform="linux/amd64", run_args=("--rm",)),
    ]
    dag = DAG(tmp_path / "nodes")
    p = Pipeline(dag)
    text = host_text(p)
    first = docker_copy(p, text, env=base)
    first_child = plain_copy(p, first)
    for variant in variants:
        other = docker_copy(p, text, env=variant)
        assert other.relative_path != first.relative_path
        assert other.rule_call.rule_hash == first.rule_call.rule_hash
        assert plain_copy(p, other).relative_path != first_child.relative_path
    assert docker_copy(p, text, env=Docker(IMAGE, platform="linux/amd64")) is first


def test_host_rules_do_not_carry_container_policy(tmp_path):
    """Adding container support must not move any host-only call.

    Only container-capable rules add the policy version to provenance.
    """
    docker = Docker(IMAGE, platform="linux/amd64")
    dag = DAG(tmp_path / "nodes")
    p = Pipeline(dag)
    text = host_text(p)
    host = plain_callback(p, text).rule_call
    with_env = host_callback(p, text, env=docker).rule_call
    assert not plain_callback.container_capable
    assert host_callback.container_capable
    expected = hashlib.sha256(
        canonical_bytes(
            {
                "domain": PROVENANCE_HASH_DOMAIN,
                "rule_hash": host.rule_hash,
                "config": {},
                "execution_context": {},
                "parents": host._parent_identity(),
            },
            path="provenance",
        )
    ).hexdigest()
    assert host.provenance_hash == expected
    assert with_env.container is None


def test_argv_mounts_inputs_and_workdir(tmp_path):
    """Inputs bind read-only at node paths from resolved sources; cwd is the workdir.

    Symlinked inputs expose only their target, repeated inputs mount once, and
    framework-owned arguments follow user run args with ids expanded late.
    """
    external = tmp_path / "external dir" / "reads.txt"
    external.parent.mkdir()
    external.write_text("x")
    dag = DAG(tmp_path / "node store")
    p = Pipeline(dag)
    text = host_text(p)
    docker = Docker(IMAGE, platform="linux/amd64")
    assert docker["run_args"][3] == "--user={uid}:{gid}"
    node = docker_concat(p, (text, text), env=docker)
    text.path.parent.mkdir(parents=True)
    text.path.symlink_to(external)
    call = node.rule_call
    argv = docker_argv(call, docker, call.resolve())
    user_args = [
        "--rm",
        "--init",
        "--pull=missing",
        f"--user={os.getuid()}:{os.getgid()}",
        "--env=HOME=/tmp",
    ]
    assert argv[:7] == ["docker", "run", *user_args]
    assert argv[7] == "--platform=linux/amd64"
    assert argv[8:] == [
        "--mount",
        f"type=bind,src={external},dst={text.path},readonly",
        "--mount",
        f"type=bind,src={call.workdir},dst={call.workdir}",
        f"--workdir={call.workdir}",
        "--entrypoint=/bin/sh",
        IMAGE,
        "-c",
        call.resolve(),
    ]
    assert not call.resolve().startswith("{env}")


def test_argv_rejects_mount_delimiters(tmp_path):
    """Docker parses --mount as CSV; a comma would silently change the mount."""
    dag = DAG(tmp_path / "a,b")
    p = Pipeline(dag)
    docker = Docker(IMAGE, platform="linux/amd64")
    node = docker_copy(p, host_text(p), env=docker)
    with pytest.raises(ValueError, match="must not contain"):
        docker_argv(node.rule_call, docker, "true")


def test_run_executes_prefixed_template_through_docker(tmp_path, fake_docker):
    """Plain dag.run() launches prefixed rules in Docker and host rules on the host.

    Provenance metadata stays TOML-serialisable and records the selected input.
    """
    dag, p = build(tmp_path, docker_copy, docker=Docker(IMAGE, platform="linux/amd64"))
    p.finish()
    dag.require([p.out])
    dag.run()
    assert p.out.path.read_text() == "hello"
    (argv,) = fake_docker()
    assert argv[0] == "run" and IMAGE in argv
    deps = tomlkit.parse(
        (p.out.rule_call.workdir / ".rip" / "dependencies.toml").read_text()
    )
    assert deps["execution"]["container_input"] == "env"
    assert deps["config"]["env"]["image"] == IMAGE
    assert not deps["command"]["realized"].startswith("{env}")
    dag.run()
    assert len(fake_docker()) == 1, "cached calls must not launch containers"


def test_run_executes_prefixed_callback_through_docker(tmp_path, fake_docker):
    """Callbacks select Docker by returning the literal `{env}:` prefix."""
    dag, p = build(
        tmp_path, docker_callback, docker=Docker(IMAGE, platform="linux/amd64")
    )
    p.finish()
    dag.require([p.out])
    dag.run()
    assert p.out.path.read_text() == "hello"
    assert len(fake_docker()) == 1


def test_container_failure_is_a_process_failure(tmp_path, fake_docker):
    """A nonzero container exit fails the call exactly like a host command."""
    dag = DAG(tmp_path / "nodes")
    p = Pipeline(dag)
    p.out = docker_failure(p, env=Docker(IMAGE, platform="linux/amd64"))
    p.finish()
    dag.require([p.out])
    with pytest.raises(subprocess.CalledProcessError) as info:
        dag.run()
    assert info.value.returncode == 7
    assert p.out.rule_call.state_file.read_text() == "failed"


@pytest.mark.skipif(
    not os.environ.get("NECROFLOW_DOCKER_IMAGE"),
    reason="opt-in: set NECROFLOW_DOCKER_IMAGE to a digest-pinned image with /bin/sh",
)
def test_real_docker_execution(tmp_path):
    """Real Docker reads inputs read-only and writes user-owned outputs in cwd."""
    root = tmp_path / "with space"
    dag, p = build(
        root,
        docker_copy,
        docker=Docker(os.environ["NECROFLOW_DOCKER_IMAGE"], platform="linux/amd64"),
    )
    p.finish()
    dag.require([p.out])
    dag.run()
    assert p.out.path.read_text() == "hello"
    assert p.out.path.stat().st_uid == os.getuid()
