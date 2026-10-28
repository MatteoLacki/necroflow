"""Concrete artifact classes unify runtime objects and declaration annotations."""

import shlex
from pathlib import Path

import pytest

from necroflow import (
    Command,
    DAG,
    Node,
    Pipeline,
    Resource,
    command,
    output,
    run,
    workflow,
)
from necroflow.fingerprints import _command_identity
from necroflow.rules import Inputs, Outputs


class Raw(Node):
    __module__ = "typed_artifact_baseline"
    filename = "raw.txt"

    def quoted_path(self) -> str:
        return shlex.quote(str(self.path))


class Done(Node):
    __module__ = "typed_artifact_baseline"
    filename = "done.txt"


class SpecializedRaw(Raw):
    filename = "special.txt"


@command
def make(text: str = "hello") -> Raw:
    raw = output(Raw)
    command = "echo {text} > {raw}"
    return raw


@command
def process(raw: Raw, /, count: int = 1, *, threads: Resource[int] = 1) -> Done:
    done = output(Done)

    def command():
        assert isinstance(raw, Raw)
        assert type(done) is Done
        return f"cat {raw.quoted_path()} > {shlex.quote(str(done.path))}"

    return done


def test_concrete_artifacts_and_signature_binding(tmp_path):
    """Artifacts keep their class/methods; calls respect ordinary parameter kinds."""
    pipeline = Pipeline(DAG(tmp_path / "space here"))
    raw = make(pipeline, "hello")
    assert type(raw) is Raw
    assert not hasattr(raw, "node_type")
    pipeline.done = process(pipeline, raw, 2, threads=1)
    with pytest.raises(TypeError, match="positional only|positional-only"):
        process(pipeline, raw=raw)
    with pytest.raises(TypeError, match="too many positional"):
        process(pipeline, raw, 2, 1)
    with pytest.raises(TypeError, match="expected"):
        process(pipeline, raw, "bad")
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert pipeline.done.path.read_text() == "hello\n"


def test_scoped_subtype_and_keyword_only_input(tmp_path):
    """Scoped calls retain subclass acceptance and keyword-only dependencies."""

    @command
    def special() -> SpecializedRaw:
        raw = output(SpecializedRaw)
        command = "touch {raw}"
        return raw

    @command
    def keyword_input(*, raw: Raw) -> Done:
        done = output(Done)
        command = "cp {raw} {done}"
        return done

    @workflow
    def build(pipeline: Pipeline):
        raw = special()
        pipeline.done = keyword_input(raw=raw)
        assert process(raw).parents == [raw]
        with pytest.raises(TypeError, match="too many positional"):
            keyword_input(raw)

    pipeline = Pipeline(DAG(tmp_path))
    build(pipeline)
    assert isinstance(pipeline.done.parents[0], Raw)
    assert type(pipeline.done.parents[0]) is SpecializedRaw


def test_template_identity_matches_pre_merge_fixture(tmp_path):
    """Changing the artifact base/layout does not invalidate template identities."""
    pipeline = Pipeline(DAG(tmp_path))
    raw = Command("source", Inputs(text=str), Outputs(raw=Raw), "echo {text} > {raw}")(
        pipeline, text="hello"
    )
    done = Command("copy", Inputs(raw=Raw), Outputs(done=Done), "cp {raw} {done}")(
        pipeline, raw
    )
    assert [(node.rule_hash, node.provenance_hash) for node in (raw, done)] == [
        (
            "321ecc00d6c5addbb06e1a1017c2c2ad8376046903cb0a8f28cc1b19e0184b28",
            "591223b7cb49f765cc12e4a4eaaf4f704f4cf34dc15aa4016f21082a3ba0b513",
        ),
        (
            "106f4ec96c5613a4b94acbe5baf436b3cd00be9ac5be54ca3edff61cb241eae0",
            "3ec01bb1900a96977c1edaca4d4259fb030a1501e91d76499c0d776a6eb284d4",
        ),
    ]


def external_builder(args):
    assert isinstance(args.inputs.raw, Path)
    assert isinstance(args.outputs.done, Path)
    return (
        f"cp {shlex.quote(str(args.inputs.raw))} {shlex.quote(str(args.outputs.done))}"
    )


def test_builder_binding_identity_is_local_to_nested_commands(tmp_path):
    """Only nested recipes change binding identity; external callbacks keep Paths."""
    external = Command(
        "external", Inputs(raw=Raw), Outputs(done=Done), external_builder
    )
    pipeline = Pipeline(DAG(tmp_path))
    raw = make(pipeline)
    assert external(pipeline, raw).rule_call.resolve().startswith("cp ")
    assert _command_identity(external_builder, None)["kind"] == "python"
    assert (
        _command_identity(process.command, None)["kind"] == "python-nested-artifacts-v1"
    )
