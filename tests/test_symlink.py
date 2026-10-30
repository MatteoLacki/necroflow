"""Typed external-file ingestion keeps ordinary rule and workflow semantics."""

import os

import pytest

from necroflow import DAG, Node, Pipeline, command, outputs, shell, symlink, workflow


class Source(Node):
    filename = "source.txt"


class Result(Node):
    filename = "result.txt"


@command
def copy(source: Source):
    result = outputs(Result)
    shell("cat {source} > {result}")
    return result


def test_labels_views_and_workflows_share_ingestion_without_io(tmp_path):
    """One type/path denotes one call across views and roots of a shared DAG."""
    dag = DAG(tmp_path / "nodes")
    path = tmp_path / "not-yet-present.txt"

    @workflow
    def fragment(P):
        P.source = symlink(Source, path)

    @workflow
    def build(P):
        fragment(P.subpipeline("a"))
        fragment(P.subpipeline("b"))
        P.alias = symlink(Source, str(path))

    first, second = Pipeline(dag), Pipeline(dag)
    build(first)
    fragment(second)
    assert first["a/source"] is first["b/source"] is first.alias is second.source
    assert first.labels == ("a/source", "b/source", "alias")
    assert len(dag.calls) == 1
    assert not (tmp_path / "nodes").exists()
    assert not path.exists()
    assert not first.finished


def test_generated_identity_distinguishes_types_paths_and_store_roots(tmp_path):
    """Qualified artifact types separate same-named classes; store roots do not."""
    other = type(
        "Source", (Node,), {"__module__": "other", "filename": Source.filename}
    )
    first = Pipeline(DAG(tmp_path / "first"))
    second = Pipeline(DAG(tmp_path / "second"))
    node = symlink(Source, "input.txt", pipeline=first)
    assert (
        node.relative_path
        == symlink(Source, "input.txt", pipeline=second).relative_path
    )
    assert (
        node.relative_path != symlink(other, "input.txt", pipeline=first).relative_path
    )
    assert (
        node.relative_path != symlink(Source, "other.txt", pipeline=first).relative_path
    )
    assert type(node) is Source


@pytest.mark.parametrize("directory", [False, True])
@pytest.mark.parametrize("relative", [False, True])
def test_symlink_runs_for_quoted_file_and_directory_paths(
    tmp_path, monkeypatch, directory, relative
):
    """Paths with spaces and quotes survive ingestion for files and directories."""
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "external 'data'"
    if directory:
        source.mkdir()
        (source / "part.txt").write_text("data\n")
    else:
        source.write_text("data\n")
    path = source.relative_to(tmp_path) if relative else source
    P = Pipeline(DAG(tmp_path / "node store"))
    P.source = symlink(Source, path, pipeline=P)
    P.finish()
    P.dag.require([P.source])
    P.dag.run()
    assert P.source.path.is_symlink()
    assert P.source.path.resolve() == source
    content = P.source.path / "part.txt" if directory else P.source.path
    assert content.read_text() == "data\n"


def test_external_edits_invalidate_cached_consumers(tmp_path):
    """The shorthand retains content tracking across cached runs and source edits."""
    source = tmp_path / "external.txt"
    source.write_text("before\n")

    @workflow
    def build(P):
        P.source = symlink(Source, source)
        P.result = copy(P.source)

    P = Pipeline(DAG(tmp_path / "nodes"))
    build(P)
    P.finish()
    P.dag.require([P.result])
    P.dag.run()
    assert P.result.path.read_text() == "before\n"
    P.dag.run()
    assert all(event.cached for event in P.dag.last_execution_report.values())
    old = source.stat()
    source.write_text("after\n")
    os.utime(source, ns=(old.st_atime_ns, old.st_mtime_ns + 2_000_000_000))
    P.dag.run()
    assert P.result.path.read_text() == "after\n"
    assert not P.dag.last_execution_report[
        P.result.rule_call.relative_path.as_posix()
    ].cached


def test_explicit_owner_overrides_context_and_context_is_restored(tmp_path):
    """Explicit ownership is local to the call; workflow exceptions restore context."""
    outer, inner = Pipeline(DAG(tmp_path / "outer")), Pipeline(DAG(tmp_path / "inner"))

    @workflow
    def child(P):
        P.source = symlink(Source, "input")
        raise ValueError("stop")

    @workflow
    def parent(P):
        explicit = symlink(Source, "input", pipeline=inner)
        assert explicit.rule_call.dag is inner.dag
        with pytest.raises(ValueError, match="stop"):
            child(inner)
        P.source = symlink(Source, "input")
        assert P.source.rule_call.dag is outer.dag

    parent(outer)
    with pytest.raises(RuntimeError, match="active @workflow or pipeline=P"):
        symlink(Source, "input")


def test_invalid_ingestion_fails_without_registering_calls(tmp_path):
    """Invalid contracts, owners, paths, and command nesting leave no DAG entries."""
    P = Pipeline(DAG(tmp_path / "nodes"))
    with pytest.raises(TypeError, match="Node subclass"):
        symlink(str, "input", pipeline=P)
    with pytest.raises(TypeError, match="must define filename"):
        symlink(Node, "input", pipeline=P)
    with pytest.raises(TypeError, match="pipeline must be a Pipeline"):
        symlink(Source, "input", pipeline="wrong")
    with pytest.raises(TypeError):
        symlink(Source, 1, pipeline=P)
    with pytest.raises(TypeError, match="text PathLike"):
        symlink(Source, b"input", pipeline=P)

    @command
    def bad():
        result = outputs(Result)
        symlink(Source, "input")
        shell("touch {result}")
        return result

    @workflow
    def invalid(P):
        bad()

    with pytest.raises(RuntimeError, match="inside command declarations"):
        invalid(P)
    P.finish()
    with pytest.raises(RuntimeError, match="finished"):
        symlink(Source, "input", pipeline=P)
    assert not P.dag.calls
    assert not (tmp_path / "nodes").exists()
