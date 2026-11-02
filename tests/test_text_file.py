"""Typed text artifacts use ordinary workflow, identity, and execution semantics."""

import pytest

import necroflow as nf


class Config(nf.Node):
    filename = "config.toml"


@nf.command
def copy_config(config: Config):
    result = nf.outputs(Config)
    nf.shell("cat {config} > {result}")
    return result


def test_text_file_interns_across_labels_views_and_roots_without_io(tmp_path):
    """Same type and text share one artifact independently of labels or call site."""
    dag = nf.DAG(tmp_path / "nodes")

    @nf.workflow
    def build(P):
        P.config = nf.text_file(Config, "value = 1\n")
        P.subpipeline("child").config = nf.text_file(Config, text="value = 1\n")

    first, second = nf.Pipeline(dag), nf.Pipeline(dag)
    build(first)
    build(second)
    assert first.config is first["child/config"] is second.config
    assert type(first.config) is Config
    assert len(dag.calls) == 1
    assert not (tmp_path / "nodes").exists()


def test_text_file_identity_tracks_type_filename_text_and_encoding(tmp_path):
    """Output contract and bytes determine identity; store roots do not."""
    first = nf.Pipeline(nf.DAG(tmp_path / "first"))
    second = nf.Pipeline(nf.DAG(tmp_path / "second"))
    original = nf.text_file(Config, "hello", pipeline=first)
    assert (
        original.relative_path
        == nf.text_file(Config, "hello", pipeline=second).relative_path
    )
    other_type = type(
        "Config", (nf.Node,), {"__module__": "other", "filename": Config.filename}
    )
    other_filename = type(
        "Config",
        (nf.Node,),
        {"__module__": Config.__module__, "filename": "other.toml"},
    )
    variants = [
        nf.text_file(other_type, "hello", pipeline=first),
        nf.text_file(other_filename, "hello", pipeline=first),
        nf.text_file(Config, "changed", pipeline=first),
        nf.text_file(Config, "hello", encoding="utf-16-le", pipeline=first),
    ]
    assert all(node.relative_path != original.relative_path for node in variants)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16-le"])
def test_text_file_executes_exact_content_and_caches_consumers(tmp_path, encoding):
    """The Python materializer preserves arbitrary text and caches downstream work."""
    text = "héllo ' \" $HOME {braces}\n" * 10000
    P = nf.Pipeline(nf.DAG(tmp_path / "nodes"))
    P.config = nf.text_file(Config, text, encoding=encoding, pipeline=P)
    P.result = copy_config(P, P.config)
    P.finish()
    P.dag.require([P.result])
    P.dag.run()
    assert (
        P.config.path.read_bytes()
        == P.result.path.read_bytes()
        == text.encode(encoding)
    )
    P.dag.run()
    assert all(event.cached for event in P.dag.last_execution_report.values())


def test_text_file_invalid_calls_leave_no_artifacts(tmp_path):
    """Bad contracts, ownership, nesting, and closed pipelines cannot register calls."""
    P = nf.Pipeline(nf.DAG(tmp_path / "nodes"))
    with pytest.raises(RuntimeError, match="active @workflow or pipeline=P"):
        nf.text_file(Config, "text")
    with pytest.raises(TypeError, match="Node subclass"):
        nf.text_file(str, "text", pipeline=P)
    with pytest.raises(TypeError, match="must define filename"):
        nf.text_file(nf.Node, "text", pipeline=P)
    with pytest.raises(TypeError, match="pipeline must be a Pipeline"):
        nf.text_file(Config, "text", pipeline="wrong")
    with pytest.raises(TypeError):
        nf.text_file(Config, 123, pipeline=P)

    @nf.command
    def invalid():
        result = nf.outputs(Config)
        nf.text_file(Config, "text")
        nf.shell("touch {result}")
        return result

    @nf.workflow
    def build(P):
        invalid()

    with pytest.raises(RuntimeError, match="inside command declarations"):
        build(P)
    P.finish()
    with pytest.raises(RuntimeError, match="finished"):
        nf.text_file(Config, "text", pipeline=P)
    assert not P.dag.calls
    assert not (tmp_path / "nodes").exists()
