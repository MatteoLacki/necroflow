"""Executable declarations construct typed artifacts without hidden runtime rewriting."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from necroflow import DAG, Node, Pipeline, Resource, Shell, command, outputs, run
from necroflow.construction import _current_build


class Product(Node):
    filename = "product.txt"


@command
def construct(text: str, threads: Resource[int] = 1):
    product = outputs(Product)
    assert isinstance(product, Product)
    assert product.path == product.rule_call.workdir / Product.filename
    print(f"construct {text}")
    if text == "fail":
        Shell("touch {product}")
        raise RuntimeError("body failed")
    options = "{text}" if threads > 1 else "single"
    Shell(f"printf %s {options} > {{product}}")
    return product


def test_body_executes_per_call_before_interning(tmp_path, capsys):
    """Construction repeats while identical successful calls share canonical artifacts."""
    pipeline = Pipeline(DAG(tmp_path, resources={"construct": {"threads": 2}}))
    first = construct(pipeline, "hello")
    assert first is construct(pipeline, "hello")
    assert capsys.readouterr().out.count("construct hello") == 2
    assert len(pipeline.dag.calls) == 1
    assert first.rule_call.compute_identity()[1:] == (
        first.rule_hash,
        first.provenance_hash,
    )
    pipeline.result = first
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert first.path.read_text() == "hello"


def test_failed_construction_restores_context_and_leaves_dag_empty(tmp_path):
    """A body exception cannot leak context or register partially constructed nodes."""
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    with pytest.raises(RuntimeError, match="body failed"):
        construct(pipeline, "fail")
    assert _current_build.get() is None
    assert not pipeline.dag.calls
    assert not pipeline.dag.nodes_dir.exists()
    with pytest.raises(RuntimeError, match="active @command"):
        Shell("true")
    assert construct(pipeline, "ok").rule_call.command


def test_concurrent_construction_isolation(tmp_path):
    """Each thread binds Shell and outputs to its own call."""

    def build(i):
        pipeline = Pipeline(DAG(tmp_path / str(i)))
        return construct(pipeline, str(i), threads=2)

    with ThreadPoolExecutor(max_workers=4) as pool:
        nodes = list(pool.map(build, range(8)))
    for i, node in enumerate(nodes):
        assert node.config == {"text": str(i)}
        assert f"printf %s {i} " in node.rule_call.resolve()
    assert _current_build.get() is None


def test_nested_command_rejected(tmp_path):
    """Workflow composition cannot hide dependency construction inside a command."""

    @command
    def outer():
        product = outputs(Product)
        construct("child")
        Shell("touch {product}")
        return product

    pipeline = Pipeline(DAG(tmp_path))
    with pytest.raises(RuntimeError, match="compose them in a workflow"):
        outer(pipeline)
    assert not pipeline.dag.calls
    assert _current_build.get() is None


@pytest.mark.parametrize("mode", ["missing", "twice", "replacement"])
def test_shell_and_return_contracts(tmp_path, mode):
    """Runtime validation rejects incomplete, ambiguous, or replaced artifacts."""

    @command
    def invalid(mode: str):
        product = outputs(Product)
        if mode != "missing":
            Shell("touch {product}")
        if mode == "twice":
            Shell("echo extra")
        if mode == "replacement":
            product = None
        return product

    pipeline = Pipeline(DAG(tmp_path))
    with pytest.raises(ValueError, match="Shell|output artifacts"):
        invalid(pipeline, mode)
    assert not pipeline.dag.calls
    assert _current_build.get() is None
