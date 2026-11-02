"""Workflow context must preserve explicit ownership and label semantics."""

from necroflow import shell

from concurrent.futures import ThreadPoolExecutor
import inspect
from threading import Barrier
from typing import Annotated

import pytest

from necroflow import (
    DAG,
    Inputs,
    Many,
    Node,
    Outputs,
    Pipeline,
    command,
    outputs,
    symlink,
    text_file,
    workflow,
)


class Text(Node):
    filename = "text.txt"


class Left(Node):
    filename = "left.txt"


class Right(Node):
    filename = "right.txt"


@command
def split(source: Text):
    left = outputs(Left)
    right = outputs(Right)
    shell("cp {source} {left} && cp {source} {right}")
    return left, right


@command
def merge(sources: Annotated[tuple[Left | Right, ...], Many(min=2)]):
    result = outputs(Text)
    shell("cat {sources} > {result}")
    return result


@command
def source_rule(value: str):
    result = outputs(Text)
    shell("printf %s {value} > {result}")
    return result


def test_workflow_rules_and_file_helpers_execute_with_explicit_labels(tmp_path):
    """All rule forms share scoped ownership without implicitly publishing locals."""
    external = tmp_path / "external.txt"
    external.write_text("external\n")

    @workflow
    def build(P):
        local = text_file(Text, text="hello\n")
        left, right = split(local)
        P.result = merge((left, right))
        P.external = symlink(Text, external)
        P.source = source_rule(value="source")
        # Explicit and scoped calls have identical fingerprints and canonical Nodes.
        assert source_rule(P, value="source") is P.source
        assert split(P, local) == (left, right)

    P = Pipeline(DAG(tmp_path / "nodes"))
    build(P)
    assert P.labels == ("result", "external", "source")
    assert not P.finished
    P.finish()
    P.dag.require(P.sinks())
    P.dag.run()
    assert P.result.path.read_text() == "hello\nhello\n"
    assert P.external.path.read_text() == "external\n"
    assert P.source.path.read_text() == "source"


def test_nested_views_preserve_labels_deduplication_and_parent_context(tmp_path):
    """Nested views qualify explicit labels and reuse canonical parent computations."""

    @workflow
    def qc(P, source):
        P.left, P.right = split(source)

    @workflow
    def sample(P):
        P.source = text_file(Text, text="shared")
        qc(P.subpipeline("qc"), P.source)

    @workflow
    def cohort(P):
        sample(P.subpipeline("samples/A"))
        sample(P.subpipeline("samples/B"))
        P.tail = text_file(Text, text="tail")

    P = Pipeline(DAG(tmp_path))
    cohort(P)
    assert P.labels == (
        "samples/A/source",
        "samples/A/qc/left",
        "samples/A/qc/right",
        "samples/B/source",
        "samples/B/qc/left",
        "samples/B/qc/right",
        "tail",
    )
    assert P["samples/A/qc/left"] is P["samples/B/qc/left"]
    assert len(P.dag.calls) == 3
    with pytest.raises(RuntimeError, match="active @workflow"):
        text_file(Text, text="outside")


def test_nested_independent_owner_and_exceptions_restore_context(tmp_path):
    """Success, rejected owners, and exceptions all restore the enclosing DAG context."""
    outer = Pipeline(DAG(tmp_path / "outer"))
    inner = Pipeline(DAG(tmp_path / "inner"))

    @workflow
    def child(P, fail=False):
        result = text_file(Text, text="child")
        assert result.rule_call.dag is P.dag
        if fail:
            raise ValueError("child failed")
        return result

    def helper():
        return text_file(Text, text="helper")

    @workflow
    def parent(P):
        assert child(inner).rule_call.dag is inner.dag
        with pytest.raises(ValueError, match="child failed"):
            child(inner, fail=True)
        with pytest.raises(TypeError, match="open Pipeline"):
            child(None)
        P.result = helper()
        assert P.result.rule_call.dag is outer.dag

    parent(outer)
    with pytest.raises(ValueError, match="child failed"):
        child(inner, fail=True)
    with pytest.raises(RuntimeError, match="active @workflow"):
        helper()
    child(inner)
    assert not inner.finished and not outer.finished


def test_explicit_owner_wins_without_changing_active_context(tmp_path):
    """Explicit ownership remains usable anywhere and never replaces workflow context."""
    outer = Pipeline(DAG(tmp_path / "outer"))
    explicit = Pipeline(DAG(tmp_path / "explicit"))
    before = text_file(Text, pipeline=explicit, text="explicit")

    @workflow
    def build(P):
        assert text_file(Text, pipeline=explicit, text="explicit") is before
        P.result = text_file(Text, text="implicit")
        assert P.result.rule_call.dag is P.dag
        with pytest.raises(ValueError, match="different DAG"):
            split(before)
        explicit.finish()
        with pytest.raises(RuntimeError, match="construction has finished"):
            text_file(Text, pipeline=explicit, text="late")
        P.next = text_file(Text, text="next")

    build(outer)
    assert outer.next.rule_call.dag is outer.dag


@pytest.mark.parametrize(
    "args, detail",
    [((), "none was supplied"), ((None,), "got NoneType"), ((42,), "got int")],
)
def test_workflow_requires_first_positional_pipeline(args, detail):
    """Invalid owners fail before the workflow body with actionable diagnostics."""

    @workflow
    def build(P):
        pytest.fail("invalid owner reached workflow body")

    with pytest.raises(TypeError) as error:
        build(*args)
    assert str(error.value) == (
        f"Workflow {build.__qualname__}: first argument must be an open Pipeline; {detail}."
    )


def test_finished_root_and_views_are_rejected_before_body(tmp_path):
    """Finishing a root prevents entry to workflows using either root or views."""

    @workflow
    def build(P):
        pytest.fail("finished owner reached workflow body")

    P = Pipeline(DAG(tmp_path))
    view = P.subpipeline("sample")
    P.finish()
    for owner in (P, view):
        with pytest.raises(RuntimeError) as error:
            build(owner)
        assert str(error.value) == (
            f"Workflow {build.__qualname__}: first argument must be an open Pipeline; "
            "supplied Pipeline has finished construction."
        )
    assert not P.dag.calls


def test_finishing_inside_workflow_prevents_further_rule_calls(tmp_path):
    """Every scoped rule call checks that its owner remains open."""

    @workflow
    def build(P):
        P.finish()
        text_file(Text, text="late")

    P = Pipeline(DAG(tmp_path))
    with pytest.raises(RuntimeError, match="construction has finished"):
        build(P)
    assert not P.dag.calls


def test_deferred_functions_are_rejected_at_decoration():
    """Deferred bodies cannot outlive the synchronous workflow context."""

    async def coroutine(P):
        pass

    def generator(P):
        yield None

    async def async_generator(P):
        yield None

    for fn, kind in (
        (coroutine, "coroutine"),
        (generator, "generator"),
        (async_generator, "async-generator"),
    ):
        with pytest.raises(TypeError) as error:
            workflow(fn)
        assert str(error.value) == (
            f"Workflow {fn.__qualname__}: only synchronous functions are supported; "
            f"{kind} functions defer execution beyond the workflow context."
        )


def test_workflow_preserves_signature_metadata_and_return_value(tmp_path):
    """Wrapping changes ownership context, not the callable's public metadata or result."""

    def original(scope: Pipeline, value: str = "default"):
        """Original docstring."""
        return text_file(Text, text=value)

    wrapped = workflow(original)
    assert inspect.signature(wrapped) == inspect.signature(original)
    assert wrapped.__name__ == original.__name__
    assert wrapped.__doc__ == original.__doc__
    P = Pipeline(DAG(tmp_path))
    assert wrapped(P) is text_file(Text, pipeline=P, text="default")
    assert P.labels == ()


def test_independent_threads_do_not_share_active_pipeline(tmp_path):
    """Concurrent construction with separate DAGs cannot steal another workflow's owner."""
    barrier = Barrier(2)

    @workflow
    def build(P):
        barrier.wait(timeout=10)
        P.source = text_file(Text, text="same")
        barrier.wait(timeout=10)
        return P.source

    pipelines = [Pipeline(DAG(tmp_path / str(i))) for i in range(2)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        nodes = list(pool.map(build, pipelines))
    for node, P in zip(nodes, pipelines):
        assert node.rule_call.dag is P.dag
    assert nodes[0].relative_path == nodes[1].relative_path
    with pytest.raises(RuntimeError, match="active @workflow"):
        text_file(Text, text="outside")
