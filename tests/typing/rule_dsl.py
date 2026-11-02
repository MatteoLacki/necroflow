"""Static-analysis fixture for rule output and dynamic pipeline shapes."""

from necroflow import shell

from typing import Annotated, assert_type

from necroflow import (
    DAG,
    Many,
    Node,
    Pipeline,
    command,
    outputs,
    workflow,
)


class Source(Node):
    filename = "source.txt"


class Left(Node):
    filename = "left.txt"


class Right(Node):
    filename = "right.txt"


@command
def make_source(text: str):
    source = outputs(Source)
    shell("printf %s {text} > {source}")
    return source


@command
def make_default_source(text: str = "default"):
    source = outputs(Source)
    shell("printf %s {text} > {source}")
    return source


@command
def split_source(source: Source):
    left, right = outputs(Left, Right)
    shell("cp {source} {left} && cp {source} {right}")
    return left, right


@command
def callback_rule(source: Source):
    callback_left = outputs(Left)
    shell("cp {source} {callback_left}")
    return callback_left


@command
def merge_sources(sources: Annotated[tuple[Source, ...], Many()]):
    left = outputs(Left)
    shell("cat {sources} > {left}")
    return left


pipeline = Pipeline(DAG("/tmp/necroflow-typing"))
source_node = make_source(pipeline, text="value")
default_source_node = make_default_source(pipeline)
left_node: Node
right_node: Node
left_node, right_node = split_source(pipeline, source_node)
callback_node: Node = callback_rule(pipeline, source_node)
merged_node: Node = merge_sources(pipeline, (source_node,))

pipeline.source = source_node
pipeline.left, pipeline.right = split_source(pipeline, pipeline.source)


def accepts_node(node: Node) -> None:
    pass


accepts_node(pipeline.left)
accepts_node(pipeline.right)


@workflow
def scoped_sample(P: Pipeline, text: str) -> Node:
    P.source = make_source(text=text)
    P.left, P.right = split_source(P.source)
    P.callback = callback_rule(P.source)
    return P.left


@workflow
def scoped_cohort(P: Pipeline, text: str) -> None:
    P.result = scoped_sample(P.subpipeline("sample"), text)
    P.merged = merge_sources((make_default_source(),))


scoped_cohort(Pipeline(DAG("/tmp/necroflow-workflow-typing")), "value")


from necroflow import Resource


@command
def inline_source(text: str, threads: Resource[int] = 4):
    source = outputs(Source)
    shell("printf %s {text} > {source}")
    return source


@command(repeat=2)
def nested_source(text: str, threads: Resource[int] = 4):
    source = outputs(Source)

    def build_command():
        return f"echo {text} > {source.path}"

    shell(build_command())

    return source


inline_node: Node = inline_source(pipeline, text="hello", threads=2)
nested_node: Node = nested_source(pipeline, text="hello", threads=2)


# Exact inference matters: assignment to bare Node would also accept Any.
def check_output_inference() -> None:
    assert_type(outputs(Source), Source)
    assert_type(outputs(Left), Left)
    assert_type(outputs(Left, Right), tuple[Left, Right])
    assert_type(type(outputs(Source)), type[Source])


assert_type(make_source(pipeline, text="value"), Source)
assert_type(make_default_source(pipeline), Source)
assert_type(split_source(pipeline, source_node), tuple[Left, Right])
assert_type(callback_rule(pipeline, source_node), Left)
assert_type(inline_source(pipeline, text="hello"), Source)
assert_type(nested_source(pipeline, text="hello"), Source)


@command
def annotated_source(text: str) -> Source:
    source = outputs(Source)
    shell("printf %s {text} > {source}")
    return source


assert_type(annotated_source(pipeline, text="hello"), Source)


from pathlib import Path
from necroflow import text_file, symlink


class SubSource(Source):
    filename = "sub.txt"


@text_file
def text_source(text: str) -> Source:
    source = outputs(Source)
    return source


@command
def precise(source: Source, /, *, count: int = 1, threads: Resource[int] = 1) -> Left:
    left = outputs(Left)

    def build_command():
        return f"cat {source.path} > {left.path}"

    shell(build_command())

    return left


def check_precise_calls(source: Source, sub: SubSource) -> None:
    assert_type(source.path, Path)
    assert_type(precise(source, count=2), Left)
    assert_type(precise(pipeline, sub, threads=2), Left)
    assert_type(text_source("hello"), Source)
    assert_type(symlink(SubSource, Path("/tmp/input")), SubSource)
    assert_type(symlink(Source, "/tmp/input", pipeline=pipeline), Source)


@command
def fractional(gpu: Resource[float] = 0.5, scratch: Resource[str] = "2Gi"):
    source = outputs(Source)

    def build_command():
        assert_type(gpu, float)
        assert_type(scratch, str)
        return f"echo {gpu} {scratch} > {source.path}"

    shell(build_command())

    return source


assert_type(fractional(pipeline, gpu=0.25), Source)


@command
def merge_list(sources: list[Source]) -> Left:
    left = outputs(Left)
    assert_type(sources, list[Source])
    shell("cat {sources} > {left}")
    return left


assert_type(merge_list(pipeline, [source_node]), Left)


@command
def merge_star(*sources: Source, threads: Resource[int] = 1) -> Left:
    left = outputs(Left)
    assert_type(sources, tuple[Source, ...])
    shell("cat {sources} > {left}")
    return left


assert_type(merge_star(source_node, threads=2), Left)
assert_type(merge_star(pipeline, source_node, source_node, threads=2), Left)
assert_type(merge_star(pipeline, *[source_node]), Left)
assert_type(merge_star(pipeline), Left)
