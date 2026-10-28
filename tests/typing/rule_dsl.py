"""Static-analysis fixture for rule output and dynamic pipeline shapes."""

from typing import Annotated, assert_type

from necroflow import (
    CommandArgs,
    Constraints,
    DAG,
    Inputs,
    Many,
    Node,
    NodeType,
    Outputs,
    Pipeline,
    command,
    output,
    workflow,
)


class Source(NodeType):
    filename = "source.txt"


class Left(NodeType):
    filename = "left.txt"


class Right(NodeType):
    filename = "right.txt"


@command("printf %s {text} > {source}")
def make_source(text: str):
    source = output(Source)
    return source


@command("printf %s {text} > {source}")
def make_default_source(text: str = "default"):
    source = output(Source)
    return source


@command("cp {source} {left} && cp {source} {right}")
def split_source(source: Source):
    left = output(Left)
    right = output(Right)
    return left, right


factory_rule = command(
    "cp {source} {factory_left}",
    Inputs(source=Source),
    Outputs(factory_left=Left),
    Constraints(threads=2),
    name="factory_rule",
    doc="Factory rule.",
)


def callback_command(args: CommandArgs) -> str:
    return f"cp {args.inputs.source} {args.outputs.callback_left}"


@command(callback_command)
def callback_rule(source: Source):
    callback_left = output(Left)
    return callback_left


@command("cat {sources} > {left}")
def merge_sources(sources: Annotated[tuple[Source, ...], Many()]):
    left = output(Left)
    return left


pipeline = Pipeline(DAG("/tmp/necroflow-typing"))
source_node: Node = make_source(pipeline, text="value")
default_source_node: Node = make_default_source(pipeline)
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
    source = output(Source)
    command = "printf %s {text} > {source}"
    return source


@command(repeat=2)
def nested_source(text: str, threads: Resource[int] = 4):
    source = output(Source)

    def command():
        return f"echo {text} > {source}"

    return source


inline_node: Node = inline_source(pipeline, text="hello", threads=2)
nested_node: Node = nested_source(pipeline, text="hello", threads=2)


# Exact inference matters: assignment to bare Node would also accept Any.
def check_output_inference() -> None:
    assert_type(output(Source), Node[Source])
    assert_type(output(Left), Node[Left])
    assert_type(output(Source).node_type, type[Source])


assert_type(make_source(pipeline, text="value"), Node[Source])
assert_type(make_default_source(pipeline), Node[Source])
assert_type(split_source(pipeline, source_node), tuple[Node[Left], Node[Right]])
assert_type(callback_rule(pipeline, source_node), Node[Left])
assert_type(inline_source(pipeline, text="hello"), Node[Source])
assert_type(nested_source(pipeline, text="hello"), Node[Source])


@command
def annotated_source(text: str) -> Node[Source]:
    source = output(Source)
    command = "printf %s {text} > {source}"
    return source


assert_type(annotated_source(pipeline, text="hello"), Node[Source])
