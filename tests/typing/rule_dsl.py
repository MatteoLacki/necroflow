"""Static-analysis fixture for rule output and dynamic pipeline shapes."""

from necroflow import (
    CommandArgs,
    Constraints,
    Inputs,
    Node,
    NodeType,
    Outputs,
    Pipeline,
    command,
    output,
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


source_node: Node = make_source(text="value")
left_node: Node
right_node: Node
left_node, right_node = split_source(source_node)
callback_node: Node = callback_rule(source_node)

pipeline = Pipeline()
pipeline.source = source_node
pipeline.left, pipeline.right = split_source(pipeline.source)


def accepts_node(node: Node) -> None:
    pass


accepts_node(pipeline.left)
accepts_node(pipeline.right)
