"""Static-analysis fixture for rule output and dynamic pipeline shapes."""

from necroflow import Node, NodeType, Pipeline, command, output


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


source_node: Node = make_source(text="value")
left_node: Node
right_node: Node
left_node, right_node = split_source(source_node)

pipeline = Pipeline()
pipeline.source = source_node
pipeline.left, pipeline.right = split_source(pipeline.source)


def accepts_node(node: Node) -> None:
    pass


accepts_node(pipeline.left)
accepts_node(pipeline.right)
