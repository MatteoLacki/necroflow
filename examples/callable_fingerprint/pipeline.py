from __future__ import annotations

import shlex

from necroflow import CommandArgs, NodeType, Pipeline, command, output, symlink_file


class SourceText(NodeType):
    filename = "input.txt"


class SortedText(NodeType):
    filename = "sorted.txt"


@symlink_file
def source_text(path: str):
    source = output(SourceText)
    return source


def sort_command(args: CommandArgs) -> str:
    argv = ["sort"]
    if args.config.reverse:
        argv.append("-r")
    if args.config.unique:
        argv.append("-u")
    argv.append(str(args.inputs.source))
    return f"{shlex.join(argv)} > {shlex.quote(str(args.outputs.sorted_text))}"


@command(sort_command)
def sort_text(source: SourceText, reverse: bool, unique: bool):
    sorted_text = output(SortedText)
    return sorted_text


def sorting_pipeline(pipeline: Pipeline, config: dict) -> None:
    pipeline.source = source_text(pipeline, path=str(config["input"]))
    pipeline.sorted = sort_text(
        pipeline,
        pipeline.source,
        reverse=config.get("reverse", False),
        unique=config.get("unique", False),
    )
