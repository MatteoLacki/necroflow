from __future__ import annotations

from necroflow import shell

import json
from pathlib import Path

from necroflow import (
    Node,
    Pipeline,
    Resource,
    command,
    symlink,
    text_file,
    outputs,
    workflow,
)


class RawText(Node):
    filename = "input.txt"


class ToolConfig(Node):
    filename = "tool_config.json"


class ProcessedText(Node):
    filename = "processed.txt"


class Summary(Node):
    filename = "summary.txt"


@text_file
def write_tool_config(text: str):
    tool_config = outputs(ToolConfig)
    return tool_config


@command
def process_text(
    raw_text: RawText, tool_config: ToolConfig, threads: Resource[int] = 1
):
    processed_text = outputs(ProcessedText)
    shell("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
    return processed_text


@command
def summarize(processed_text: ProcessedText):
    summary = outputs(Summary)
    shell("wc -c {processed_text} > {summary}")
    return summary


@workflow
def canonical_pipeline(P: Pipeline, config: dict) -> None:
    P.raw = symlink(RawText, config["input"])
    P.tool_config = write_tool_config(
        text=json.dumps(config.get("tool", {}), sort_keys=True, indent=2) + "\n",
    )
    P.processed = process_text(P.raw, P.tool_config)
    P.summary = summarize(P.processed)
