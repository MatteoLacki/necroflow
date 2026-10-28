from __future__ import annotations

import json
from pathlib import Path

from necroflow import Node, Pipeline, Resource, command, text_file, outputs, workflow


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
def import_text(path: str):
    raw_text = outputs(RawText)
    command = "cp {path} {raw_text}"
    return raw_text


@command
def process_text(
    raw_text: RawText, tool_config: ToolConfig, threads: Resource[int] = 1
):
    processed_text = outputs(ProcessedText)
    command = "tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}"
    return processed_text


@command
def summarize(processed_text: ProcessedText):
    summary = outputs(Summary)
    command = "wc -c {processed_text} > {summary}"
    return summary


@workflow
def canonical_pipeline(P: Pipeline, config: dict) -> None:
    P.raw = import_text(path=str(config["input"]))
    P.tool_config = write_tool_config(
        text=json.dumps(config.get("tool", {}), sort_keys=True, indent=2) + "\n",
    )
    P.processed = process_text(P.raw, P.tool_config)
    P.summary = summarize(P.processed)
