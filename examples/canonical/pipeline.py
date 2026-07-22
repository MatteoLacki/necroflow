from __future__ import annotations

import json
from pathlib import Path

from necroflow import NodeType, Pipeline, command, text_file, output


class RawText(NodeType):
    filename = "input.txt"


class ToolConfig(NodeType):
    filename = "tool_config.json"


class ProcessedText(NodeType):
    filename = "processed.txt"


class Summary(NodeType):
    filename = "summary.txt"


@text_file
def write_tool_config(text: str):
    tool_config = output(ToolConfig)
    return tool_config


@command("cp {path} {raw_text}")
def import_text(path: str):
    raw_text = output(RawText)
    return raw_text


@command("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
def process_text(raw_text: RawText, tool_config: ToolConfig):
    processed_text = output(ProcessedText)
    return processed_text


@command("wc -c {processed_text} > {summary}")
def summarize(processed_text: ProcessedText):
    summary = output(Summary)
    return summary


def canonical_pipeline(P: Pipeline, config: dict) -> None:
    P.raw = import_text(P, path=str(config["input"]))
    P.tool_config = write_tool_config(
        P,
        text=json.dumps(config.get("tool", {}), sort_keys=True, indent=2) + "\n",
    )
    P.processed = process_text(P, P.raw, P.tool_config)
    P.summary = summarize(P, P.processed)
