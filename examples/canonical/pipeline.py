from __future__ import annotations

import json
from pathlib import Path

from necroflow import NodeType, Pipeline, Rules


class RawText(NodeType):
    filename = "input.txt"


class ToolConfig(NodeType):
    filename = "tool_config.json"


class ProcessedText(NodeType):
    filename = "processed.txt"


class Summary(NodeType):
    filename = "summary.txt"


R = Rules()
R.text_file("write_tool_config", ToolConfig)


@R.command("cp {path} {raw_text}")
def import_text(path: str):
    return RawText[raw_text]


@R.command("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
def process_text(raw_text: RawText, tool_config: ToolConfig):
    return ProcessedText[processed_text]


@R.command("wc -c {processed_text} > {summary}")
def summarize(processed_text: ProcessedText):
    return Summary[summary]


def canonical_pipeline(config: dict) -> Pipeline:
    P = Pipeline()
    P.raw = R.import_text(path=str(config["input"]))
    P.tool_config = R.write_tool_config(
        text=json.dumps(config.get("tool", {}), sort_keys=True, indent=2) + "\n"
    )
    P.processed = R.process_text(P.raw, P.tool_config)
    P.summary = R.summarize(P.processed)
    return P
