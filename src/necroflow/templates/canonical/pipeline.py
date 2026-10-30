from __future__ import annotations

import necroflow as nf

import json
from pathlib import Path


class RawText(nf.Node):
    filename = "input.txt"


class ToolConfig(nf.Node):
    filename = "tool_config.json"


class ProcessedText(nf.Node):
    filename = "processed.txt"


class Summary(nf.Node):
    filename = "summary.txt"


@nf.text_file
def write_tool_config(text: str):
    tool_config = nf.outputs(ToolConfig)
    return tool_config


@nf.command
def process_text(raw_text: RawText, tool_config: ToolConfig):
    processed_text = nf.outputs(ProcessedText)
    nf.shell("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
    return processed_text


@nf.command
def summarize(processed_text: ProcessedText):
    summary = nf.outputs(Summary)
    nf.shell("wc -c {processed_text} > {summary}")
    return summary


@nf.workflow
def canonical_pipeline(P: nf.Pipeline, config: dict) -> None:
    P.raw = nf.symlink(RawText, config["input"])
    P.tool_config = write_tool_config(
        text=json.dumps(config.get("tool", {}), sort_keys=True, indent=2) + "\n",
    )
    P.processed = process_text(P.raw, P.tool_config)
    P.summary = summarize(P.processed)
