"""Negative typing fixture: each ERROR line must produce a diagnostic."""

from necroflow import Node, Pipeline, Resource, command, output


class Raw(Node):
    filename = "raw"


class Other(Node):
    filename = "other"


@command
def copy(raw: Raw, /, *, count: int, threads: Resource[int] = 1) -> Raw:
    result = output(Raw)
    command = "cp {raw} {result}"
    return result


def check(pipeline: Pipeline, raw: Raw, other: Other):
    copy(other, count=1)  # ERROR: wrong artifact
    copy(pipeline, other, count=1)  # ERROR: wrong artifact with explicit owner
    copy(raw)  # ERROR: missing keyword-only argument
    copy(raw, count="bad")  # ERROR: wrong scalar
    copy(raw, count=1, threads="bad")  # ERROR: wrong resource
    copy(raw, 1)  # ERROR: keyword-only argument
    copy(raw=raw, count=1)  # ERROR: positional-only argument
