"""Negative typing fixture: each ERROR line must produce a diagnostic."""

from necroflow import shell

from necroflow import Node, Pipeline, Resource, command, outputs


class Raw(Node):
    filename = "raw"


class Other(Node):
    filename = "other"


@command
def copy(raw: Raw, /, *, count: int, threads: Resource[int] = 1) -> Raw:
    result = outputs(Raw)
    shell("cp {raw} {result}")
    return result


def check(pipeline: Pipeline, raw: Raw, other: Other):
    copy(other, count=1)  # ERROR: wrong artifact
    copy(pipeline, other, count=1)  # ERROR: wrong artifact with explicit owner
    copy(raw)  # ERROR: missing keyword-only argument
    copy(raw, count="bad")  # ERROR: wrong scalar
    copy(raw, count=1, threads="bad")  # ERROR: wrong resource
    copy(raw, 1)  # ERROR: keyword-only argument
    copy(raw=raw, count=1)  # ERROR: positional-only argument


@command
def merge(*sources: Raw, threads: Resource[int] = 1) -> Raw:
    result = outputs(Raw)
    shell("cat {sources} > {result}")
    return result


def check_star(pipeline: Pipeline, raw: Raw, other: Other):
    merge(other)  # ERROR: wrong artifact in star group
    merge(pipeline, other)  # ERROR: wrong artifact with explicit owner
    merge([raw])  # ERROR: list must be unpacked
    merge(raw, 2)  # ERROR: resource must be keyword-only
    merge(raw, threads="bad")  # ERROR: wrong resource
    merge(sources=raw)  # ERROR: star group cannot be supplied by keyword
