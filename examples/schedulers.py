"""Scheduler example — built-in FIFO vs custom reverse-ready order.

Run:
    python examples/schedulers.py
"""

from pathlib import Path
from necroflow import DAG, Node, Pipeline, command, outputs, workflow


class Text(Node):
    filename = "text.txt"


class Upper(Node):
    filename = "upper.txt"


class Lower(Node):
    filename = "lower.txt"


class Merged(Node):
    filename = "merged.txt"


@command
def make_text(word: str):
    text = outputs(Text)
    command = "echo {word} > {text}"
    return text


@command
def to_upper(text: Text):
    upper = outputs(Upper)
    command = "tr a-z A-Z < {text} > {upper}"
    return upper


@command
def to_lower(text: Text):
    lower = outputs(Lower)
    command = "tr A-Z a-z < {text} > {lower}"
    return lower


@command
def merge(upper: Upper, lower: Lower):
    merged = outputs(Merged)
    command = "paste {upper} {lower} > {merged}"
    return merged


@workflow
def diamond(P, word: str) -> None:
    P.text = make_text(word=word)
    P.upper = to_upper(P.text)
    P.lower = to_lower(P.text)
    P.merged = merge(P.upper, P.lower)


OUTDIR = Path("/tmp/schedulers_example")

# FIFO is default.
dag1 = DAG(OUTDIR / "default")
for word in ["hello", "world"]:
    pipeline = Pipeline(dag1)
    diamond(pipeline, word)
    pipeline.finish()
    dag1.require(pipeline.sinks())
print("--- fifo_scheduler (default) ---")
dag1.run()


def reverse_ready(ready, remaining, available_resources):
    """Prefer most recently registered ready RuleCall."""
    return list(reversed(ready))


dag2 = DAG(OUTDIR / "reverse")
for word in ["hello", "world"]:
    pipeline = Pipeline(dag2)
    diamond(pipeline, word)
    pipeline.finish()
    dag2.require(pipeline.sinks())
print("--- reverse_ready (custom) ---")
dag2.run(scheduler=reverse_ready)
