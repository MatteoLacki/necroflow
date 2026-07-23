"""Scheduler comparison — fifo vs connected_component on a diamond DAG.

fifo_scheduler submits nodes in registration order (topological).
connected_component_scheduler (default) prioritises the smallest
remaining connected component, finishing pipelines early rather than
interleaving them.

Run:
    python examples/schedulers.py
"""

from pathlib import Path
from necroflow import DAG, NodeType, Pipeline, command, fifo_scheduler, output


class Text(NodeType):
    filename = "text.txt"


class Upper(NodeType):
    filename = "upper.txt"


class Lower(NodeType):
    filename = "lower.txt"


class Merged(NodeType):
    filename = "merged.txt"


@command("echo {word} > {text}")
def make_text(word: str):
    text = output(Text)
    return text


@command("tr a-z A-Z < {text} > {upper}")
def to_upper(text: Text):
    upper = output(Upper)
    return upper


@command("tr A-Z a-z < {text} > {lower}")
def to_lower(text: Text):
    lower = output(Lower)
    return lower


@command("paste {upper} {lower} > {merged}")
def merge(upper: Upper, lower: Lower):
    merged = output(Merged)
    return merged


def diamond(P, word: str) -> None:
    P.text = make_text(P, word=word)
    P.upper = to_upper(P, P.text)
    P.lower = to_lower(P, P.text)
    P.merged = merge(P, P.upper, P.lower)


OUTDIR = Path("/tmp/schedulers_example")

# default scheduler
dag1 = DAG(OUTDIR / "default")
for word in ["hello", "world"]:
    pipeline = Pipeline(dag1)
    diamond(pipeline, word)
    dag1.require(pipeline.sinks())
print("--- connected_component_scheduler (default) ---")
dag1.execute()

# fifo scheduler
dag2 = DAG(OUTDIR / "fifo")
for word in ["hello", "world"]:
    pipeline = Pipeline(dag2)
    diamond(pipeline, word)
    dag2.require(pipeline.sinks())
print("--- fifo_scheduler ---")
dag2.execute(scheduler=fifo_scheduler)
