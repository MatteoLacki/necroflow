"""Scheduler comparison — fifo vs connected_component on a diamond DAG.

fifo_scheduler submits nodes in registration order (topological).
connected_component_scheduler (default) prioritises the smallest
remaining connected component, finishing pipelines early rather than
interleaving them.

Run:
    python examples/schedulers.py
"""

from pathlib import Path
from necroflow import DAG, NodeType, command, fifo_scheduler


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
    return Text[text]


@command("tr a-z A-Z < {text} > {upper}")
def to_upper(text: Text):
    return Upper[upper]


@command("tr A-Z a-z < {text} > {lower}")
def to_lower(text: Text):
    return Lower[lower]


@command("paste {upper} {lower} > {merged}")
def merge(upper: Upper, lower: Lower):
    return Merged[merged]


def diamond(word: str):
    from necroflow import Pipeline

    P = Pipeline()
    P.text = make_text(word=word)
    P.upper = to_upper(P.text)
    P.lower = to_lower(P.text)
    P.merged = merge(P.upper, P.lower)
    return P


OUTDIR = Path("/tmp/schedulers_example")

# default scheduler
dag1 = DAG(OUTDIR / "default")
for word in ["hello", "world"]:
    dag1.add(diamond(word))
print("--- connected_component_scheduler (default) ---")
dag1.execute()

# fifo scheduler
dag2 = DAG(OUTDIR / "fifo")
for word in ["hello", "world"]:
    dag2.add(diamond(word))
print("--- fifo_scheduler ---")
dag2.execute(scheduler=fifo_scheduler)
