"""Scheduler comparison — fifo vs connected_component on a diamond DAG.

fifo_scheduler submits nodes in registration order (topological).
connected_component_scheduler (default) prioritises the smallest
remaining connected component, finishing pipelines early rather than
interleaving them.

Run:
    python examples/schedulers.py
"""
from pathlib import Path
from necroflow import DAG, Inputs, Outputs, NodeType, Rules, fifo_scheduler

class Text(NodeType):
    filename = "text.txt"

class Upper(NodeType):
    filename = "upper.txt"

class Lower(NodeType):
    filename = "lower.txt"

class Merged(NodeType):
    filename = "merged.txt"

R = Rules()
R.register("make_text",  Inputs(word=str),              Outputs(text=Text),     "echo {word} > {text}")
R.register("to_upper",   Inputs(text=Text),              Outputs(upper=Upper),   "tr a-z A-Z < {text} > {upper}")
R.register("to_lower",   Inputs(text=Text),              Outputs(lower=Lower),   "tr A-Z a-z < {text} > {lower}")
R.register("merge",      Inputs(upper=Upper, lower=Lower), Outputs(merged=Merged), "paste {upper} {lower} > {merged}")


def diamond(word: str):
    from necroflow import Pipeline
    P = Pipeline()
    P.text   = R.make_text(word=word)
    P.upper  = R.to_upper(P.text)
    P.lower  = R.to_lower(P.text)
    P.merged = R.merge(P.upper, P.lower)
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
