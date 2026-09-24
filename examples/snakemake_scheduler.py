"""Snakemake's greedy job selector, ported to necroflow's scheduler protocol.

Snakemake's default is `--scheduler ilp`, which solves a 0-1 multidimensional
knapsack with PuLP and *falls back to this greedy heuristic* whenever the ILP
times out (10s), errors, or selects nothing. The greedy path is the one worth
porting: it needs no solver dependency, and it is what Snakemake actually runs
whenever the ILP is unavailable or too slow.

The heuristic is from Akcay, Li & Xu, "A Greedy Algorithm for the General
Multidimensional Knapsack Problem" (Annals of Operations Research, 2012):
repeatedly take the highest-reward job that still fits in the remaining
resource capacity, decrement capacity, repeat.

Snakemake's reward is the tuple ``(priority, temp_size, input_size)``, compared
lexicographically — priority dominates, ties broken by how many bytes of
temporary input the job would make deletable, then by total input size. The
intent is to finish jobs that free disk soonest.

Run:
    python examples/snakemake_scheduler.py

Three deliberate deviations from Snakemake, all forced by necroflow's model:

1. **Priority.** Snakemake has a `priority:` directive. necroflow has no such
   concept, so this reads `priority` out of a rule's constraints. Constraints
   that are never given a cap are ignored by the executor's resource admission,
   so `Constraints(priority=10)` is inert as a resource and usable as a rank.

2. **temp_size.** Snakemake sizes only inputs explicitly marked `temp()`.
   necroflow has no `temp()` marker — under `autoclean` *any* intermediate
   becomes deletable once its consumers finish — so this sizes all parent
   outputs. That makes temp_size and input_size coincide more often than in
   Snakemake; the tuple still orders correctly, it just ties more.

3. **Liveness.** Snakemake's selector returns only the jobs that fit, and raises
   if a job can never fit. necroflow's executor does its own admission control
   and relies on the scheduler to keep offering work — returning an empty list
   while nothing is running would end the run early, leaving nodes unbuilt. So
   this returns the fitting jobs first and then the rest in reward order,
   preserving Snakemake's ordering while letting necroflow's own solo-job
   fallback handle anything oversized.
"""

from pathlib import Path

from necroflow import DAG, NodeType, Pipeline, command, output
from necroflow.rules import Constraints, Inputs, Outputs, Rule


def _output_size(node) -> int:
    """Total bytes of a node's output, or 0 when it does not exist yet."""
    path = node.path
    if path is None:
        return 0
    try:
        if path.is_dir():
            return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
        return path.stat().st_size
    except OSError:
        return 0


def _job_reward(node) -> tuple[int, int, int]:
    """Snakemake's (priority, temp_size, input_size) reward tuple."""
    priority = node.rule.constraints.get("priority", 0)
    parent_sizes = [_output_size(parent) for parent in node.parents]
    temp_size = sum(parent_sizes)  # see deviation 2 in the module docstring
    input_size = sum(parent_sizes)
    return (priority, temp_size, input_size)


def make_snakemake_greedy_scheduler(greediness: float = 1.0):
    """Return a scheduler applying Snakemake's greedy 0-1 MDKP heuristic.

    ``greediness`` mirrors Snakemake's ``--scheduler-greedy-greediness``; at the
    default 1.0 each job is considered exactly once, which is the fast path.
    """
    if not 0 < greediness <= 1:
        raise ValueError(f"greediness must be in (0, 1], got {greediness!r}")

    def snakemake_greedy_scheduler(ready, remaining, available_resources):
        if not ready:
            return []
        # Only capped resources constrain selection; uncapped rule constraints
        # (including our 'priority' rank) are invisible to admission control.
        dimensions = sorted(available_resources)
        capacity = [available_resources[name] for name in dimensions]
        weights = [
            [node.resources.get(name, 0) for name in dimensions] for node in ready
        ]
        rewards = [_job_reward(node) for node in ready]

        selected: list[int] = []
        candidates = set(range(len(ready)))
        while candidates:
            # A job is takeable when every dimension it consumes still has room.
            takeable = [
                j
                for j in candidates
                if all(
                    weight <= budget
                    for weight, budget in zip(weights[j], capacity)
                    if weight > 0
                )
            ]
            if not takeable:
                break
            chosen = max(takeable, key=lambda j: rewards[j])
            selected.append(chosen)
            candidates.discard(chosen)
            capacity = [
                budget - weight for budget, weight in zip(capacity, weights[chosen])
            ]

        # Fitting jobs first, then everything else by reward — see deviation 3.
        leftover = sorted(candidates, key=lambda j: rewards[j], reverse=True)
        return [ready[j] for j in selected] + [ready[j] for j in leftover]

    return snakemake_greedy_scheduler


# ── demo ──────────────────────────────────────────────────────────────────────


class Raw(NodeType):
    filename = "raw.txt"


class Small(NodeType):
    filename = "small.txt"


class Big(NodeType):
    filename = "big.txt"


@command("head -c 200000 /dev/zero | tr '\\0' 'a' > {raw}")
def make_raw(tag: str):
    raw = output(Raw)
    return raw


# Two consumers of the same parent, distinguished only by declared priority.
r_low = Rule(
    "consume_low",
    Inputs(raw=Raw),
    Outputs(small=Small),
    "wc -c < {raw} > {small}",
    Constraints(threads=2, priority=1),
)
r_high = Rule(
    "consume_high",
    Inputs(raw=Raw),
    Outputs(big=Big),
    "wc -c < {raw} > {big}",
    Constraints(threads=2, priority=100),
)


if __name__ == "__main__":
    outdir = Path("/tmp/necroflow-snakemake-scheduler")
    dag = DAG(outdir)
    pipeline = Pipeline(dag)
    pipeline.raw = make_raw(pipeline, tag="demo")
    pipeline.low = r_low(pipeline, pipeline.raw)
    pipeline.high = r_high(pipeline, pipeline.raw)
    pipeline.finish()
    dag.require(pipeline.sinks())

    order: list[str] = []
    base = make_snakemake_greedy_scheduler()

    def recording_scheduler(ready, remaining, available_resources):
        chosen = base(ready, remaining, available_resources)
        if chosen:
            order.append(",".join(node.rule.__name__ for node in chosen))
        return chosen

    print("--- snakemake greedy scheduler (threads capped at 2) ---")
    dag.run(resource_caps={"threads": 2}, scheduler=recording_scheduler)
    print("scheduler returned, in order:")
    for line in order:
        print(f"  {line}")
