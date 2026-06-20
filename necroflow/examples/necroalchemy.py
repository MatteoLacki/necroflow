"""
Necroalchemy — a silly 17-node text-transformation pipeline.

Structure (nontrivial):
  - Seed fans out to 4 parallel branches (upper/lower/reverse/sort_chars)
  - Diamond: upper + lower → merge_cases
  - Co-outputs: make_stats produces Stats + Audit from the same rule call
  - 4-way merge: combine_all(merged, rot13, repeated, reversed)
  - Combined forks into 3 consumers (make_stats, sort_combined, count_lines)
  - Final convergence: grand_summary(stats, line_counts, final_mix)

Run from the necroflow/ directory:
    source .venv/bin/activate
    python examples/necroalchemy.py

Renders saved to:
    /tmp/necroalchemy_hello.txt   — single-word pipeline
    /tmp/necroalchemy_dag.txt     — full 3-word DAG
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    _ip = get_ipython()  # type: ignore[name-defined]
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from necroflow import Constraints, DAG, Inputs, Outputs, Pipeline, Rules, node_types

# ── node types ────────────────────────────────────────────────────────────────

(
    Seed,
    Upper,
    Lower,
    Reversed,
    Rot13,
    Repeated,
    Merged,
    SortedChars,
    UniqueChars,
    Combined,
    Stats,
    Audit,
    UpperRot,
    SortedCombined,
    LineCounts,
    FinalMix,
    GrandSummary,
) = node_types(
    "seed=seed.txt"
    " upper=upper.txt"
    " lower=lower.txt"
    " reversed=rev.txt"
    " rot13=rot13.txt"
    " repeated=rep.txt"
    " merged=merged.txt"
    " sorted_chars=sorted.txt"
    " unique_chars=unique.txt"
    " combined=combined.txt"
    " stats=stats.txt"
    " audit=audit.txt"
    " upper_rot=upper_rot.txt"
    " sorted_combined=sorted_combined.txt"
    " line_counts=lines.txt"
    " final_mix=final_mix.txt"
    " grand_summary=summary.txt"
)

# ── rules (16 rules → 17 nodes) ───────────────────────────────────────────────
# Each command sleeps 1-3 s so the scheduler and parallelism are visible.

_S = "sleep $((RANDOM % 3 + 1)) && "

R = Rules()

# step 1 — materialise the seed word
R.register(
    "make_seed",
    Inputs(word=str),
    Outputs(seed=Seed),
    _S + "echo {word} > {seed}",
)

# steps 2-4 — three independent transforms of the seed (fan-out)
R.register(
    "to_upper",
    Inputs(seed=Seed),
    Outputs(upper=Upper),
    _S + "tr a-z A-Z < {seed} > {upper}",
)

R.register(
    "to_lower",
    Inputs(seed=Seed),
    Outputs(lower=Lower),
    _S + "tr A-Z a-z < {seed} > {lower}",
)

R.register(
    "reverse_it",
    Inputs(seed=Seed),
    Outputs(reversed=Reversed),
    _S + "rev {seed} > {reversed}",
)

# step 5 — character inventory of the seed
R.register(
    "sort_chars",
    Inputs(seed=Seed),
    Outputs(sorted_chars=SortedChars),
    _S + "grep -o . {seed} | sort > {sorted_chars}",
)

# step 6 — rot13 on the uppercased text
R.register(
    "encode_rot13",
    Inputs(upper=Upper),
    Outputs(rot13=Rot13),
    _S + "tr A-Za-z N-ZA-Mn-za-m < {upper} > {rot13}",
)

# step 7 — repeat the lowercased word n times
R.register(
    "repeat_word",
    Inputs(lower=Lower, n=int),
    Outputs(repeated=Repeated),
    _S + "for _ in $(seq {n}); do cat {lower}; done > {repeated}",
)

# step 8 — diamond merge: put upper and lower side by side
R.register(
    "merge_cases",
    Inputs(upper=Upper, lower=Lower),
    Outputs(merged=Merged),
    _S + "paste {upper} {lower} > {merged}",
)

# step 9 — unique character set
R.register(
    "unique_chars",
    Inputs(sorted_chars=SortedChars),
    Outputs(unique_chars=UniqueChars),
    _S + "uniq {sorted_chars} > {unique_chars}",
)

# step 10 — 4-way merge into one blob
R.register(
    "combine_all",
    Inputs(merged=Merged, rot13=Rot13, repeated=Repeated, reversed=Reversed),
    Outputs(combined=Combined),
    _S + "cat {merged} {rot13} {repeated} {reversed} > {combined}",
)

# step 11+12 — co-outputs: byte count + unique-char count (from the same rule call)
R.register(
    "make_stats",
    Inputs(combined=Combined, unique_chars=UniqueChars),
    Outputs(stats=Stats, audit=Audit),
    _S + "wc -c {combined} > {stats} && wc -l {unique_chars} > {audit}",
)

# step 13 — shout the rot13 (uppercase again)
R.register(
    "shout_rot",
    Inputs(rot13=Rot13),
    Outputs(upper_rot=UpperRot),
    _S + "tr a-z A-Z < {rot13} > {upper_rot}",
)

# step 14 — sort the combined blob
R.register(
    "sort_combined",
    Inputs(combined=Combined),
    Outputs(sorted_combined=SortedCombined),
    _S + "sort {combined} > {sorted_combined}",
)

# step 15 — count lines in combined blob
R.register(
    "count_lines",
    Inputs(combined=Combined),
    Outputs(line_counts=LineCounts),
    _S + "wc -l < {combined} > {line_counts}",
)

# step 16 — mix the shouted rot13 with the sorted blob
R.register(
    "final_mix",
    Inputs(upper_rot=UpperRot, sorted_combined=SortedCombined),
    Outputs(final_mix=FinalMix),
    _S + "cat {upper_rot} {sorted_combined} > {final_mix}",
)

# step 17 — grand convergence: stats + line counts + the final mix
R.register(
    "grand_summary",
    Inputs(stats=Stats, line_counts=LineCounts, final_mix=FinalMix),
    Outputs(grand_summary=GrandSummary),
    _S + "cat {stats} {line_counts} {final_mix} > {grand_summary}",
)


# ── pipeline factory ──────────────────────────────────────────────────────────

def alchemy_pipeline(word: str, n: int = 3) -> Pipeline:
    """Build one necroalchemy pipeline for a given word."""
    P = Pipeline()
    P.seed            = R.make_seed(word=word)
    P.upper           = R.to_upper(P.seed)
    P.lower           = R.to_lower(P.seed)
    P.reversed        = R.reverse_it(P.seed)
    P.sorted_chars    = R.sort_chars(P.seed)
    P.rot13           = R.encode_rot13(P.upper)
    P.repeated        = R.repeat_word(P.lower, n=n)
    P.merged          = R.merge_cases(P.upper, P.lower)
    P.unique_chars    = R.unique_chars(P.sorted_chars)
    P.combined        = R.combine_all(P.merged, P.rot13, P.repeated, P.reversed)
    P.stats, P.audit  = R.make_stats(P.combined, P.unique_chars)
    P.upper_rot       = R.shout_rot(P.rot13)
    P.sorted_combined = R.sort_combined(P.combined)
    P.line_counts     = R.count_lines(P.combined)
    P.final_mix       = R.final_mix(P.upper_rot, P.sorted_combined)
    P.summary         = R.grand_summary(P.stats, P.line_counts, P.final_mix)
    return P


# ── run ───────────────────────────────────────────────────────────────────────

WORDS = ["necroflow", "snakemake", "python"]
OUTDIR = Path("/tmp/necroalchemy_out")

if __name__ == "__main__":
    P = alchemy_pipeline("hello", n=2)
    P.save("/tmp/necroalchemy_hello.txt")
    print("Pipeline render → /tmp/necroalchemy_hello.txt")

    dag = DAG(OUTDIR)
    for word in WORDS:
        dag.add(alchemy_pipeline(word, n=3))

    dag.save("/tmp/necroalchemy_dag.txt")
    print("DAG render      → /tmp/necroalchemy_dag.txt")

    dag.execute(keep_going=True)
