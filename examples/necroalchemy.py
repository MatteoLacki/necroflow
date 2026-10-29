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

from necroflow import Shell

import sys
from pathlib import Path

try:
    _ip = get_ipython()  # type: ignore[name-defined]
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from necroflow import DAG, Node, Pipeline, command, outputs, workflow

# ── node types ────────────────────────────────────────────────────────────────


class Seed(Node):
    """The input word written to disk — starting point for all transforms."""

    filename = "seed.txt"


class Upper(Node):
    """The seed word converted to uppercase."""

    filename = "upper.txt"


class Lower(Node):
    """The seed word converted to lowercase."""

    filename = "lower.txt"


class Reversed(Node):
    """The seed word with characters in reverse order."""

    filename = "rev.txt"


class Rot13(Node):
    """ROT13 cipher applied to the uppercased seed."""

    filename = "rot13.txt"


class Repeated(Node):
    """The lowercased word repeated n times, one per line."""

    filename = "rep.txt"


class Merged(Node):
    """Uppercase and lowercase variants pasted side by side."""

    filename = "merged.txt"


class SortedChars(Node):
    """Individual characters of the seed, sorted alphabetically."""

    filename = "sorted.txt"


class UniqueChars(Node):
    """Deduplicated character set derived from the sorted character list."""

    filename = "unique.txt"


class Combined(Node):
    """All four transform outputs concatenated into one blob."""

    filename = "combined.txt"


class Stats(Node):
    """Byte count of the combined blob."""

    filename = "stats.txt"


class Audit(Node):
    """Unique-character count of the combined blob."""

    filename = "audit.txt"


class UpperRot(Node):
    """ROT13 output re-uppercased for maximum shouting energy."""

    filename = "upper_rot.txt"


class SortedCombined(Node):
    """Combined blob with lines sorted lexicographically."""

    filename = "sorted_combined.txt"


class LineCounts(Node):
    """Total line count of the combined blob."""

    filename = "lines.txt"


class FinalMix(Node):
    """Shouted ROT13 and sorted blob concatenated."""

    filename = "final_mix.txt"


class GrandSummary(Node):
    """Final assembly of stats, line counts, and the final mix."""

    filename = "summary.txt"


# ── rules (16 rules → 17 nodes) ───────────────────────────────────────────────
# Each command sleeps 1-3 s so the scheduler and parallelism are visible.


@command  # step 1 — materialise the seed word
def make_seed(word: str):
    """Write the input word to a file — the starting point for all transforms."""
    seed = outputs(Seed)
    Shell("sleep $((RANDOM % 3 + 1)) && echo {word} | tee {seed}")
    return seed


@command  # steps 2-4 — fan-out
def to_upper(seed: Seed):
    """Convert all characters to uppercase."""
    upper = outputs(Upper)
    Shell("sleep $((RANDOM % 3 + 1)) && tr a-z A-Z < {seed} | tee {upper}")
    return upper


@command
def to_lower(seed: Seed):
    """Convert all characters to lowercase."""
    lower = outputs(Lower)
    Shell("sleep $((RANDOM % 3 + 1)) && tr A-Z a-z < {seed} | tee {lower}")
    return lower


@command
def reverse_it(seed: Seed):
    """Reverse the character order of the seed."""
    reversed = outputs(Reversed)
    Shell("sleep $((RANDOM % 3 + 1)) && rev {seed} | tee {reversed}")
    return reversed


@command  # step 5
def sort_chars(seed: Seed):
    """Extract individual characters and sort them alphabetically."""
    sorted_chars = outputs(SortedChars)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && grep -o . {seed} | sort | tee " "{sorted_chars}"
    )
    return sorted_chars


@command  # step 6
def encode_rot13(upper: Upper):
    """Apply ROT13 substitution cipher to the uppercased text."""
    rot13 = outputs(Rot13)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && tr A-Za-z N-ZA-Mn-za-m < {upper} | tee " "{rot13}"
    )
    return rot13


@command  # step 7
def repeat_word(lower: Lower, n: int):
    """Repeat the lowercased word n times, one per line."""
    repeated = outputs(Repeated)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && for _ in $(seq {n}); do cat {lower}; "
        "done | tee {repeated}"
    )
    return repeated


@command  # step 8 — diamond merge
def merge_cases(upper: Upper, lower: Lower):
    """Paste uppercase and lowercase versions side by side (diamond convergence)."""
    merged = outputs(Merged)
    Shell("sleep $((RANDOM % 3 + 1)) && paste {upper} {lower} | tee {merged}")
    return merged


@command  # step 9
def unique_chars(sorted_chars: SortedChars):
    """Deduplicate the sorted character list to get the unique character set."""
    unique_chars = outputs(UniqueChars)
    Shell("sleep $((RANDOM % 3 + 1)) && uniq {sorted_chars} | tee {unique_chars}")
    return unique_chars


@command  # step 10
def combine_all(merged: Merged, rot13: Rot13, repeated: Repeated, reversed: Reversed):
    """Concatenate all four transform outputs into one blob (4-way fan-in)."""
    combined = outputs(Combined)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && cat {merged} {rot13} {repeated} "
        "{reversed} | tee {combined}"
    )
    return combined


@command  # step 11+12
def make_stats(combined: Combined, unique_chars: UniqueChars):
    """Compute byte count of combined blob and unique-character count (co-outputs)."""
    stats = outputs(Stats)
    audit = outputs(Audit)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && wc -c {combined} | tee {stats} && wc -l "
        "{unique_chars} | tee {audit}"
    )
    return stats, audit


@command  # step 13
def shout_rot(rot13: Rot13):
    """Re-uppercase the ROT13 output for maximum shouting energy."""
    upper_rot = outputs(UpperRot)
    Shell("sleep $((RANDOM % 3 + 1)) && tr a-z A-Z < {rot13} | tee {upper_rot}")
    return upper_rot


@command  # step 14
def sort_combined(combined: Combined):
    """Lexicographically sort all lines in the combined blob."""
    sorted_combined = outputs(SortedCombined)
    Shell("sleep $((RANDOM % 3 + 1)) && sort {combined} | tee {sorted_combined}")
    return sorted_combined


@command  # step 15
def count_lines(combined: Combined):
    """Count the total number of lines in the combined blob."""
    line_counts = outputs(LineCounts)
    Shell("sleep $((RANDOM % 3 + 1)) && wc -l < {combined} | tee {line_counts}")
    return line_counts


@command  # step 16
def final_mix(upper_rot: UpperRot, sorted_combined: SortedCombined):
    """Concatenate shouted ROT13 and sorted blob into the final mix."""
    final_mix = outputs(FinalMix)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && cat {upper_rot} {sorted_combined} | tee "
        "{final_mix}"
    )
    return final_mix


@command  # step 17
def grand_summary(stats: Stats, line_counts: LineCounts, final_mix: FinalMix):
    """Assemble stats, line counts, and final mix into the grand summary."""
    grand_summary = outputs(GrandSummary)
    Shell(
        "sleep $((RANDOM % 3 + 1)) && cat {stats} {line_counts} {final_mix} | "
        "tee {grand_summary}"
    )
    return grand_summary


# ── workflow ──────────────────────────────────────────────────────────


@workflow
def alchemy_pipeline(P: Pipeline, word: str, n: int = 3) -> None:
    """Build one necroalchemy pipeline for a given word."""
    P.seed = make_seed(word=word)
    P.upper = to_upper(P.seed)
    P.lower = to_lower(P.seed)
    P.reversed = reverse_it(P.seed)
    P.sorted_chars = sort_chars(P.seed)
    P.rot13 = encode_rot13(P.upper)
    P.repeated = repeat_word(P.lower, n=n)
    P.merged = merge_cases(P.upper, P.lower)
    P.unique_chars = unique_chars(P.sorted_chars)
    P.combined = combine_all(P.merged, P.rot13, P.repeated, P.reversed)
    P.stats, P.audit = make_stats(P.combined, P.unique_chars)
    P.upper_rot = shout_rot(P.rot13)
    P.sorted_combined = sort_combined(P.combined)
    P.line_counts = count_lines(P.combined)
    P.final_mix = final_mix(P.upper_rot, P.sorted_combined)
    P.summary = grand_summary(P.stats, P.line_counts, P.final_mix)


# ── run ───────────────────────────────────────────────────────────────────────

WORDS = ["necroflow", "snakemake", "python"]
OUTDIR = Path("/tmp/necroalchemy_out")

if __name__ == "__main__":
    P = Pipeline(DAG(OUTDIR))
    alchemy_pipeline(P, "hello", n=2)
    P.save("/tmp/necroalchemy_hello.txt")
    print("Pipeline render → /tmp/necroalchemy_hello.txt")

    dag = DAG(OUTDIR)
    for word in WORDS:
        P = Pipeline(dag)
        alchemy_pipeline(P, word, n=3)
        P.finish()
        dag.require(P.sinks())

    dag.save("/tmp/necroalchemy_dag.txt")
    print("DAG render      → /tmp/necroalchemy_dag.txt")

    dag.run(keep_going=True)

    # Pipeline labels remain local handles for canonical outputs.
    for node in dag.nodes:
        if "summary" in dag.labels_for(node) and node.path:
            print(f"summary → {node.path}")
