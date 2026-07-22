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

import sys
from pathlib import Path

try:
    _ip = get_ipython()  # type: ignore[name-defined]
    _ip.run_line_magic("load_ext", "autoreload")
    _ip.run_line_magic("autoreload", "2")
except NameError:
    pass

from necroflow import DAG, NodeType, Pipeline, command, output

# ── node types ────────────────────────────────────────────────────────────────


class Seed(NodeType):
    """The input word written to disk — starting point for all transforms."""

    filename = "seed.txt"


class Upper(NodeType):
    """The seed word converted to uppercase."""

    filename = "upper.txt"


class Lower(NodeType):
    """The seed word converted to lowercase."""

    filename = "lower.txt"


class Reversed(NodeType):
    """The seed word with characters in reverse order."""

    filename = "rev.txt"


class Rot13(NodeType):
    """ROT13 cipher applied to the uppercased seed."""

    filename = "rot13.txt"


class Repeated(NodeType):
    """The lowercased word repeated n times, one per line."""

    filename = "rep.txt"


class Merged(NodeType):
    """Uppercase and lowercase variants pasted side by side."""

    filename = "merged.txt"


class SortedChars(NodeType):
    """Individual characters of the seed, sorted alphabetically."""

    filename = "sorted.txt"


class UniqueChars(NodeType):
    """Deduplicated character set derived from the sorted character list."""

    filename = "unique.txt"


class Combined(NodeType):
    """All four transform outputs concatenated into one blob."""

    filename = "combined.txt"


class Stats(NodeType):
    """Byte count of the combined blob."""

    filename = "stats.txt"


class Audit(NodeType):
    """Unique-character count of the combined blob."""

    filename = "audit.txt"


class UpperRot(NodeType):
    """ROT13 output re-uppercased for maximum shouting energy."""

    filename = "upper_rot.txt"


class SortedCombined(NodeType):
    """Combined blob with lines sorted lexicographically."""

    filename = "sorted_combined.txt"


class LineCounts(NodeType):
    """Total line count of the combined blob."""

    filename = "lines.txt"


class FinalMix(NodeType):
    """Shouted ROT13 and sorted blob concatenated."""

    filename = "final_mix.txt"


class GrandSummary(NodeType):
    """Final assembly of stats, line counts, and the final mix."""

    filename = "summary.txt"


# ── rules (16 rules → 17 nodes) ───────────────────────────────────────────────
# Each command sleeps 1-3 s so the scheduler and parallelism are visible.

_S = "sleep $((RANDOM % 3 + 1)) && "


@command(_S + "echo {word} | tee {seed}")  # step 1 — materialise the seed word
def make_seed(word: str):
    """Write the input word to a file — the starting point for all transforms."""
    seed = output(Seed)
    return seed


@command(_S + "tr a-z A-Z < {seed} | tee {upper}")  # steps 2-4 — fan-out
def to_upper(seed: Seed):
    """Convert all characters to uppercase."""
    upper = output(Upper)
    return upper


@command(_S + "tr A-Z a-z < {seed} | tee {lower}")
def to_lower(seed: Seed):
    """Convert all characters to lowercase."""
    lower = output(Lower)
    return lower


@command(_S + "rev {seed} | tee {reversed}")
def reverse_it(seed: Seed):
    """Reverse the character order of the seed."""
    reversed = output(Reversed)
    return reversed


@command(_S + "grep -o . {seed} | sort | tee {sorted_chars}")  # step 5
def sort_chars(seed: Seed):
    """Extract individual characters and sort them alphabetically."""
    sorted_chars = output(SortedChars)
    return sorted_chars


@command(_S + "tr A-Za-z N-ZA-Mn-za-m < {upper} | tee {rot13}")  # step 6
def encode_rot13(upper: Upper):
    """Apply ROT13 substitution cipher to the uppercased text."""
    rot13 = output(Rot13)
    return rot13


@command(_S + "for _ in $(seq {n}); do cat {lower}; done | tee {repeated}")  # step 7
def repeat_word(lower: Lower, n: int):
    """Repeat the lowercased word n times, one per line."""
    repeated = output(Repeated)
    return repeated


@command(_S + "paste {upper} {lower} | tee {merged}")  # step 8 — diamond merge
def merge_cases(upper: Upper, lower: Lower):
    """Paste uppercase and lowercase versions side by side (diamond convergence)."""
    merged = output(Merged)
    return merged


@command(_S + "uniq {sorted_chars} | tee {unique_chars}")  # step 9
def unique_chars(sorted_chars: SortedChars):
    """Deduplicate the sorted character list to get the unique character set."""
    unique_chars = output(UniqueChars)
    return unique_chars


@command(_S + "cat {merged} {rot13} {repeated} {reversed} | tee {combined}")  # step 10
def combine_all(merged: Merged, rot13: Rot13, repeated: Repeated, reversed: Reversed):
    """Concatenate all four transform outputs into one blob (4-way fan-in)."""
    combined = output(Combined)
    return combined


@command(
    _S + "wc -c {combined} | tee {stats} && wc -l {unique_chars} | tee {audit}"
)  # step 11+12
def make_stats(combined: Combined, unique_chars: UniqueChars):
    """Compute byte count of combined blob and unique-character count (co-outputs)."""
    stats = output(Stats)
    audit = output(Audit)
    return stats, audit


@command(_S + "tr a-z A-Z < {rot13} | tee {upper_rot}")  # step 13
def shout_rot(rot13: Rot13):
    """Re-uppercase the ROT13 output for maximum shouting energy."""
    upper_rot = output(UpperRot)
    return upper_rot


@command(_S + "sort {combined} | tee {sorted_combined}")  # step 14
def sort_combined(combined: Combined):
    """Lexicographically sort all lines in the combined blob."""
    sorted_combined = output(SortedCombined)
    return sorted_combined


@command(_S + "wc -l < {combined} | tee {line_counts}")  # step 15
def count_lines(combined: Combined):
    """Count the total number of lines in the combined blob."""
    line_counts = output(LineCounts)
    return line_counts


@command(_S + "cat {upper_rot} {sorted_combined} | tee {final_mix}")  # step 16
def final_mix(upper_rot: UpperRot, sorted_combined: SortedCombined):
    """Concatenate shouted ROT13 and sorted blob into the final mix."""
    final_mix = output(FinalMix)
    return final_mix


@command(_S + "cat {stats} {line_counts} {final_mix} | tee {grand_summary}")  # step 17
def grand_summary(stats: Stats, line_counts: LineCounts, final_mix: FinalMix):
    """Assemble stats, line counts, and final mix into the grand summary."""
    grand_summary = output(GrandSummary)
    return grand_summary


# ── pipeline factory ──────────────────────────────────────────────────────────


def alchemy_pipeline(P: Pipeline, word: str, n: int = 3) -> None:
    """Build one necroalchemy pipeline for a given word."""
    P.seed = make_seed(P, word=word)
    P.upper = to_upper(P, P.seed)
    P.lower = to_lower(P, P.seed)
    P.reversed = reverse_it(P, P.seed)
    P.sorted_chars = sort_chars(P, P.seed)
    P.rot13 = encode_rot13(P, P.upper)
    P.repeated = repeat_word(P, P.lower, n=n)
    P.merged = merge_cases(P, P.upper, P.lower)
    P.unique_chars = unique_chars(P, P.sorted_chars)
    P.combined = combine_all(P, P.merged, P.rot13, P.repeated, P.reversed)
    P.stats, P.audit = make_stats(P, P.combined, P.unique_chars)
    P.upper_rot = shout_rot(P, P.rot13)
    P.sorted_combined = sort_combined(P, P.combined)
    P.line_counts = count_lines(P, P.combined)
    P.final_mix = final_mix(P, P.upper_rot, P.sorted_combined)
    P.summary = grand_summary(P, P.stats, P.line_counts, P.final_mix)


# ── run ───────────────────────────────────────────────────────────────────────

WORDS = ["necroflow", "snakemake", "python"]
OUTDIR = Path("/tmp/necroalchemy_out")

if __name__ == "__main__":
    P = Pipeline(OUTDIR)
    alchemy_pipeline(P, "hello", n=2)
    P.save("/tmp/necroalchemy_hello.txt")
    print("Pipeline render → /tmp/necroalchemy_hello.txt")

    dag = DAG(OUTDIR)
    for word in WORDS:
        P = Pipeline(OUTDIR)
        alchemy_pipeline(P, word, n=3)
        dag.add(P)

    dag.save("/tmp/necroalchemy_dag.txt")
    print("DAG render      → /tmp/necroalchemy_dag.txt")

    dag.execute(keep_going=True)

    # pipeline_label (the P.xxx attribute name) is the handle for each output
    for node in dag.nodes:
        if node.pipeline_label == "summary" and node.path:
            print(f"summary → {node.path}")
