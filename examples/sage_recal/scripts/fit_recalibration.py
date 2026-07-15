#!/usr/bin/env python3
"""Fit a ppm-error-vs-m/z correction from confident Sage PSMs and derive narrower,
zero-centered precursor_tol/fragment_tol bounds from the *residual* (post-correction)
error distribution.

Ported from necromerge2's searchops.recalibration (filter_top_psms + fit_correction +
the tolerance-bound derivation in recalibrate()). The tof2mz-array-correction step
itself doesn't carry over unchanged (there is no lookup array once spectra come from an
mzML/MGF exporter) -- but a real correction step turned out to still be required, just
applied to the spectra file directly: see recalibrate_spectra.py, chained after this
script in pipeline.py's recalibrate_spectra rule. Verified directly against real Sage
(v0.14.7): a precursor_tol/fragment_tol window that doesn't straddle zero finds *nothing
at all*, even when the true error clearly falls within the stated ppm bounds -- so an
uncorrected systematic bias (e.g. an off-center window like `[4, 9]` ppm) isn't just
suboptimal, it's non-functional. Both bounds here are therefore residual (bias-corrected)
values, output alongside the fitted `ppm_offset` recalibrate_spectra.py applies to the
actual spectra file -- unlike the original tool, where the *same* offset already got
applied to the shared tof2mz array before search, so its fragment_tol could stay raw.

Bounds come from the residual error's min/max, not a quantile -- a quantile cutoff has no
safety margin and was found (in the original tof2mz-based tool) to shrink Sage's candidate
search space too aggressively, losing more identifications than the tighter tolerance was
worth. See pipeline.py's recalibrate_spectra docstring.

`--q-column` defaults to `peptide_q` (matching the original searchops tool this was
ported from) but can be set to `spectrum_q` instead. Why both exist: Sage's peptide-level
FDR (`fdr.rs::picked_peptide`) is a separate "picked" target-decoy competition with its
own KDE-fit posterior-error-probability estimate, which needs a large, heterogeneous PSM
population to converge -- confirmed by reading Sage v0.14.7's source directly, it can
report peptide_q=1.0 for every single PSM (even objectively excellent ones) on a small
dataset. `spectrum_q` (`qvalue.rs::spectrum_q_value`) uses a much simpler, more robust
cumulative decoy/target count that stays well-behaved at any sample size. Large,
realistic multi-protein datasets should use the (stricter, more standard) `peptide_q`
default; jobs/q99536_synthetic_100/ uses `spectrum_q` because its single-protein source
caps the population at a size too small for the picked-peptide KDE to converge.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def filter_top_psms(sage_results_tsv: Path, fdr: float, q_column: str) -> pd.DataFrame:
    df = pd.read_csv(sage_results_tsv, sep="\t")
    return df[(df["rank"] == 1) & (df[q_column] <= fdr)]


def fit_recalibration_tolerance(
    sage_results_tsv: Path, fdr: float, q_column: str = "peptide_q"
) -> dict:
    df = filter_top_psms(sage_results_tsv, fdr, q_column)
    if df.empty:
        raise SystemExit(
            f"no rank-1 PSMs with {q_column} <= {fdr} in {sage_results_tsv}; "
            "cannot fit a recalibration -- widen --fdr, switch --q-column, or check the "
            "first-pass search"
        )
    precursor_ppm = df["precursor_ppm"].to_numpy()
    fragment_ppm = df["fragment_ppm"].to_numpy()

    # Single global offset, fit from precursor errors only, applied to both bounds --
    # matches the original tool's "one shared calibration corrects everything" model
    # (there it was a shared tof2mz array; here it's the one ppm_offset
    # recalibrate_spectra.py applies uniformly to every precursor and fragment m/z).
    offset = float(np.median(precursor_ppm))
    residual_precursor = precursor_ppm - offset
    residual_fragment = fragment_ppm - offset

    return {
        "ppm_offset": offset,
        "precursor_tol": {
            "ppm": [float(residual_precursor.min()), float(residual_precursor.max())]
        },
        "fragment_tol": {
            "ppm": [float(residual_fragment.min()), float(residual_fragment.max())]
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fit a ppm offset + derive precursor_tol/fragment_tol bounds from confident Sage PSMs."
    )
    parser.add_argument("results_tsv", type=Path, help="Sage results.sage.tsv")
    parser.add_argument("--fdr", type=float, required=True)
    parser.add_argument(
        "--q-column",
        default="peptide_q",
        choices=["peptide_q", "spectrum_q"],
        help="See module docstring for why spectrum_q exists as an alternative.",
    )
    parser.add_argument("-o", "--output", type=Path, required=True)
    args = parser.parse_args()

    tolerance = fit_recalibration_tolerance(args.results_tsv, args.fdr, args.q_column)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(tolerance, indent=2) + "\n")


if __name__ == "__main__":
    main()
