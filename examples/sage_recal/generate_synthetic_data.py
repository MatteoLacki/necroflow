#!/usr/bin/env python3
"""Generate a synthetic, multi-spectrum MGF for the two-pass recalibration demo.

This is a one-off generator, not part of the pipeline's rule graph (pipeline.py never
imports or calls it) -- it produced the checked-in
jobs/q99536_synthetic_100/spectra.mgf and is kept only so that file's provenance is
reproducible. Re-run it with `python generate_synthetic_data.py` to regenerate that
exact file (deterministic seed; requires network access to Koina).

Why this exists: jobs/q99536_example/ (Sage's own real single-spectrum CI fixture) proves
the pipeline's plumbing/caching but can't demonstrate real recalibration -- one confident
PSM gives fit_recalibration_tolerance's residual-bound math nothing to derive a *width*
from, so the second pass's window collapses to zero and finds nothing (see README.md).
There is no small public multi-spectrum Sage test fixture to reach for instead (checked
lazear/sage's own repo tree at the pinned release tag -- just the one file).

So: digest the same bundled Q99536 protein in-silico (Sage's own cleavage rule: after
K/R, not before P; up to 2 missed cleavages), predict realistic fragment ion m/z and
relative intensities for each candidate peptide (charge 2+ and 3+) with a deep-learning
model served by Koina (https://koina.wilhelmlab.org), and inject a deterministic,
per-spectrum ppm bias on top of those predicted m/z values to simulate an uncalibrated
instrument -- exactly the shape of error pipeline.py's recalibrate_spectra rule is
meant to correct. Every resulting PSM is real and correct by construction (the
fragments are real Prosit-predicted ions for that exact peptide, just m/z-shifted).

The injected bias is sinusoidal in acquisition order around a nonzero mean
(PPM_MEAN=3, PPM_AMPLITUDE=16 -- see below), not a constant plus noise: both the
wave's peaks (+19ppm) and troughs (-13ppm) fall outside the production fragment_tol
(+-10ppm, see jobs/q99536_synthetic_100/job.toml's [sage.fragment_tol]), so spectra
near either extreme are genuinely unfindable by an uncorrected, production-tolerance
search -- checked directly: running the raw (uncorrected) full spectra file against
the plain production config finds fewer PSMs than the corrected, recalibrated second
pass does. That gap is the point: with a constant-plus-noise bias small enough to sit
inside the production window at every point (an earlier version of this generator,
PPM_MEAN=6/PPM_JITTER_SD=1.5 with no oscillation), recalibration could only ever *lose*
borderline hits by narrowing the search space with nothing to gain in return --
verified empirically, that earlier version's recalibrated pass found *fewer* PSMs than
just searching the raw data with the wide production tolerance. A bias that's
genuinely too large for the production window to already cover -- and a calibration
pass with its own separate, wide-enough tolerance to actually see that full range (see
pipeline.py's recalibrated_sage_search docstring) -- is what makes recalibration a net
win instead of a net loss on this demo.

Koina is a web-accessible inference server for proteomics ML models -- a plain HTTP
KServe v2 API, no client library needed (see _koina_predict_batch). Model used here:
Prosit_2020_intensity_HCD, a fragment-intensity model trained on synthetic peptide
libraries. Please cite both if you reuse this script:
  Lautenbacher et al. (2024) "Koina: Democratizing machine learning for proteomics
  research." Nature Communications. https://doi.org/10.1038/s41467-025-64870-5
  Gessulat et al. (2019) "Prosit: proteome-wide prediction of peptide tandem mass
  spectra by deep learning." Nature Methods 16, 509-518.
  https://doi.org/10.1038/s41592-019-0426-7
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import requests
from pyteomics import mass, mgf, parser

PROTON_MASS = 1.00727646688
STATIC_MODS = {"C": 57.0216}
SAGE_CLEAVAGE_RULE = "[KR](?!P)"  # matches sage_config's cleave_at="KR", restrict="P"

KOINA_URL = "https://koina.wilhelmlab.org/v2/models/Prosit_2020_intensity_HCD/infer"
COLLISION_ENERGY = 25.0
CHARGES = (2, 3)
# Prosit_2020_intensity_HCD's output is a fixed [n, 174] block = 29 cleavage
# positions x 3 fragment charges x {b, y} -- peptides longer than this aren't covered.
MAX_KOINA_PEPTIDE_LENGTH = 29
# Koina marks impossible fragments (ion index >= peptide length, or fragment charge >
# precursor charge) with an exact -1.0 sentinel on both mz and intensity (verified
# empirically). Genuinely negligible-but-real predictions are tiny positive floats, not
# -1 -- drop those too (relative to each spectrum's own max) so peak lists look like
# real spectra instead of ~150 near-zero floor artifacts.
MIN_RELATIVE_INTENSITY = 0.01

SEED = 20260715
# Sinusoidal, not constant-plus-noise: a per-spectrum ppm offset that oscillates with
# "acquisition order" (spectrum index) around a real, nonzero mean bias. Both
# PPM_MEAN - PPM_AMPLITUDE and PPM_MEAN + PPM_AMPLITUDE fall outside the production
# fragment_tol (+-10ppm, see jobs/q99536_synthetic_100/job.toml's [sage.fragment_tol]),
# so spectra near *both* the wave's peaks and its troughs are genuinely unfindable by
# an uncorrected, production-tolerance search (fragment matches fall outside +-10ppm,
# matched_peaks drops below Sage's min_matched_peaks=4), while spectra near its
# zero-crossings still are. PPM_MEAN != 0 specifically so the m/z-correction step (not
# just the tolerance-widening step) is doing real, necessary work -- with a zero-mean
# wave, the fitted offset would itself be ~0 and correction would be a near no-op, the
# demo would work through widening the tolerance alone. See
# recalibrated_sage_search's docstring in pipeline.py for why the calibration pass
# needs its own wide `calibration_tol`, decoupled from the narrower production
# tolerance, for the fitted correction to actually see this full range.
PPM_MEAN = 3.0
PPM_AMPLITUDE = 16.0
PPM_PERIOD = 13.0  # spectra per full sine cycle
PPM_JITTER_SD = 1.0


def _aa_mass() -> dict[str, float]:
    aa_mass = dict(mass.std_aa_mass)
    for residue, delta in STATIC_MODS.items():
        aa_mass[residue] += delta
    return aa_mass


def _candidate_peptides(fasta_path: Path) -> list[str]:
    lines = fasta_path.read_text().splitlines()
    sequence = "".join(line for line in lines if not line.startswith(">"))
    peptides = sorted(
        set(parser.cleave(sequence, SAGE_CLEAVAGE_RULE, missed_cleavages=2))
    )

    aa_mass = _aa_mass()
    candidates = []
    for pep in peptides:
        # >=9 residues -> >=16 b/y fragment peaks even before Koina's own intensity
        # filtering, comfortably over Sage's default min_peaks=15 spectrum-quality
        # filter. <=29 residues is Prosit_2020_intensity_HCD's coverage limit.
        if not (9 <= len(pep) <= MAX_KOINA_PEPTIDE_LENGTH):
            continue
        neutral_mass = mass.fast_mass(pep, charge=0, aa_mass=aa_mass)
        if not (500.0 <= neutral_mass <= 5000.0):
            continue
        candidates.append(pep)
    return candidates


def _koina_predict_batch(
    peptides: list[str], charges: list[int]
) -> list[tuple[list[float], list[float]]]:
    """One batched Koina call for all peptide/charge combinations. Returns, per
    combination, the (fragment_mzs, fragment_intensities) Prosit actually predicts as
    real (non-sentinel, non-negligible) ions."""
    n = len(peptides)
    payload = {
        "id": "0",
        "inputs": [
            {
                "name": "peptide_sequences",
                "shape": [n, 1],
                "datatype": "BYTES",
                "data": peptides,
            },
            {
                "name": "precursor_charges",
                "shape": [n, 1],
                "datatype": "INT32",
                "data": charges,
            },
            {
                "name": "collision_energies",
                "shape": [n, 1],
                "datatype": "FP32",
                "data": [COLLISION_ENERGY] * n,
            },
        ],
    }
    response = requests.post(KOINA_URL, json=payload, timeout=180)
    response.raise_for_status()
    outputs = {o["name"]: o["data"] for o in response.json()["outputs"]}
    per_sequence = len(outputs["mz"]) // n

    results = []
    for i in range(n):
        chunk = slice(i * per_sequence, (i + 1) * per_sequence)
        pairs = [
            (m, x)
            for m, x in zip(outputs["mz"][chunk], outputs["intensities"][chunk])
            if m != -1.0
        ]
        if not pairs:
            results.append(([], []))
            continue
        threshold = MIN_RELATIVE_INTENSITY * max(x for _, x in pairs)
        kept = [(m, x) for m, x in pairs if x >= threshold]
        mzs, intensities = zip(*kept) if kept else ((), ())
        results.append((list(mzs), list(intensities)))
    return results


def generate_spectra(fasta_path: Path, rng: np.random.Generator) -> list[dict]:
    aa_mass = _aa_mass()
    peptides = _candidate_peptides(fasta_path)
    combos = [(pep, charge) for pep in peptides for charge in CHARGES]
    predictions = _koina_predict_batch(
        [pep for pep, _ in combos], [c for _, c in combos]
    )

    spectra = []
    for i, ((pep, charge), (true_fragment_mzs, fragment_intensities)) in enumerate(
        zip(combos, predictions)
    ):
        # Sage's min_peaks=15 default: skip combos Koina predicted too few real ions for
        # (e.g. a short peptide at a charge its fragments barely support).
        if len(true_fragment_mzs) < 15:
            continue

        true_precursor_mz = mass.fast_mass(pep, charge=charge, aa_mass=aa_mass)

        # One offset per spectrum -- a single measurement's calibration error applies
        # coherently to its precursor and all its fragments. Sinusoidal in `i` (the
        # combo's position in acquisition order, assigned before the min_peaks filter
        # above so the phase pattern doesn't shift depending on which combos survive
        # it) plus small per-spectrum jitter.
        phase = 2.0 * np.pi * i / PPM_PERIOD
        ppm_offset = (
            PPM_MEAN + PPM_AMPLITUDE * np.sin(phase) + rng.normal(0.0, PPM_JITTER_SD)
        )
        correction = 1.0 + ppm_offset * 1e-6
        observed_precursor_mz = true_precursor_mz * correction
        observed_fragment_mzs = [mz * correction for mz in true_fragment_mzs]

        order = np.argsort(observed_fragment_mzs)
        mzs = np.asarray(observed_fragment_mzs)[order]
        # Koina's intensities are relative (0-1); scale to a realistic absolute range.
        intensities = np.asarray(fragment_intensities)[order] * 1e6

        spectra.append(
            {
                "m/z array": mzs,
                "intensity array": intensities,
                "params": {
                    "title": f"{pep}_z{charge}",
                    "pepmass": observed_precursor_mz,
                    "charge": f"{charge}+",
                },
            }
        )
    return spectra


def main() -> None:
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument(
        "--fasta",
        type=Path,
        default=Path(__file__).parent / "jobs" / "q99536_example" / "fasta.fasta",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path(__file__).parent / "jobs" / "q99536_synthetic_100" / "spectra.mgf",
    )
    args = arg_parser.parse_args()

    rng = np.random.default_rng(SEED)
    spectra = generate_spectra(args.fasta, rng)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    mgf.write(spectra, output=str(args.output))
    print(f"wrote {len(spectra)} synthetic spectra to {args.output}")


if __name__ == "__main__":
    main()
