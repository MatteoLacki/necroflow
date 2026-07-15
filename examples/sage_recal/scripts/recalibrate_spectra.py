#!/usr/bin/env python3
"""Correct a spectra file's precursor and fragment m/z by a fitted ppm offset.

Companion to fit_recalibration.py (chained after it in pipeline.py's
recalibrate_spectra rule -- see that rule's docstring for why both steps exist and why
this one is required, not optional). Reads the `ppm_offset` fit_recalibration.py wrote
into its tolerance JSON and divides every precursor and fragment m/z in the full spectra
file by `(1 + ppm_offset / 1e6)` -- same sign convention as necromerge2's
searchops.recalibration (`recalibration.py`'s module docstring: Sage's own
`precursor_ppm` is positive when the *observed* mass is heavier than theoretical, so
correcting means dividing, not adding). Always writes MGF (see select_top_precursors.py
for why); Sage treats mzML and MGF as equivalent input either way.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyteomics import mgf

from _spectra_io import precursor_mz_charge, read_spectra, spectrum_title


def recalibrate_spectra(
    spectra_path: Path, tolerance_path: Path, output_path: Path
) -> None:
    offset_ppm = json.loads(tolerance_path.read_text())["ppm_offset"]
    correction = 1.0 + offset_ppm * 1e-6

    corrected = []
    for i, spectrum in enumerate(read_spectra(spectra_path)):
        mz, charge = precursor_mz_charge(spectrum)
        params = {"title": spectrum_title(spectrum, i)}
        if mz is not None:
            params["pepmass"] = mz / correction
        if charge is not None:
            params["charge"] = charge
        corrected.append(
            {
                "m/z array": spectrum["m/z array"] / correction,
                "intensity array": spectrum["intensity array"],
                "params": params,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    mgf.write(corrected, output=str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Correct a spectra file's m/z by fit_recalibration.py's fitted offset."
    )
    parser.add_argument("spectra", type=Path, help="Input spectra file (.mzML or .mgf)")
    parser.add_argument(
        "tolerance", type=Path, help="fit_recalibration.py's output JSON"
    )
    parser.add_argument("-o", "--output", type=Path, required=True)
    args = parser.parse_args()
    recalibrate_spectra(args.spectra, args.tolerance, args.output)


if __name__ == "__main__":
    main()
