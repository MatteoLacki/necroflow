#!/usr/bin/env python3
"""Select the top-K most intense precursors from an mzML/MGF spectra file.

Always writes MGF, regardless of the input format: Sage treats mzML and MGF as
equivalent spectra input (see pipeline.py's RecalibrationSpectraSubset docstring), and
pyteomics has no mzML writer, so standardizing the subset step's output on the one format
it can write keeps this script small. Ranks by summed fragment-ion intensity rather than
a reported precursor intensity, since not every spectra exporter fills that field in.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyteomics import mgf

from _spectra_io import precursor_mz_charge, read_spectra, spectrum_title


def _to_mgf_entry(spectrum: dict, index: int) -> dict:
    mz, charge = precursor_mz_charge(spectrum)
    params = {"title": spectrum_title(spectrum, index)}
    if mz is not None:
        params["pepmass"] = mz
    if charge is not None:
        params["charge"] = charge
    return {
        "m/z array": spectrum["m/z array"],
        "intensity array": spectrum["intensity array"],
        "params": params,
    }


def select_top_precursors(spectra_path: Path, top_k: int, output_path: Path) -> None:
    spectra = [
        (float(s["intensity array"].sum()) if len(s["intensity array"]) else 0.0, i, s)
        for i, s in enumerate(read_spectra(spectra_path))
    ]
    spectra.sort(key=lambda item: item[0], reverse=True)
    selected = [_to_mgf_entry(s, i) for _, i, s in spectra[:top_k]]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mgf.write(selected, output=str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select the top-K most intense precursors, written as MGF."
    )
    parser.add_argument("spectra", type=Path, help="Input spectra file (.mzML or .mgf)")
    parser.add_argument("--top-k", type=int, required=True, dest="top_k")
    parser.add_argument("-o", "--output", type=Path, required=True)
    args = parser.parse_args()
    select_top_precursors(args.spectra, args.top_k, args.output)


if __name__ == "__main__":
    main()
