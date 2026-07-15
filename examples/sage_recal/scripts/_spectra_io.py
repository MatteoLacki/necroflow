"""Shared mzML/MGF read helpers for select_top_precursors.py and recalibrate_spectra.py.

Not a pipeline rule itself -- pipeline.py never calls this directly, both scripts that
do run as `python scripts/<name>.py`, which puts this file's directory on `sys.path`
automatically, so the plain `from _spectra_io import ...` import works with no packaging.
"""

from __future__ import annotations

from pathlib import Path

from pyteomics import mgf, mzml


def read_spectra(path: Path):
    if path.suffix.lower() == ".mzml":
        with mzml.read(str(path)) as reader:
            yield from reader
    else:
        with mgf.read(str(path)) as reader:
            yield from reader


def precursor_mz_charge(spectrum: dict) -> tuple[float | None, int | None]:
    """Returns (mz, charge). Charge as a plain int -- pyteomics.mgf.write formats a
    bare int charge as MGF's `"N+"` convention on its own (verified against real Sage:
    round-trips correctly), no need to hand-format a string."""
    precursors = spectrum.get("precursorList", {}).get("precursor")
    if precursors:
        ions = precursors[0].get("selectedIonList", {}).get("selectedIon", [{}])
        ion = ions[0] if ions else {}
        mz = ion.get("selected ion m/z")
        charge = ion.get("charge state")
        return (
            float(mz) if mz is not None else None,
            int(charge) if charge is not None else None,
        )
    params = spectrum.get("params", {})
    pepmass = params.get("pepmass")
    mz = pepmass[0] if isinstance(pepmass, (list, tuple)) else pepmass
    charge = params.get("charge")
    ch = int(charge[0]) if charge else None
    return (float(mz) if mz is not None else None, ch)


def spectrum_title(spectrum: dict, index: int) -> str:
    params = spectrum.get("params", {})
    if params.get("title"):
        return str(params["title"])
    spectrum_id = spectrum.get("id") or spectrum.get("spectrum title")
    return str(spectrum_id) if spectrum_id else f"spectrum_{index}"
