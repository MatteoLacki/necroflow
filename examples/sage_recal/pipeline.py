"""Two-pass Sage recalibration example against generic (mzML/MGF) Sage.

Ported from necroflowpaper's Example S4 ("reusing one rule within a pipeline"), which
targeted necromerge2's timsTOF-specific Sage fork -- three intermediate objects (`pmsms`,
`tof2mz`, a `precursors` mmappet table) instead of a plain spectra file. Generic upstream
Sage (https://github.com/lazear/sage) reads mzML/MGF spectra directly, so this version
drops those three types in favor of one `SpectraFile` input. The reuse property the
original example demonstrated is unchanged: `run_sage` is called twice with different
lineage (subset spectra + original config, then full spectra + recalibrated config), so
each call gets its own cache address and only the second call reruns when recalibration
changes.

Factory signature follows necroflow's CLI contract (`factory(config: dict) -> Pipeline`,
see necroflow/src/necroflow/templates/canonical/pipeline.py) so this file runs unmodified
both via `import` (structural tests) and via `necroflow --pipeline pipeline.py:... --config
job.toml` (the Docker image, see ../README.md).
"""

from __future__ import annotations

import json

from necroflow import NodeType, Pipeline, Rules

r = Rules()


# --- node types ---
class MzMlSpectra(NodeType):
    filename = "spectra.mzml"


class MgfSpectra(NodeType):
    filename = "spectra.mgf"


# A union, not a shared abstract base with no filename: NodeType output filenames are
# fixed per class, and Sage dispatches input format by file extension (silently -- a
# mismatched extension parses as zero spectra with no error, verified against real
# Sage), so mzML and MGF need their own concrete types rather than one shared filename.
# necroflow's own convention for "either format is fine" is a union input (see
# docs/rules.md's "Use a union for alternatives" -- distinct from multiple inheritance,
# which is for "must satisfy both contracts"); every rule below that takes `spectra`
# accepts this union, and a subtype of either member (e.g. RecalibrationSpectraSubset,
# a subtype of MgfSpectra) is accepted through that member the same way it would be
# accepted directly.
SpectraFile = MzMlSpectra | MgfSpectra


class Fasta(NodeType):
    filename = "fasta.fasta"


class SageConfig(NodeType):
    filename = "sage_config.json"


class SageResultsJson(NodeType):
    filename = "results.json"


class SageResultsPin(NodeType):
    filename = "results.sage.pin"


class SageResultsTsv(NodeType):
    filename = "results.sage.tsv"


class SageMatchedFragments(NodeType):
    filename = "matched_fragments.sage.tsv"


class RecalibrationSpectraSubset(MgfSpectra):
    """A subset of MgfSpectra -- accepted wherever SpectraFile is, e.g. run_sage's
    `spectra` input for the calibration-only pass. Always MGF regardless of the parent
    spectra file's format (see scripts/select_top_precursors.py) -- Sage treats mzML and
    MGF as equivalent spectra input, and pyteomics can't round-trip mzML, so the subset
    step standardizes on the one format it can write."""

    filename = "recal_subset.mgf"


class RecalibratedSpectra(MgfSpectra):
    """The *full* spectra file, m/z-corrected by recalibrate_spectra.py's fitted ppm
    offset -- accepted wherever SpectraFile is, e.g. the second run_sage call's
    `spectra` input. Also always MGF (same reasoning as RecalibrationSpectraSubset)."""

    filename = "recalibrated_spectra.mgf"


class RecalibrationTolerance(NodeType):
    filename = "recalibration_tolerance.json"


# --- source rules ---
# Built-in symlink_file registrar (see docs/caching.md#external-dataset-ingestion),
# not hand-written `ln -s` rules: symlinking (not copying) an external path in means
# Path.stat() follows through to the real file, so necroflow's normal mtime/content-hash
# STALE machinery notices if you edit your own spectra.mzml/fasta.fasta in place and
# reruns everything downstream -- a bare `path=` string config value alone would not
# (necroflow only ever sees the path text, never the file's bytes, so an in-place edit
# would silently leave every downstream node marked UP_TO_DATE forever).
#
# Two rules, not one with a runtime-chosen filename -- NodeType output filenames are
# fixed per class (see SpectraFile above), so the extension has to be picked at the
# call site (see raw_spectra() below) rather than inside a single templated command.
r.symlink_file("raw_mzml", MzMlSpectra)
r.symlink_file("raw_mgf", MgfSpectra)
r.symlink_file("raw_fasta", Fasta)


def raw_spectra(path: str, rules=r):
    """Dispatch to raw_mzml/raw_mgf by the real file's extension -- see the comment
    above SpectraFile's definition for why a mismatched extension is a silent,
    zero-spectra Sage failure."""
    lower = str(path).lower()
    if lower.endswith(".mgf"):
        return rules.raw_mgf(path=path)
    if lower.endswith(".mzml") or lower.endswith(".mzml.gz"):
        return rules.raw_mzml(path=path)
    raise ValueError(
        f"unsupported spectra file extension: {path!r} (expected .mzml or .mgf)"
    )


r.text_file("write_sage_config", SageConfig)


# --- compute rules ---
@r.command(
    "python scripts/select_top_precursors.py {spectra} --top-k {top_k} -o {subset}"
)
def select_recalibration_spectra(spectra: SpectraFile, top_k: int):
    """Subset a spectra file to its top-K most intense precursors, for mass-error
    estimation on a fast first pass."""
    return RecalibrationSpectraSubset[subset]


@r.command(
    "sage {config} -f {fasta} --write-pin --annotate-matches -o {workdir} {spectra}"
    " && test -f {results_json} && test -f {results_pin}"
    " && test -f {results_tsv} && test -f {matched_fragments}"
)
def run_sage(spectra: SpectraFile, fasta: Fasta, config: SageConfig):
    return (
        SageResultsJson[results_json],
        SageResultsPin[results_pin],
        SageResultsTsv[results_tsv],
        SageMatchedFragments[matched_fragments],
    )


@r.command(
    "python scripts/fit_recalibration.py {results_tsv}"
    " --fdr {fdr} --q-column {q_column} -o {tolerance}"
    " && python scripts/recalibrate_spectra.py {spectra} {tolerance} -o {recalibrated_spectra}"
)
def recalibrate_spectra(
    results_tsv: SageResultsTsv,
    spectra: SpectraFile,
    fdr: float,
    q_column: str = "peptide_q",
):
    """Fit a ppm offset from confident first-pass PSMs, derive narrower
    precursor_tol/fragment_tol bounds from the *residual* (post-correction) error, and
    apply that same offset to the *full* spectra file's precursor and fragment m/z.

    Ported from necromerge2's searchops.recalibration (`recalibrate_mz`), which
    corrected the shared tof2mz lookup array and derived tolerance bounds from it --
    generic Sage has no such array, so this corrects the spectra file directly instead.
    That correction step turned out to still be required, not optional: verified
    directly against real Sage (v0.14.7), a precursor_tol/fragment_tol window that
    doesn't straddle zero returns *zero PSMs even when the true error clearly falls
    within the stated bounds* -- Sage's own candidate search appears to require a
    zero-centered window. An early version of this rule only narrowed the tolerance
    window (using the *raw*, uncorrected error) without correcting the spectra file --
    that's a bug, not an alternative design: it works only when the true error already
    straddles zero, which a real systematic calibration bias by definition does not.

    `q_column` defaults to Sage's peptide-level FDR (the standard, stricter choice for
    real datasets); see scripts/fit_recalibration.py's docstring for why a small
    dataset may need `spectrum_q` instead."""
    return RecalibrationTolerance[tolerance], RecalibratedSpectra[recalibrated_spectra]


@r.command(
    "python -m necroflow.tools.config_set"
    " {sage_config} {workdir}/precursor_tol_updated.json"
    " --target precursor_tol --source {tolerance} --source-field precursor_tol"
    " && python -m necroflow.tools.config_set"
    " {workdir}/precursor_tol_updated.json {recalibrated_sage_config}"
    " --target fragment_tol --source {tolerance} --source-field fragment_tol"
)
def update_sage_config(sage_config: SageConfig, tolerance: RecalibrationTolerance):
    return SageConfig[recalibrated_sage_config]


def recalibrated_sage_search(config: dict, rules=r) -> Pipeline:
    """Two-pass Sage search: the first pass searches a subset of the spectra to
    estimate mass error, then the second pass re-runs the full spectra file with
    recalibrated tolerances. `run_sage` is instantiated twice on distinct lineage.

    The first pass deliberately searches with its own *wide, exploratory*
    `calibration_tol` (config["calibration_tol"]), not the production
    `config["sage"]` tolerance the second pass starts from. This matters, not just
    stylistic: if the calibration pass used the same (narrow) production tolerance,
    it could only ever see PSMs already inside that window -- its confident-PSM sample
    would be censored to that range before recalibrate_spectra ever sees it, so the
    *derived* tolerance could never end up wider than what was already visible, and
    could never rescue a PSM the production tolerance was too narrow to find in the
    first place. Verified empirically: with both passes sharing one narrow tolerance
    against synthetic_100_config()'s data (whose injected ppm error swings well beyond
    the production window at points), the recalibrated second pass found *fewer* PSMs
    than a naive, uncorrected search with the wide production default -- recalibration
    made things worse, because the calibration sample never saw the full error range
    to correct for. A wide calibration-only tolerance fixes that."""
    P = Pipeline()
    P.spectra = raw_spectra(config["spectra_path"], rules)
    P.fasta = rules.raw_fasta(path=config["fasta_path"])
    P.sage_config = rules.write_sage_config(
        text=json.dumps(config["sage"], sort_keys=True, indent=2) + "\n"
    )
    P.calibration_sage_config = rules.write_sage_config(
        text=json.dumps(
            {**config["sage"], **config["calibration_tol"]}, sort_keys=True, indent=2
        )
        + "\n"
    )

    # First search: top-K subset, wide exploratory tolerance, for mass-error estimation.
    P.recal_subset = rules.select_recalibration_spectra(
        P.spectra, top_k=config["recal_top_k"]
    )
    P.recal_json, P.recal_pin, P.recal_tsv, P.recal_fragments = rules.run_sage(
        P.recal_subset, P.fasta, P.calibration_sage_config
    )

    # Recalibrate: fit tolerances AND correct the full spectra file's m/z from the
    # first-pass identifications (see recalibrate_spectra's docstring for why both).
    P.tolerance, P.recalibrated_spectra = rules.recalibrate_spectra(
        P.recal_tsv,
        P.spectra,
        fdr=config["fdr"],
        q_column=config.get("recal_q_column", "peptide_q"),
    )
    P.recalibrated_config = rules.update_sage_config(P.sage_config, P.tolerance)

    # Second search: full, m/z-corrected spectra file, recalibrated tolerances.
    P.json, P.pin, P.tsv, P.fragments = rules.run_sage(
        P.recalibrated_spectra, P.fasta, P.recalibrated_config
    )
    return P


def example_config(**overrides) -> dict:
    """Config matching lazear/sage's own CI fixture (tests/Q99536.fasta +
    tests/LQSRPAAPPAPGPGQLTLR.mzML) -- see jobs/q99536_example/. `fdr=1.0` because that
    fixture is a single spectrum: Sage's q-value estimation needs a target/decoy
    population to work with, so a real dataset should use a real fdr (e.g. 0.01).

    Verified end to end against real Sage (v0.14.7) and real necroflow execution: the
    first pass finds the fixture's one real PSM and both `run_sage` calls land in
    distinct, independently cached output directories -- the reuse property this example
    exists to demonstrate. But a single confident PSM gives `recalibrate_spectra`
    nothing to derive a *width* from (bounds collapse to a single point), so the second
    pass's recalibrated window is degenerate and finds zero PSMs. That's expected for
    this tiny fixture, not a bug: it's a plumbing/caching demo, not a realistic
    recalibration result -- see jobs/q99536_synthetic_100/ (synthetic_100_config()
    below) for a bundled example where the tolerance actually narrows to a real,
    searchable window."""
    values = {
        "spectra_path": "jobs/q99536_example/spectra.mzml",
        "fasta_path": "jobs/q99536_example/fasta.fasta",
        "recal_top_k": 1,
        "fdr": 1.0,
        # Same as sage's own tolerance -- this fixture's single real PSM (precursor_ppm
        # 0.82) is well within it either way, so there's no real error range to widen
        # into; see synthetic_100_config() for where calibration_tol actually matters.
        "calibration_tol": {
            "precursor_tol": {"ppm": [-50, 50]},
            "fragment_tol": {"ppm": [-10, 10]},
        },
        "sage": {
            "database": {
                "bucket_size": 16384,
                "fragment_min_mz": 150.0,
                "fragment_max_mz": 1500.0,
                "enzyme": {
                    "missed_cleavages": 1,
                    "cleave_at": "KR",
                    "restrict": "P",
                },
                "static_mods": {"C": 57.0216},
                "decoy_tag": "rev_",
                "generate_decoys": True,
                "fasta": "fasta.fasta",
            },
            "deisotope": True,
            "chimera": False,
            "max_fragment_charge": 1,
            "report_psms": 1,
            "precursor_tol": {"ppm": [-50, 50]},
            "fragment_tol": {"ppm": [-10, 10]},
            "isotope_errors": [-1, 3],
        },
    }
    values.update(overrides)
    return values


def synthetic_100_config(**overrides) -> dict:
    """Config for jobs/q99536_synthetic_100/ -- an in-silico, deterministic
    multi-spectrum dataset (see ../generate_synthetic_data.py) that, unlike
    example_config()'s single real spectrum, gives fit_recalibration_tolerance a real
    population to derive a non-degenerate window from. `fdr=0.05`/`recal_q_column`:
    see jobs/q99536_synthetic_100/job.toml's comments -- a single ~237-residue source
    protein caps the candidate-peptide population at a size too small for Sage's
    peptide-level "picked" FDR to converge, so this uses spectrum-level FDR instead.

    The injected ppm bias (see generate_synthetic_data.py) is sinusoidal in
    acquisition order with amplitude bigger than `sage.fragment_tol`'s +-10ppm, so a
    real chunk of spectra are genuinely unfindable by an uncorrected, production-
    tolerance search -- `calibration_tol` gives the first pass its own wide,
    exploratory window specifically so it can see that full range before
    recalibrate_spectra fits a correction from it (see recalibrated_sage_search's
    docstring for why reusing the narrow production tolerance for calibration doesn't
    work: it censors the calibration sample to the same narrow range, so the fitted
    correction can never be wider than what was already visible).

    Verified end to end against real Sage (v0.14.7): searching the raw, uncorrected
    full spectra file with the plain production tolerance (no recalibration at all)
    finds fewer PSMs than the corrected, recalibrated second pass -- i.e. recalibration
    is a net win on this dataset, not just non-degenerate. (An earlier version of this
    dataset used a small constant-plus-noise bias that stayed inside the production
    window at every point; recalibrating that one was verified to be a net *loss*
    versus not recalibrating at all -- narrowing a window that already covered
    everything can only exclude borderline hits, never gain any.)"""
    values = {
        "spectra_path": "jobs/q99536_synthetic_100/spectra.mgf",
        "fasta_path": "jobs/q99536_example/fasta.fasta",
        "recal_top_k": 40,
        "fdr": 0.05,
        "recal_q_column": "spectrum_q",
        # Wide exploratory window for the calibration pass only -- see this function's
        # docstring. sage.fragment_tol below (+-10ppm) is the narrower "production
        # default" a user unaware of the calibration drift would naively pick.
        "calibration_tol": {
            "precursor_tol": {"ppm": [-50, 50]},
            "fragment_tol": {"ppm": [-25, 25]},
        },
        "sage": {
            "database": {
                "bucket_size": 16384,
                "fragment_min_mz": 150.0,
                "fragment_max_mz": 1500.0,
                "enzyme": {
                    "missed_cleavages": 2,
                    "cleave_at": "KR",
                    "restrict": "P",
                },
                "static_mods": {"C": 57.0216},
                "decoy_tag": "rev_",
                "generate_decoys": True,
                "fasta": "fasta.fasta",
            },
            "deisotope": True,
            "chimera": False,
            "max_fragment_charge": 3,
            "report_psms": 1,
            "precursor_tol": {"ppm": [-50, 50]},
            "fragment_tol": {"ppm": [-10, 10]},
            "isotope_errors": [-1, 3],
        },
    }
    values.update(overrides)
    return values
