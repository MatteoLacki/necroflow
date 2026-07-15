# Two-pass Sage recalibration (generic Sage, no Docker/Rust/local Sage install needed)

Runnable counterpart to necroflowpaper's Example S4 ("reusing one rule within a
pipeline"), ported from a timsTOF-specific Sage fork to generic
[Sage](https://github.com/lazear/sage) reading mzML/MGF directly. `run_sage` is called
twice with different lineage -- a fast first pass on a subset of spectra estimates mass
error, then a second pass re-runs the full, m/z-corrected spectra file with recalibrated
tolerances -- so each call gets its own independently cached output directory
(`necroflow`'s reuse property). See `pipeline.py` for the rule/pipeline definitions.

## Prerequisites

Docker only. No local Sage, Rust, or Python environment needed -- everything runs inside
the image built from `Dockerfile` (a pinned, checksum-verified Sage release binary +
pyteomics/psims/pandas/numpy + `necroflow==0.0.3`).

```
docker run hello-world   # sanity check your Docker install works
```

## Quick start

```
docker compose build
./run.sh jobs/q99536_example
./run.sh jobs/q99536_synthetic_100
```

Both run end to end and write into their own `jobs/<name>/outputs/`. Look under
`outputs/run_sage/<hash>/` -- there are two directories, one per `run_sage` call, each
with its own `results.sage.tsv` etc. That's the reuse property made visible: same rule,
two calls, two independently cached results.

Two bundled jobs, because one real dataset can't show both things at once:

- **`jobs/q99536_example/`** -- Sage's own single-spectrum CI fixture
  (`tests/Q99536.fasta` + `tests/LQSRPAAPPAPGPGQLTLR.mzML` from `lazear/sage`, pinned to
  the same release tag as the Docker image): real, public, and guaranteed to produce one
  real PSM in the first pass. But one confident PSM gives the recalibration step nothing
  to derive a tolerance *width* from, so the second pass's window is degenerate and
  finds zero PSMs. That's expected here, not a bug -- this fixture demonstrates the
  caching/lineage plumbing, not a realistic recalibration outcome.
- **`jobs/q99536_synthetic_100/`** -- an in-silico, deterministic multi-spectrum dataset
  (see `generate_synthetic_data.py`) generated from the *same* Q99536 protein: its
  tryptic peptides, realistic Prosit-predicted fragment ions (via
  [Koina](https://koina.wilhelmlab.org)), with a deliberate synthetic calibration bias
  injected on top -- sinusoidal in acquisition order (mean +3ppm, amplitude 16ppm, so it
  swings from -13 to +19ppm), not just a constant offset, so both the wave's peaks and
  troughs fall outside `[sage.fragment_tol]`'s +-10ppm. This is the demo that actually
  answers "does recalibration help": **yes, measurably** -- searching the raw,
  uncorrected data with the plain production tolerance finds 41 PSMs; the recalibrated,
  m/z-corrected second pass finds 51 (verified end to end against real Sage v0.14.7,
  both locally and in Docker -- see `pipeline.py`'s `synthetic_100_config()` and
  `recalibrated_sage_search` docstrings). An earlier, simpler version of this dataset
  (constant bias, no oscillation, small enough to sit inside the default tolerance at
  every point) showed the *opposite*: recalibrating found *fewer* PSMs than not
  bothering, because narrowing a window that already covered everything can only lose
  borderline hits, never gain any -- that failure mode is exactly why the bias needed to
  be large enough to actually exceed the default tolerance, not just present.

## Reproduce on your own data

Create a new folder under `jobs/` containing three files:

```
jobs/<your_job_name>/
  fasta.fasta      # your protein database
  spectra.mzml      # or spectra.mgf -- either works identically
  job.toml            # see jobs/q99536_synthetic_100/job.toml for the schema
                       # (closer to real usage than q99536_example/'s single-spectrum one)
```

Then run:

```
./run.sh jobs/<your_job_name>
```

Outputs land in `jobs/<your_job_name>/outputs/`, owned by your user (not root) --
`run.sh` runs the container as your uid/gid, the same way
[midia_docker](https://github.com/midiaIDorg/midia_docker) does.

### `job.toml` schema

- `.pipeline` -- always `"pipeline.py:recalibrated_sage_search"`.
- `spectra_path`, `fasta_path` -- paths to your two input files, resolved relative to
  this directory (i.e. `examples/sage_recal/` inside the container).
- `recal_top_k` -- how many of the most intense precursors to search in the fast first
  (calibration) pass. For real datasets, a few hundred to a few thousand is typical --
  enough confident PSMs to fit a stable tolerance, far fewer than the full run.
- `fdr` -- FDR cutoff for which first-pass PSMs count as "confident" when fitting the
  recalibration. Use a real value (e.g. `0.01`) on real, multi-protein data.
- `recal_q_column` -- optional, defaults to Sage's peptide-level `peptide_q` (the
  standard, stricter choice). Only set this to `"spectrum_q"` if your dataset is small
  enough that Sage's peptide-level "picked" FDR can't converge (see
  `scripts/fit_recalibration.py`'s docstring for why -- this is what
  `q99536_synthetic_100/job.toml` does, and why).
- `[calibration_tol.precursor_tol]`/`[calibration_tol.fragment_tol]` -- a **wide**,
  exploratory tolerance used only for the first (calibration) pass -- deliberately
  separate from `[sage.precursor_tol]`/`[sage.fragment_tol]` below. See "How
  recalibration actually works here" for why reusing the narrow production tolerance
  for calibration silently caps how much recalibration can ever help.
- `[sage.*]` -- Sage's own JSON config fields, written straight through
  (`database.fasta` gets overridden by `-f`/the resolved `fasta_path` at run time, so its
  value in `job.toml` doesn't matter). See
  [Sage's config docs](https://sage-docs.vercel.app/docs/configuration) for the full
  schema.

## How recalibration actually works here

Two things had to be right for recalibration to be a genuine net win here, not just a
non-degenerate result -- both found by testing against real Sage, not by reasoning
about it in the abstract:

1. **`recalibrate_spectra` corrects the spectra file, not just the tolerance window.**
   It fits a single global ppm offset from the first pass's confident PSMs, derives
   *residual* (post-correction) precursor_tol/fragment_tol bounds from it, and applies
   that same offset to the **full** spectra file's precursor and fragment m/z --
   writing a corrected copy that the second `run_sage` call actually searches. Both the
   correction and the width matter: a precursor_tol/fragment_tol window that doesn't
   straddle zero returns **zero PSMs even when the true error clearly falls within the
   stated bounds** (verified directly -- Sage's own candidate search appears to require
   a zero-centered window), so narrowing the raw, uncorrected window only ever works by
   accident, when the true error already happens to straddle zero. This mirrors what
   the original tof2mz-based tool did (it corrected a shared lookup array before
   search); generic Sage has no such array, so this corrects the spectra file directly
   instead.
2. **The calibration (first) pass needs its own wide tolerance
   (`calibration_tol`), separate from the narrow production one
   (`[sage.precursor_tol]`/`[sage.fragment_tol]`).** If the first pass reused the
   narrow production tolerance, its confident-PSM sample would be *censored* to that
   same narrow range before `recalibrate_spectra` ever sees it -- the derived
   correction could then never end up wider than what the production tolerance already
   covered, so it could never rescue a PSM the production tolerance was too narrow to
   find in the first place. `jobs/q99536_synthetic_100/job.toml`'s injected error
   deliberately swings well beyond the production tolerance specifically to make this
   failure mode reproducible: with both passes sharing one narrow tolerance, the
   "recalibrated" second pass found *fewer* PSMs than a naive uncorrected search, not
   more -- recalibration made things worse. Giving the calibration pass its own wide,
   exploratory tolerance is what turns that into a net win (41 -> 51 PSMs; see above).

## Provenance / reproducibility

- Sage version: pinned in `Dockerfile` (`SAGE_VERSION`/`SAGE_SHA256` build args) to a
  specific GitHub release asset, checksum-verified at build time. Sage itself has no
  Zenodo DOI -- cite it via `10.1021/acs.jproteome.3c00486` (its `CITATION.cff`).
- necroflow version: currently built from **local source** (see `docker-compose.yml`'s
  `build.context: ../..` and the comment at the top of `Dockerfile`), not the PyPI
  release archived for this repo -- this example's `spectra: MzMlSpectra | MgfSpectra`
  union input needs a `resolve_command()` fix in `necroflow/src/necroflow/dag.py`
  (union-typed positional inputs weren't being substituted into `{name}` command
  placeholders, despite `docs/rules.md` documenting unions as supported) that isn't in
  a released version yet. Once a release including that fix ships, this should revert
  to `pip install necroflow==X.Y.Z` and the build context back to `.` (see
  `necroflowpaper/submission.mk`, `CITATION.cff` for how releases get archived).
- `jobs/q99536_example/`: `lazear/sage`'s own CI fixture, at the same pinned release tag
  as the Sage binary -- one version pin covers both.
- `jobs/q99536_synthetic_100/`: generated by `generate_synthetic_data.py` from the same
  bundled Q99536 protein, deterministically (fixed seed), using fragment ion predictions
  from a model served by [Koina](https://koina.wilhelmlab.org) -- a plain HTTP inference
  API (no client library needed). Please cite both if you touch that script:
  - Lautenbacher et al. (2024) "Koina: Democratizing machine learning for proteomics
    research." *Nature Communications*. <https://doi.org/10.1038/s41467-025-64870-5>
  - Gessulat et al. (2019) "Prosit: proteome-wide prediction of peptide tandem mass
    spectra by deep learning." *Nature Methods* 16, 509-518.
    <https://doi.org/10.1038/s41592-019-0426-7>
- The built image itself (not just its recipe) is archived separately for long-term
  reproducibility -- see `make sage-image-archive` in `necroflow/Makefile` -- since a
  Dockerfile alone still depends on the upstream GitHub release asset surviving at
  rebuild time, while the archived image tarball doesn't.
