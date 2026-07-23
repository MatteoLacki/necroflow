"""Structural checks for the generic-Sage two-pass recalibration example.

Mirrors test_paper_example.py's approach: import the example module and assert DAG/typing
contracts, without requiring Docker, a real `sage` binary, or real spectra (see
examples/sage_recal/README.md for the Docker-based end-to-end run against real Sage).
"""

import importlib.util
from pathlib import Path

from necroflow import DAG, Pipeline

EXAMPLE = (
    Path(__file__).resolve().parents[1] / "examples" / "sage_recal" / "pipeline.py"
)


def _load_example():
    spec = importlib.util.spec_from_file_location("sage_recal_pipeline", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_no_tof_specific_types_remain():
    example = _load_example()
    names = {name for name in dir(example) if not name.startswith("_")}
    tof_specific = {"Tof2Mz", "Pmsms", "Precursors"}
    assert not (names & tof_specific)


def test_run_sage_calls_get_distinct_lineage_and_caching():
    example = _load_example()
    config = example.example_config()
    config["calibration_tol"] = {
        "precursor_tol": {"ppm": [-75, 75]},
        "fragment_tol": {"ppm": [-25, 25]},
    }
    pipeline = Pipeline(DAG("/results"))
    example.recalibrated_sage_search(pipeline, config)

    # Both run_sage calls produce the same four output types...
    for tsv_node in (pipeline.recal_tsv, pipeline.tsv):
        assert tsv_node.node_type is example.SageResultsTsv
    for json_node in (pipeline.recal_json, pipeline.json):
        assert json_node.node_type is example.SageResultsJson

    # ...but distinct lineage (subset+calibration-config vs. full+recalibrated-config)
    # gives each call its own fingerprint/cache directory -- the reuse property this
    # example exists to demonstrate.
    assert pipeline.recal_tsv.rule is pipeline.tsv.rule
    assert pipeline.recal_tsv.fingerprint != pipeline.tsv.fingerprint
    assert pipeline.recal_tsv.path != pipeline.tsv.path

    # The second call's inputs really are the recalibrated ones, not the originals --
    # regression coverage for a real bug: an earlier version searched the *raw*
    # spectra with a narrowed-but-uncorrected tolerance window, which returned zero
    # PSMs even when the true error fell within the stated bounds (verified against
    # real Sage: its candidate search requires a zero-centered tolerance window).
    assert pipeline.tsv.parents[0] is pipeline.recalibrated_spectra
    assert pipeline.tsv.parents[0] is not pipeline.spectra
    assert pipeline.tsv.parents[-1] is pipeline.recalibrated_config

    # The first call's config is the wide, exploratory calibration_sage_config, not the
    # narrower production sage_config -- regression coverage for a second real bug: an
    # earlier version reused sage_config for both passes, which censors the calibration
    # sample to the production tolerance's own (narrow) range, so the fitted correction
    # could never end up wider than what was already visible (see
    # recalibrated_sage_search's docstring).
    assert pipeline.recal_tsv.parents[-1] is pipeline.calibration_sage_config
    assert pipeline.recal_tsv.parents[-1] is not pipeline.sage_config


def test_second_pass_depends_transitively_on_first_pass():
    example = _load_example()
    pipeline = Pipeline(DAG("/results"))
    example.recalibrated_sage_search(pipeline, example.example_config())

    # recalibrated_config is built from the plain sage_config plus the tolerance fitted
    # from the first pass's own results -- this is what sequences pass 2 after pass 1.
    parents = pipeline.recalibrated_config.parents
    assert len(parents) == 2
    assert pipeline.sage_config in parents
    assert pipeline.tolerance in parents

    # tolerance and recalibrated_spectra are co-outputs of the same recalibrate_spectra
    # call (fitting the offset and applying it to the full spectra file happen
    # together -- see pipeline.py's recalibrate_spectra docstring for why), so they
    # share parents: the first pass's own results, and the full original spectra.
    assert pipeline.tolerance.parents == pipeline.recalibrated_spectra.parents
    assert pipeline.recal_tsv in pipeline.tolerance.parents
    assert pipeline.spectra in pipeline.tolerance.parents


def test_example_config_matches_bundled_fixture_paths():
    example = _load_example()
    config = example.example_config()
    fixture_dir = EXAMPLE.parent / "jobs" / "q99536_example"
    assert (fixture_dir / "fasta.fasta").exists()
    assert (fixture_dir / "spectra.mzml").exists()
    assert (fixture_dir / "job.toml").exists()


def test_spectra_dispatch_picks_the_matching_node_type_by_extension():
    # Regression test: MzMlSpectra and MgfSpectra are two concrete NodeTypes joined by
    # a union (SpectraFile = MzMlSpectra | MgfSpectra), not one shared type with a
    # runtime-chosen filename -- NodeType filenames are fixed per class, so an .mgf
    # input must never get symlinked under a fixed "spectra.mzml" name. Verified
    # against real Sage: a mismatched extension doesn't error, it silently parses as
    # zero spectra ("0 spectra/s", 0 PSMs, exit 0).
    example = _load_example()

    pipeline = Pipeline(DAG("/results"))
    mzml_node = example.raw_spectra(pipeline, "/data/run.mzML")
    assert mzml_node.node_type is example.MzMlSpectra
    assert issubclass(example.MzMlSpectra, example.SpectraFile)

    mgf_node = example.raw_spectra(pipeline, "/data/run.mgf")
    assert mgf_node.node_type is example.MgfSpectra
    assert issubclass(example.MgfSpectra, example.SpectraFile)

    assert mzml_node.node_type is not mgf_node.node_type

    import pytest

    with pytest.raises(ValueError):
        example.raw_spectra(pipeline, "/data/run.raw")


def test_spectra_file_is_a_union_not_a_shared_base_type():
    # If this ever regresses (e.g. someone replaces the union with a shared abstract
    # NodeType base that both MzMlSpectra and MgfSpectra inherit from), a NodeType
    # base's own `filename` -- or the lack of any real filename to fall back on --
    # would risk every .mgf input silently getting symlinked under a ".mzml" name
    # again, or vice versa -- see the test above.
    import types

    example = _load_example()
    assert isinstance(example.SpectraFile, types.UnionType)
    assert set(example.SpectraFile.__args__) == {
        example.MzMlSpectra,
        example.MgfSpectra,
    }
    assert example.MzMlSpectra.filename != example.MgfSpectra.filename


def test_synthetic_100_config_matches_bundled_fixture_paths():
    example = _load_example()
    config = example.synthetic_100_config()
    fixture_dir = EXAMPLE.parent / "jobs" / "q99536_synthetic_100"
    assert (fixture_dir / "spectra.mgf").exists()
    assert (fixture_dir / "job.toml").exists()
    # Real, correct number of in-silico spectra generate_synthetic_data.py produced
    # (Koina/Prosit-predicted fragments for tryptic Q99536 peptides) -- see
    # generate_synthetic_data.py's module docstring.
    spectra_count = (fixture_dir / "spectra.mgf").read_text().count("BEGIN IONS")
    assert spectra_count == 90
    assert config["recal_top_k"] < spectra_count
