"""Tests for CLI internals: _create_link_outputs, manifest keys, main()."""
import textwrap
import tomlkit
import pytest
from pathlib import Path
from necroflow import NodeType, Inputs, Outputs, Rules, Pipeline
from necroflow.dag import resolve_paths
from necroflow.cli import _create_link_outputs, main
from necroflow.pipeline import _sinks


class Out(NodeType): name = "out.txt"
class Log(NodeType): name = "run.log"


R = Rules()
R.register("step1", Inputs(v=str),     Outputs(out=Out),      "echo {v} > {out}")
R.register("step2", Inputs(out=Out),   Outputs(log=Log),      "cat {out} > {log}")


def _make_pipeline_with_outputs(tmp_path) -> tuple[Pipeline, Path]:
    """Build a pipeline, resolve paths, and create real output files."""
    P = Pipeline()
    P.out = R.step1(v="hello")
    P.log = R.step2(P.out)
    resolve_paths(P.nodes, tmp_path)
    for node in P.nodes:
        node.path.parent.mkdir(parents=True, exist_ok=True)
        node.path.touch()
    return P, tmp_path


# ── symlink creation ──────────────────────────────────────────────────────────

def test_combo_dir_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    assert (outdir / "run1").is_dir()


def test_symlinks_created_for_existing_outputs(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    combo_dir = outdir / "run1"
    symlinks = list(combo_dir.rglob("*.txt")) + list(combo_dir.rglob("*.log"))
    assert len(symlinks) > 0
    assert all(f.is_symlink() for f in symlinks)


def test_symlinks_point_to_real_files(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    combo_dir = outdir / "run1"
    for link in combo_dir.rglob("*"):
        if link.is_symlink():
            assert link.resolve().exists()


def test_skips_missing_outputs(tmp_path):
    P = Pipeline()
    P.out = R.step1(v="hello")
    resolve_paths(P.nodes, tmp_path)
    # do NOT create the output file
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir=tmp_path, combos=combos)
    combo_dir = tmp_path / "run1"
    assert combo_dir.is_dir()  # dir still created
    assert not any(combo_dir.rglob("*.txt"))  # but no symlinks for missing output


# ── manifest ─────────────────────────────────────────────────────────────────

def test_manifest_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _sinks(P))]
    _create_link_outputs(outdir, combos)
    assert (outdir / "run1" / "manifest.toml").exists()


def test_manifest_keys_are_pipeline_labels(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _sinks(P)
    combos = [("run1", P, sinks)]
    _create_link_outputs(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # sink has pipeline_label "log" (the last node P.log = R.step2(...))
    assert "log" in keys


def test_manifest_only_sinks(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _sinks(P)
    combos = [("run1", P, sinks)]
    _create_link_outputs(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # "out" is intermediate (P.out), "log" is the sink (P.log)
    assert "out" not in keys
    assert "log" in keys


# ── main() integration ───────────────────────────────────────────────────────

FACTORY_SRC = textwrap.dedent("""\
    from necroflow import Pipeline, NodeType, Inputs, Outputs, Rules
    class A(NodeType): name = "a.txt"
    class B(NodeType): name = "b.txt"
    R = Rules()
    R.register("make_a", Inputs(v=str), Outputs(a=A), "touch {a}")
    R.register("make_b", Inputs(a=A),  Outputs(b=B), "touch {b}")
    def factory(cfg):
        P = Pipeline()
        P.a = R.make_a(v=cfg["v"])
        P.b = R.make_b(P.a)
        return P
""")


@pytest.fixture
def factory_file(tmp_path):
    f = tmp_path / "pipe.py"
    f.write_text(FACTORY_SRC)
    return f


@pytest.fixture
def job_toml(tmp_path, factory_file):
    t = tmp_path / "job.toml"
    t.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    return t


def test_main_runs_pipeline(tmp_path, factory_file):
    """main() executes a job TOML and produces output files."""
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    # both nodes should have been run (rglob includes symlinks; just check presence)
    assert any(outdir.rglob("a.txt"))
    assert any(outdir.rglob("b.txt"))


def test_main_request_limits_execution(tmp_path, factory_file):
    """.requests = ['a'] should only run the requested node and its ancestors."""
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n".requests" = ["a"]\n'
    )
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    assert list(outdir.rglob("a.txt"))
    assert not list(outdir.rglob("b.txt"))


def test_main_dry_run_no_outputs(tmp_path, factory_file):
    """--dry-run must not create any output files."""
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), "--dry-run", str(job)])
    assert not list(outdir.rglob("a.txt"))


def test_main_grid_expansion(tmp_path, factory_file):
    """__grid in job TOML expands into multiple pipeline runs."""
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv__grid = ["hello", "world"]\n'
    )
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    # two distinct hash dirs (different v → different hash) — ignore symlink copies
    a_real = [p for p in outdir.rglob("a.txt") if not p.is_symlink()]
    assert len(a_real) == 2


def test_main_missing_pipeline_key_errors(tmp_path):
    """A job TOML without a '.pipeline' key must raise SystemExit."""
    job = tmp_path / "job.toml"
    job.write_text('v = "hello"\n')
    with pytest.raises(SystemExit):
        main(["--outdir", str(tmp_path / "out"), str(job)])


def test_main_bad_request_label_errors(tmp_path, factory_file):
    """A .requests label that doesn't match any pipeline_label must raise SystemExit."""
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n".requests" = ["nonexistent"]\n'
    )
    with pytest.raises(SystemExit):
        main(["--outdir", str(tmp_path / "out"), str(job)])


def test_narrow_request_combo_excludes_prior_outputs(tmp_path, factory_file):
    """Combo dir must not link b.txt when it exists from a prior run but isn't requested.

    Guards the regression where _create_link_outputs iterated all pipeline nodes
    instead of only requested nodes, leaking outputs from earlier broader runs.
    """
    outdir = tmp_path / "out"

    # full run — produces both a.txt and b.txt in the hash tree
    job_full = tmp_path / "job_full.toml"
    job_full.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    main(["--outdir", str(outdir), str(job_full)])
    assert any(p for p in outdir.rglob("b.txt") if not p.is_symlink())

    # narrow run — only request a; b.txt still exists in hash tree
    job_narrow = tmp_path / "job_narrow.toml"
    job_narrow.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n".requests" = ["a"]\n'
    )
    main(["--outdir", str(outdir), str(job_narrow)])

    combo_dir = outdir / "job_narrow"
    assert any(combo_dir.rglob("a.txt"))       # requested output symlinked
    assert not any(combo_dir.rglob("b.txt"))   # unrequested output excluded


# ── multiple combos ───────────────────────────────────────────────────────────

def test_multiple_combos(tmp_path):
    P1 = Pipeline()
    P1.out = R.step1(v="alpha")
    resolve_paths(P1.nodes, tmp_path)
    for n in P1.nodes:
        n.path.parent.mkdir(parents=True, exist_ok=True)
        n.path.touch()

    P2 = Pipeline()
    P2.out = R.step1(v="beta")
    resolve_paths(P2.nodes, tmp_path)
    for n in P2.nodes:
        n.path.parent.mkdir(parents=True, exist_ok=True)
        n.path.touch()

    combos = [("combo_alpha", P1, _sinks(P1)), ("combo_beta", P2, _sinks(P2))]
    _create_link_outputs(tmp_path, combos)
    assert (tmp_path / "combo_alpha").is_dir()
    assert (tmp_path / "combo_beta").is_dir()
