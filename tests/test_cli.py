"""Tests for CLI internals: _create_link_outputs, manifest keys, main()."""
import shutil
import textwrap
import time
import tomlkit
import pytest
from pathlib import Path
from necroflow import NodeType, Inputs, Outputs, Rules, Pipeline
from necroflow.dag import resolve_paths
from necroflow.cli import _create_link_outputs, main
from necroflow.pipeline import _sinks


class Out(NodeType): filename = "out.txt"
class Log(NodeType): filename = "run.log"


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
    _create_link_outputs(results_dir=tmp_path, combos=combos)
    combo_dir = tmp_path / "run1"
    assert combo_dir.is_dir()  # dir still created
    assert not any(combo_dir.rglob("*.txt"))  # but no symlinks for missing output


def test_link_outputs_can_use_separate_nodes_and_results_dirs(tmp_path):
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"
    P, _ = _make_pipeline_with_outputs(nodes_dir)

    _create_link_outputs(results_dir, [("run1", P, _sinks(P))], nodes_dir=nodes_dir)

    links = [p for p in (results_dir / "run1").rglob("*") if p.is_symlink()]
    assert links
    assert all(p.resolve().is_file() for p in links)
    content = (results_dir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    assert all(not str(v).startswith("../") for v in doc["outputs"].values())


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
    class A(NodeType): filename = "a.txt"
    class B(NodeType): filename = "b.txt"
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


def _real_output(outdir: Path, filename: str) -> Path:
    matches = [p for p in outdir.rglob(filename) if not p.is_symlink()]
    assert len(matches) == 1
    return matches[0]


def test_main_invalidate_parent_reruns_parent_and_child(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    a_path = _real_output(outdir, "a.txt")
    b_path = _real_output(outdir, "b.txt")
    a_mtime = a_path.stat().st_mtime
    b_mtime = b_path.stat().st_mtime

    time.sleep(0.05)
    main(["--outdir", str(outdir), "--invalidate", "a", str(job)])

    assert a_path.stat().st_mtime > a_mtime
    assert b_path.stat().st_mtime > b_mtime


def test_main_invalidate_child_reruns_only_child(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    a_path = _real_output(outdir, "a.txt")
    b_path = _real_output(outdir, "b.txt")
    a_mtime = a_path.stat().st_mtime
    b_mtime = b_path.stat().st_mtime

    time.sleep(0.05)
    main(["--outdir", str(outdir), "--invalidate", "b", str(job)])

    assert a_path.stat().st_mtime == a_mtime
    assert b_path.stat().st_mtime > b_mtime


def test_main_invalidate_inactive_label_does_not_request_it(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n".requests" = ["a"]\n'
    )
    outdir = tmp_path / "out"

    main(["--outdir", str(outdir), "--invalidate", "b", str(job)])

    assert [p for p in outdir.rglob("a.txt") if not p.is_symlink()]
    assert not [p for p in outdir.rglob("b.txt") if not p.is_symlink()]


def test_main_invalidate_missing_label_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    with pytest.raises(SystemExit, match="invalidation labels not found"):
        main(["--outdir", str(tmp_path / "out"), "--invalidate", "missing", str(job)])


def test_main_reap_file_expands_invalidation_labels(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    reap = tmp_path / "reap.toml"
    reap.write_text('quick = ["b"]\n')
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    a_path = _real_output(outdir, "a.txt")
    b_path = _real_output(outdir, "b.txt")
    a_mtime = a_path.stat().st_mtime
    b_mtime = b_path.stat().st_mtime

    time.sleep(0.05)
    main(["--outdir", str(outdir), "--reap", "quick", "--reap-file", str(reap), str(job)])

    assert a_path.stat().st_mtime == a_mtime
    assert b_path.stat().st_mtime > b_mtime


def test_main_reap_missing_file_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    with pytest.raises(SystemExit, match="reap file not found"):
        main(["--outdir", str(tmp_path / "out"), "--reap", "quick", str(job)])


def test_main_reap_missing_group_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    reap = tmp_path / "reap.toml"
    reap.write_text('other = ["a"]\n')
    with pytest.raises(SystemExit, match="not found"):
        main(["--outdir", str(tmp_path / "out"), "--reap", "quick", "--reap-file", str(reap), str(job)])


def test_main_reap_invalid_group_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    reap = tmp_path / "reap.toml"
    reap.write_text('quick = "a"\n')
    with pytest.raises(SystemExit, match="list of strings"):
        main(["--outdir", str(tmp_path / "out"), "--reap", "quick", "--reap-file", str(reap), str(job)])


def test_main_validation_rejects_config_before_execution(tmp_path, factory_file):
    validator = tmp_path / "validator.py"
    validator.write_text(textwrap.dedent("""\
        def validate(cfg):
            if cfg["v"] != "ok":
                raise ValueError("v must be ok")
    """))
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "bad"\n')
    outdir = tmp_path / "out"

    with pytest.raises(SystemExit, match="v must be ok"):
        main(["--outdir", str(outdir), "--validation", f"{validator}:validate", str(job)])

    assert not list(outdir.rglob("a.txt"))


def test_main_validation_is_repeatable_and_ordered(tmp_path, factory_file):
    log = tmp_path / "validation.log"
    validator = tmp_path / "validator.py"
    validator.write_text(textwrap.dedent(f"""\
        from pathlib import Path
        LOG = Path({str(log)!r})
        def first(cfg):
            LOG.write_text(LOG.read_text() + "first\\n" if LOG.exists() else "first\\n")
        def second(cfg):
            LOG.write_text(LOG.read_text() + "second\\n")
    """))
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main([
        "--outdir", str(tmp_path / "out"),
        "--validation", f"{validator}:first",
        "--validation", f"{validator}:second",
        str(job),
    ])

    assert log.read_text() == "first\nsecond\n"


def test_main_validation_sees_expanded_metadata_stripped_config(tmp_path, factory_file):
    log = tmp_path / "seen.txt"
    validator = tmp_path / "validator.py"
    validator.write_text(textwrap.dedent(f"""\
        from pathlib import Path
        LOG = Path({str(log)!r})
        def validate(cfg):
            assert ".pipeline" not in cfg
            assert ".requests" not in cfg
            assert "v__grid" not in cfg
            LOG.write_text(LOG.read_text() + cfg["v"] + "\\n" if LOG.exists() else cfg["v"] + "\\n")
    """))
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\n".requests" = ["a"]\nv__grid = ["one", "two"]\n'
    )

    main(["--outdir", str(tmp_path / "out"), "--validation", f"{validator}:validate", str(job)])

    assert log.read_text().splitlines() == ["one", "two"]


def test_main_validation_bad_spec_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="validation spec must be"):
        main(["--outdir", str(tmp_path / "out"), "--validation", "validator.py", str(job)])


def test_main_validation_missing_function_errors(tmp_path, factory_file):
    validator = tmp_path / "validator.py"
    validator.write_text("def validate(cfg):\n    pass\n")
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="validation function 'missing' not found"):
        main(["--outdir", str(tmp_path / "out"), "--validation", f"{validator}:missing", str(job)])


def test_iter_job_configs_python_api_yields_expanded_configs_without_validation(tmp_path, factory_file):
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv__grid = ["good", "bad"]\n')

    jobs = list(iter_job_configs(job))

    assert [j.config["v"] for j in jobs] == ["good", "bad"]


def test_python_api_callers_validate_expanded_configs_in_their_own_loop(tmp_path, factory_file):
    from necroflow.config import iter_job_configs

    seen = []

    def validate(cfg):
        seen.append(cfg["v"])
        if cfg["v"] == "bad":
            raise ValueError("bad value")

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv__grid = ["good", "bad"]\n')

    with pytest.raises(ValueError, match="bad value"):
        for job_config in iter_job_configs(job):
            validate(job_config.config)

    assert seen == ["good", "bad"]


def test_main_runs_pipeline_with_default_nodes_and_results_dirs(tmp_path, factory_file, monkeypatch):
    """main() defaults hashed outputs to nodes/ and job links to results/."""
    monkeypatch.chdir(tmp_path)
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main([str(job)])

    assert any((tmp_path / "nodes").rglob("a.txt"))
    assert any((tmp_path / "nodes").rglob("b.txt"))
    assert (tmp_path / "results" / "job" / "manifest.toml").exists()
    assert not any((tmp_path / "results" / "job").rglob("a.txt"))
    assert any((tmp_path / "results" / "job").rglob("b.txt"))


def test_main_runs_pipeline_with_split_nodes_and_results_dirs(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes-root"
    results_dir = tmp_path / "results-root"

    main(["--nodes-dir", str(nodes_dir), "--results-dir", str(results_dir), str(job)])

    assert _real_output(nodes_dir, "a.txt").exists()
    real_b = _real_output(nodes_dir, "b.txt")
    assert not list(results_dir.rglob("*.txt")) or all(p.is_symlink() for p in results_dir.rglob("*.txt"))
    assert not list((results_dir / "job").rglob("a.txt"))
    b_links = list((results_dir / "job").rglob("b.txt"))
    assert len(b_links) == 1 and b_links[0].is_symlink() and b_links[0].resolve() == real_b


def test_main_outdir_keeps_single_root_compatibility(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"

    main(["--outdir", str(outdir), str(job)])

    assert any(outdir.rglob("a.txt"))
    assert (outdir / "job" / "manifest.toml").exists()


def test_main_outdir_cannot_be_combined_with_split_dirs(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="--outdir cannot be combined"):
        main(["--outdir", str(tmp_path / "out"), "--nodes-dir", str(tmp_path / "nodes"), str(job)])


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


# -- CLI subcommands and canonical template -----------------------------------

def test_init_creates_canonical_template(tmp_path):
    dest = tmp_path / "workflow"

    main(["init", str(dest)])

    assert (dest / "pipeline.py").exists()
    assert (dest / "job.toml").exists()
    assert (dest / "schema.py").exists()


def test_init_refuses_non_empty_directory_without_force(tmp_path):
    dest = tmp_path / "workflow"
    dest.mkdir()
    (dest / "existing.txt").write_text("keep")

    with pytest.raises(SystemExit, match="not empty"):
        main(["init", str(dest)])


def test_init_force_allows_existing_directory(tmp_path):
    dest = tmp_path / "workflow"
    dest.mkdir()
    (dest / "existing.txt").write_text("keep")

    main(["init", str(dest), "--force"])

    assert (dest / "pipeline.py").exists()
    assert (dest / "existing.txt").read_text() == "keep"


def test_canonical_template_runs(tmp_path, monkeypatch):
    dest = tmp_path / "workflow"
    main(["init", str(dest)])
    monkeypatch.chdir(dest)

    main([
        "--nodes-dir", "nodes",
        "--results-dir", "results",
        "--validation", "schema.py:validate",
        "job.toml",
    ])

    manifest = dest / "results" / "job" / "manifest.toml"
    assert manifest.exists()
    assert "summary" in manifest.read_text()


def test_graph_subcommand_prints_dag_without_outputs(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["graph", "--outdir", str(tmp_path / "out"), str(job)])

    captured = capsys.readouterr().out
    assert "make_a" in captured
    assert "make_b" in captured
    assert not list((tmp_path / "out").rglob("a.txt"))


def test_outputs_subcommand_lists_requested_paths_without_execution(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["outputs", "--nodes-dir", str(tmp_path / "nodes"), "--results-dir", str(tmp_path / "results"), str(job)])

    captured = capsys.readouterr().out
    assert "[job]" in captured
    assert "b\tnode=" in captured
    assert "result=" in captured
    assert not list((tmp_path / "nodes").rglob("a.txt"))


def test_provenance_subcommand_prints_metadata(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    main(["--nodes-dir", str(nodes_dir), "--results-dir", str(tmp_path / "results"), str(job)])
    output = _real_output(nodes_dir, "b.txt")

    main(["provenance", str(output)])

    captured = capsys.readouterr().out
    assert "rule = make_b" in captured
    assert "v = 'hello'" in captured


def test_outputs_shellpath_matches_run_shellpath_paths(tmp_path, factory_file, capsys):
    shell = shutil.which("sh") or "/bin/sh"
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"

    main(["outputs", "--shellpath", shell, "--nodes-dir", str(nodes_dir), "--results-dir", str(results_dir), str(job)])
    predicted = capsys.readouterr().out
    predicted_node = next(part.removeprefix("node=") for part in predicted.split() if part.startswith("node="))

    main(["--shellpath", shell, "--nodes-dir", str(nodes_dir), "--results-dir", str(results_dir), str(job)])

    assert Path(predicted_node).exists()


def test_provenance_prints_explicit_shellpath(tmp_path, factory_file, capsys):
    shell = str(Path(shutil.which("sh") or "/bin/sh").resolve())
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    main(["--shellpath", shell, "--nodes-dir", str(nodes_dir), "--results-dir", str(tmp_path / "results"), str(job)])
    output = _real_output(nodes_dir, "b.txt")

    main(["provenance", str(output)])

    captured = capsys.readouterr().out
    assert "[execution]" in captured
    assert f"shellpath = {shell!r}" in captured


def test_cli_invalid_shellpath_errors_cleanly(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="shellpath does not exist"):
        main(["--shellpath", str(tmp_path / "missing-shell"), str(job)])
