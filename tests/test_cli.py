"""Tests for CLI internals: _create_link_outputs, manifest keys, main()."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule

import shutil
import textwrap
import time
import tomlkit
import pytest

from necroflow._compat import ExceptionGroup
from pathlib import Path
from necroflow import NodeType, Pipeline, DAG, output
from necroflow.cli import (
    _create_link_outputs,
    _graph_payload,
    _resolve_request,
    main,
)


class Out(NodeType):
    filename = "out.txt"


class Log(NodeType):
    filename = "run.log"


R_step1 = Rule("step1", Inputs(v=str), Outputs(out=Out), "echo {v} > {out}")
R_step2 = Rule("step2", Inputs(out=Out), Outputs(log=Log), "cat {out} > {log}")


def _make_pipeline_with_outputs(tmp_path) -> tuple[Pipeline, Path]:
    """Build a pipeline and create real output files."""
    P = Pipeline(DAG(tmp_path))
    P.out = R_step1(P, v="hello")
    P.log = R_step2(P, P.out)
    for node in P.nodes:
        node.path.parent.mkdir(parents=True, exist_ok=True)
        node.path.touch()
    return P, tmp_path


# ── symlink creation ──────────────────────────────────────────────────────────


def test_combo_dir_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _create_link_outputs(outdir, combos)
    assert (outdir / "run1").is_dir()


def test_symlinks_created_for_existing_outputs(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _create_link_outputs(outdir, combos)
    combo_dir = outdir / "run1"
    symlinks = list(combo_dir.rglob("*.txt")) + list(combo_dir.rglob("*.log"))
    assert len(symlinks) > 0
    assert all(f.is_symlink() for f in symlinks)


def test_symlink_path_uses_requested_label_and_filename(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]

    _create_link_outputs(outdir, combos)

    link = outdir / "run1" / "log" / "run.log"
    assert link.is_symlink()
    assert link.resolve() == P.log.path


def test_symlinks_point_to_real_files(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _create_link_outputs(outdir, combos)
    combo_dir = outdir / "run1"
    for link in combo_dir.rglob("*"):
        if link.is_symlink():
            assert link.resolve().exists()


def test_skips_missing_outputs(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.out = R_step1(P, v="hello")
    # do NOT create the output file
    combos = [("run1", P, _resolve_request(P, None))]
    _create_link_outputs(results_dir=tmp_path, combos=combos)
    combo_dir = tmp_path / "run1"
    assert combo_dir.is_dir()  # dir still created
    assert not any(combo_dir.rglob("*.txt"))  # but no symlinks for missing output


def test_link_outputs_can_use_separate_nodes_and_results_dirs(tmp_path):
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"
    P, _ = _make_pipeline_with_outputs(nodes_dir)

    _create_link_outputs(
        results_dir, [("run1", P, _resolve_request(P, None))], nodes_dir=nodes_dir
    )

    links = [p for p in (results_dir / "run1").rglob("*") if p.is_symlink()]
    assert links
    assert all(p.resolve().is_file() for p in links)
    assert (results_dir / "run1" / "log" / "run.log").resolve() == P.log.path
    content = (results_dir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    assert all(not str(v).startswith("../") for v in doc["outputs"].values())


def test_link_outputs_removes_stale_generated_symlinks(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combo_dir = outdir / "run1"
    stale = combo_dir / "step2" / "abc123" / "run.log"
    stale.parent.mkdir(parents=True)
    stale.symlink_to(P.log.path)
    (combo_dir / "manifest.toml").write_text(
        '[outputs]\nlog = "step2/abc123/run.log"\n'
    )

    _create_link_outputs(outdir, [("run1", P, _resolve_request(P, None))])

    assert not stale.exists()
    assert not stale.parent.exists()
    assert (combo_dir / "log" / "run.log").is_symlink()


# ── manifest ─────────────────────────────────────────────────────────────────


def test_manifest_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _create_link_outputs(outdir, combos)
    assert (outdir / "run1" / "manifest.toml").exists()


def test_manifest_keys_are_requested_labels(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _resolve_request(P, None)
    combos = [("run1", P, sinks)]
    _create_link_outputs(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # The sink has the local label "log" (the last assignment in this Pipeline).
    assert "log" in keys


def test_aliases_of_one_sink_create_distinct_requested_results(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.primary = R_step1(P, v="hello")
    P.alias = P.primary
    P.primary.path.parent.mkdir(parents=True, exist_ok=True)
    P.primary.path.touch()

    request = _resolve_request(P, None)
    _create_link_outputs(tmp_path, [("run1", P, request)])

    assert P.primary is P.alias
    assert (tmp_path / "run1" / "primary" / "out.txt").is_symlink()
    assert (tmp_path / "run1" / "alias" / "out.txt").is_symlink()
    manifest = tomlkit.parse((tmp_path / "run1" / "manifest.toml").read_text())
    assert set(manifest["outputs"]) == {"primary", "alias"}


def test_non_identifier_label_is_quoted_in_manifest(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P["primary result"] = R_step1(P, v="hello")
    P["primary result"].path.parent.mkdir(parents=True, exist_ok=True)
    P["primary result"].path.touch()

    _create_link_outputs(tmp_path, [("run1", P, _resolve_request(P, None))])

    manifest = tomlkit.parse((tmp_path / "run1" / "manifest.toml").read_text())
    assert manifest["outputs"]["primary result"] == "primary result/out.txt"


def test_manifest_only_sinks(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _resolve_request(P, None)
    combos = [("run1", P, sinks)]
    _create_link_outputs(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # "out" is intermediate (P.out), "log" is the sink (P.log)
    assert "out" not in keys
    assert "log" in keys


def test_manifest_values_are_visible_result_paths(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]

    _create_link_outputs(outdir, combos)

    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    assert doc["outputs"]["log"] == "log/run.log"


# ── main() integration ───────────────────────────────────────────────────────

FACTORY_SRC = textwrap.dedent("""\
    from necroflow import Pipeline, NodeType, command, output
    class A(NodeType): filename = "a.txt"
    class B(NodeType): filename = "b.txt"
    @command("touch {a}")
    def make_a(v: str):
        a = output(A)
        return a
    @command("touch {b}")
    def make_b(a: A):
        b = output(B)
        return b
    def factory(P, cfg):
        P.a = make_a(P, v=cfg["v"])
        P.b = make_b(P, P.a)
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


def test_callable_fingerprint_example_runs_and_records_provenance(
    tmp_path, monkeypatch
):
    example_dir = (
        Path(__file__).resolve().parents[1] / "examples" / "callable_fingerprint"
    )
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"
    monkeypatch.chdir(example_dir)

    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            "job.toml",
        ]
    )

    result = results_dir / "job" / "sorted" / "sorted.txt"
    assert result.read_text() == "pear\nbanana\napple\n"
    metadata_paths = list((nodes_dir / "sort_text").glob("*/.rip/dependencies.toml"))
    assert len(metadata_paths) == 1
    metadata = tomlkit.parse(metadata_paths[0].read_text())
    assert metadata["fingerprint"]["provider"] == "fingerprint.py:project_fingerprint"
    assert metadata["command"]["kind"] == "python"
    assert "sort -r -u" in metadata["command"]["realized"]


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
    main(
        ["--outdir", str(outdir), "--reap", "quick", "--reap-file", str(reap), str(job)]
    )

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
        main(
            [
                "--outdir",
                str(tmp_path / "out"),
                "--reap",
                "quick",
                "--reap-file",
                str(reap),
                str(job),
            ]
        )


def test_main_reap_invalid_group_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    reap = tmp_path / "reap.toml"
    reap.write_text('quick = "a"\n')
    with pytest.raises(SystemExit, match="list of strings"):
        main(
            [
                "--outdir",
                str(tmp_path / "out"),
                "--reap",
                "quick",
                "--reap-file",
                str(reap),
                str(job),
            ]
        )


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
        main(
            ["--outdir", str(outdir), "--validation", f"{validator}:validate", str(job)]
        )

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

    main(
        [
            "--outdir",
            str(tmp_path / "out"),
            "--validation",
            f"{validator}:first",
            "--validation",
            f"{validator}:second",
            str(job),
        ]
    )

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

    main(
        [
            "--outdir",
            str(tmp_path / "out"),
            "--validation",
            f"{validator}:validate",
            str(job),
        ]
    )

    assert log.read_text().splitlines() == ["one", "two"]


def test_main_validation_bad_spec_errors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="validation spec must be"):
        main(
            [
                "--outdir",
                str(tmp_path / "out"),
                "--validation",
                "validator.py",
                str(job),
            ]
        )


def test_main_validation_missing_function_errors(tmp_path, factory_file):
    validator = tmp_path / "validator.py"
    validator.write_text("def validate(cfg):\n    pass\n")
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="validation function 'missing' not found"):
        main(
            [
                "--outdir",
                str(tmp_path / "out"),
                "--validation",
                f"{validator}:missing",
                str(job),
            ]
        )


def test_iter_job_configs_python_api_yields_expanded_configs_without_validation(
    tmp_path, factory_file
):
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv__grid = ["good", "bad"]\n'
    )

    jobs = list(iter_job_configs(job))

    assert [j.config["v"] for j in jobs] == ["good", "bad"]


def test_python_api_callers_validate_expanded_configs_in_their_own_loop(
    tmp_path, factory_file
):
    from necroflow.config import iter_job_configs

    seen = []

    def validate(cfg):
        seen.append(cfg["v"])
        if cfg["v"] == "bad":
            raise ValueError("bad value")

    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv__grid = ["good", "bad"]\n'
    )

    with pytest.raises(ValueError, match="bad value"):
        for job_config in iter_job_configs(job):
            validate(job_config.config)

    assert seen == ["good", "bad"]


def test_main_runs_pipeline_with_default_nodes_and_results_dirs(
    tmp_path, factory_file, monkeypatch
):
    """main() defaults hashed outputs to nodes/ and job links to results/."""
    monkeypatch.chdir(tmp_path)
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main([str(job)])

    assert any((tmp_path / "nodes").rglob("a.txt"))
    assert any((tmp_path / "nodes").rglob("b.txt"))
    assert (tmp_path / "results" / "job" / "manifest.toml").exists()
    assert not any((tmp_path / "results" / "job").rglob("a.txt"))
    assert (tmp_path / "results" / "job" / "b" / "b.txt").is_symlink()


def test_main_accepts_fifo_scheduler(tmp_path, job_toml):
    outdir = tmp_path / "out"

    main(["--outdir", str(outdir), "--scheduler", "fifo", str(job_toml)])

    assert _real_output(outdir, "b.txt").exists()


def test_main_loads_custom_scheduler(tmp_path, job_toml):
    scheduler = tmp_path / "schedulers.py"
    scheduler.write_text(
        "def choose(ready, remaining, available_resources):\n"
        '    assert available_resources["threads"] >= 0\n'
        "    return ready\n"
    )
    outdir = tmp_path / "out"

    main(
        [
            "--outdir",
            str(outdir),
            "--scheduler",
            f"{scheduler}:choose",
            str(job_toml),
        ]
    )

    assert _real_output(outdir, "b.txt").exists()


def test_main_rejects_unknown_scheduler(tmp_path, job_toml):
    with pytest.raises(SystemExit, match="--scheduler must be"):
        main(
            [
                "--outdir",
                str(tmp_path / "out"),
                "--scheduler",
                "unknown",
                str(job_toml),
            ]
        )


def test_main_runs_pipeline_with_split_nodes_and_results_dirs(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes-root"
    results_dir = tmp_path / "results-root"

    main(["--nodes-dir", str(nodes_dir), "--results-dir", str(results_dir), str(job)])

    assert _real_output(nodes_dir, "a.txt").exists()
    real_b = _real_output(nodes_dir, "b.txt")
    assert not list(results_dir.rglob("*.txt")) or all(
        p.is_symlink() for p in results_dir.rglob("*.txt")
    )
    assert not list((results_dir / "job").rglob("a.txt"))
    b_link = results_dir / "job" / "b" / "b.txt"
    assert b_link.is_symlink()
    assert b_link.resolve() == real_b


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
        main(
            [
                "--outdir",
                str(tmp_path / "out"),
                "--nodes-dir",
                str(tmp_path / "nodes"),
                str(job),
            ]
        )


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
    """A .requests entry that does not match a Pipeline label must fail."""
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
    assert any(combo_dir.rglob("a.txt"))  # requested output symlinked
    assert not any(combo_dir.rglob("b.txt"))  # unrequested output excluded


# ── multiple combos ───────────────────────────────────────────────────────────


def test_multiple_combos(tmp_path):
    P1 = Pipeline(DAG(tmp_path))
    P1.out = R_step1(P1, v="alpha")
    for n in P1.nodes:
        n.path.parent.mkdir(parents=True, exist_ok=True)
        n.path.touch()

    P2 = Pipeline(DAG(tmp_path))
    P2.out = R_step1(P2, v="beta")
    for n in P2.nodes:
        n.path.parent.mkdir(parents=True, exist_ok=True)
        n.path.touch()

    combos = [
        ("combo_alpha", P1, _resolve_request(P1, None)),
        ("combo_beta", P2, _resolve_request(P2, None)),
    ]
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

    main(
        [
            "--nodes-dir",
            "nodes",
            "--results-dir",
            "results",
            "--validation",
            "schema.py:validate",
            "job.toml",
        ]
    )

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


def test_outputs_subcommand_lists_requested_paths_without_execution(
    tmp_path, factory_file, capsys
):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(
        [
            "outputs",
            "--nodes-dir",
            str(tmp_path / "nodes"),
            "--results-dir",
            str(tmp_path / "results"),
            str(job),
        ]
    )

    captured = capsys.readouterr().out
    assert "[job]" in captured
    assert "b\tnode=" in captured
    assert "result=" in captured
    assert not list((tmp_path / "nodes").rglob("a.txt"))


def test_provenance_subcommand_prints_metadata(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(tmp_path / "results"),
            str(job),
        ]
    )
    output = _real_output(nodes_dir, "b.txt")

    main(["provenance", str(output)])

    captured = capsys.readouterr().out
    assert "rule = make_b" in captured
    assert "v = 'hello'" in captured


def test_job_fingerprint_function_is_installed_before_output_addressing(
    tmp_path, factory_file, capsys
):
    fingerprint_file = tmp_path / "fingerprint.py"
    fingerprint_file.write_text("def fingerprint(args):\n" "    return 'b' * 64\n")
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\n'
        f'".fingerprint" = "{fingerprint_file}:fingerprint"\n'
        'v = "hello"\n'
    )

    main(["outputs", "--nodes-dir", str(tmp_path / "nodes"), str(job)])

    captured = capsys.readouterr().out
    assert f"/{'b' * 64}/" in captured


def test_outputs_shellpath_matches_run_shellpath_paths(tmp_path, factory_file, capsys):
    shell = shutil.which("sh") or "/bin/sh"
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"

    main(
        [
            "outputs",
            "--shellpath",
            shell,
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            str(job),
        ]
    )
    predicted = capsys.readouterr().out
    predicted_node = next(
        part.removeprefix("node=")
        for part in predicted.split()
        if part.startswith("node=")
    )

    main(
        [
            "--shellpath",
            shell,
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            str(job),
        ]
    )

    assert Path(predicted_node).exists()


def test_provenance_prints_explicit_shellpath(tmp_path, factory_file, capsys):
    shell = str(Path(shutil.which("sh") or "/bin/sh").resolve())
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    main(
        [
            "--shellpath",
            shell,
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(tmp_path / "results"),
            str(job),
        ]
    )
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


def test_main_writes_execution_summary_for_requested_ancestors(tmp_path, factory_file):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"

    main(["--nodes-dir", str(nodes_dir), "--results-dir", str(results_dir), str(job)])

    summary = results_dir / "job" / "execution.toml"
    assert summary.exists()
    doc = tomlkit.parse(summary.read_text())
    nodes = {node["label"]: node for node in doc["nodes"]}
    assert set(nodes) == {"a", "b"}
    assert nodes["a"]["cached"] is False
    assert nodes["b"]["cached"] is False
    assert nodes["a"]["duration_seconds"] >= 0
    assert nodes["a"]["output_size_bytes"] == 0
    assert "make_a" == nodes["a"]["rule"]


def test_main_execution_summary_survives_autocleaned_intermediate(
    tmp_path, factory_file
):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"

    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            "--autoclean",
            str(job),
        ]
    )

    doc = tomlkit.parse((results_dir / "job" / "execution.toml").read_text())
    nodes = {node["label"]: node for node in doc["nodes"]}
    assert set(nodes) == {"a", "b"}
    assert not Path(nodes["a"]["path"]).exists()
    assert Path(nodes["b"]["path"]).exists()


def test_main_keep_going_failure_writes_execution_summary(tmp_path):
    factory = tmp_path / "pipe.py"
    factory.write_text(textwrap.dedent("""\
        from necroflow import Pipeline, NodeType, command, output
        class A(NodeType): filename = "a.txt"
        class B(NodeType): filename = "b.txt"
        @command("touch {a}; exit 1")
        def fail_a(v: str):
            a = output(A)
            return a
        @command("touch {b}")
        def make_b(v: str):
            b = output(B)
            return b
        def factory(P, cfg):
            P.a = fail_a(P, v=cfg["v"])
            P.b = make_b(P, v=cfg["v"])
    """))
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory}:factory"\nv = "hello"\n')
    results_dir = tmp_path / "results"

    with pytest.raises(ExceptionGroup):
        main(
            [
                "--nodes-dir",
                str(tmp_path / "nodes"),
                "--results-dir",
                str(results_dir),
                "--keep-going",
                str(job),
            ]
        )

    doc = tomlkit.parse((results_dir / "job" / "execution.toml").read_text())
    nodes = {node["label"]: node for node in doc["nodes"]}
    assert nodes["a"]["state"] == "failed"
    assert nodes["a"]["exit_code"] == 1
    assert nodes["b"]["state"] == "up_to_date"
    assert nodes["b"]["cached"] is False


# -- Agent-oriented JSON, doctor, and explain -------------------------------


def _json_stdout(capsys):
    import json

    return json.loads(capsys.readouterr().out)


def test_outputs_json_lists_requested_paths(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(
        [
            "outputs",
            "--json",
            "--nodes-dir",
            str(tmp_path / "nodes"),
            "--results-dir",
            str(tmp_path / "results"),
            str(job),
        ]
    )

    payload = _json_stdout(capsys)
    assert payload["jobs"][0]["label"] == "job"
    requested = payload["jobs"][0]["requested"]
    assert requested[0]["label"] == "b"
    assert requested[0]["rule"] == "make_b"
    assert requested[0]["node_path"].endswith("/b.txt")
    assert requested[0]["result_path"].endswith("/b.txt")


def test_graph_json_lists_nodes_and_edges(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["graph", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    payload = _json_stdout(capsys)
    assert {node["label"] for node in payload["nodes"]} == {"a", "b"}
    assert len(payload["edges"]) == 1
    assert payload["jobs"][0]["label"] == "job"


def test_graph_json_includes_pipeline_sections(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.section("Preparation")
    P.out = R_step1(P, v="hello")
    P.section("Analysis")
    P.log = R_step2(P, P.out)
    dag = P.dag
    dag.require(P.sinks())

    payload = _graph_payload(
        dag, [("job", P, _resolve_request(P, ["log"]))], nodes_dir=tmp_path
    )

    assert {node["label"]: node["section"] for node in payload["nodes"]} == {
        "out": "Preparation",
        "log": "Analysis",
    }


@pytest.mark.skipif(shutil.which("dot") is None, reason="graphviz 'dot' not on PATH")
def test_graph_png_renders_file(tmp_path, factory_file, capsys):
    pytest.importorskip("networkx")
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    png_path = tmp_path / "dag.png"

    main(["graph", "--png", str(png_path), "--outdir", str(tmp_path / "out"), str(job)])

    assert png_path.exists()
    assert png_path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_graph_png_without_networkx_fails_clearly(tmp_path, factory_file, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "networkx":
            raise ImportError("simulated missing networkx")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="dev' extra"):
        main(
            [
                "graph",
                "--png",
                str(tmp_path / "dag.png"),
                "--outdir",
                str(tmp_path / "out"),
                str(job),
            ]
        )


def test_provenance_json_prints_metadata(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(tmp_path / "results"),
            str(job),
        ]
    )
    capsys.readouterr()
    output = _real_output(nodes_dir, "b.txt")

    main(["provenance", "--json", str(output)])

    payload = _json_stdout(capsys)
    assert payload["rule"] == "make_b"
    assert payload["config"]["v"] == "hello"
    assert payload["path"].endswith("/b.txt")


def test_doctor_json_ok_for_valid_job(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["doctor", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    payload = _json_stdout(capsys)
    assert payload == {"issues": [], "ok": True}


def test_doctor_json_reports_missing_pipeline(tmp_path, capsys):
    job = tmp_path / "job.toml"
    job.write_text('v = "hello"\n')

    with pytest.raises(SystemExit) as excinfo:
        main(["doctor", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    assert excinfo.value.code == 1
    payload = _json_stdout(capsys)
    assert payload["ok"] is False
    assert payload["issues"][0]["code"] == "NF_CONFIG_MISSING_PIPELINE"


def test_doctor_json_reports_bad_request_label(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n".requests" = ["missing"]\n'
    )

    with pytest.raises(SystemExit):
        main(["doctor", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    payload = _json_stdout(capsys)
    assert payload["issues"][0]["code"] == "NF_REQUEST_LABEL_NOT_FOUND"


def test_doctor_json_reports_invalid_shellpath(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit):
        main(
            [
                "doctor",
                "--json",
                "--shellpath",
                str(tmp_path / "missing-shell"),
                "--outdir",
                str(tmp_path / "out"),
                str(job),
            ]
        )

    payload = _json_stdout(capsys)
    assert payload["issues"][0]["code"] == "NF_SHELLPATH_INVALID"


def test_explain_json_reports_missing_and_up_to_date(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"

    main(["explain", "--json", "--outdir", str(outdir), str(job)])
    missing = _json_stdout(capsys)
    by_label = {node["label"]: node for node in missing["nodes"]}
    assert by_label["a"]["will_run"] is True
    assert by_label["a"]["reasons"][0]["kind"] == "output_missing"

    main(["--outdir", str(outdir), str(job)])
    capsys.readouterr()
    main(["explain", "--json", "--outdir", str(outdir), str(job)])
    cached = _json_stdout(capsys)
    by_label = {node["label"]: node for node in cached["nodes"]}
    assert by_label["a"]["will_run"] is False
    assert by_label["a"]["reasons"][0]["kind"] == "up_to_date"


def test_explain_json_node_filter(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(
        [
            "explain",
            "--json",
            "--node",
            "b",
            "--outdir",
            str(tmp_path / "out"),
            str(job),
        ]
    )

    payload = _json_stdout(capsys)
    assert [node["label"] for node in payload["nodes"]] == ["b"]
