"""Tests for CLI internals: _materialize_results, manifest keys, main()."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule

import shutil
import fcntl
import textwrap
import time
import tomlkit
import pytest

import necroflow.cli as cli_core
from necroflow.planning import plan_execution
from pathlib import Path
from necroflow import NodeType, Pipeline, DAG, output
from necroflow.cli import (
    _materialize_results,
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
        node.path.write_text(node.output_name)
    P.finish()
    return P, tmp_path


# ── result materialization ────────────────────────────────────────────────────


def test_combo_dir_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _materialize_results(outdir, combos)
    assert (outdir / "run1").is_dir()


def test_requested_outputs_are_copied_as_regular_files(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _materialize_results(outdir, combos)
    combo_dir = outdir / "run1"
    results = list(combo_dir.rglob("*.txt")) + list(combo_dir.rglob("*.log"))
    assert results
    assert all(f.is_file() and not f.is_symlink() for f in results)


def test_copy_path_uses_requested_label_and_filename(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]

    _materialize_results(outdir, combos)

    result = outdir / "run1" / "log" / "run.log"
    assert result.is_file()
    assert not result.is_symlink()
    assert result.read_bytes() == P.log.path.read_bytes()


def test_materialization_preserves_intentional_symlink_outputs(tmp_path):
    pipeline, outdir = _make_pipeline_with_outputs(tmp_path)
    target = tmp_path / "external.log"
    target.write_text("external")
    pipeline.log.path.unlink()
    pipeline.log.path.symlink_to(target.resolve())

    _materialize_results(outdir, [("run1", pipeline, _resolve_request(pipeline, None))])

    result = outdir / "run1" / "log" / "run.log"
    assert result.is_symlink()
    assert result.resolve() == target


def test_copy_uses_clone_option_instead_of_gnu_reflink_on_macos(tmp_path, monkeypatch):
    commands = []
    monkeypatch.setattr(cli_core.sys, "platform", "darwin")
    monkeypatch.setattr(
        cli_core.subprocess,
        "run",
        lambda command, check: commands.append((command, check)),
    )

    cli_core._copy_result(tmp_path / "source", tmp_path / "destination")

    assert commands == [
        (
            [
                "cp",
                "-a",
                "-c",
                str(tmp_path / "source"),
                str(tmp_path / "destination"),
            ],
            True,
        )
    ]


def test_skips_missing_outputs(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.out = R_step1(P, v="hello")
    # do NOT create the output file
    P.finish()
    combos = [("run1", P, _resolve_request(P, None))]
    _materialize_results(results_dir=tmp_path, combos=combos)
    combo_dir = tmp_path / "run1"
    assert combo_dir.is_dir()  # dir still created
    assert not any(combo_dir.rglob("*.txt"))


def test_results_can_use_separate_nodes_and_results_dirs(tmp_path):
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"
    P, _ = _make_pipeline_with_outputs(nodes_dir)

    _materialize_results(results_dir, [("run1", P, _resolve_request(P, None))])

    result = results_dir / "run1" / "log" / "run.log"
    assert result.is_file() and not result.is_symlink()
    assert result.read_bytes() == P.log.path.read_bytes()
    content = (results_dir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    assert doc["outputs"]["log"]["path"] == "log/run.log"
    assert doc["outputs"]["log"]["origin_node_key"] == P.log.relative_path.as_posix()
    assert len(doc["outputs"]["log"]["content_sha256"]) == 64


def test_materialization_rejects_malformed_manifest(tmp_path):
    """Without a readable manifest, Necroflow cannot safely identify owned copies."""

    pipeline, outdir = _make_pipeline_with_outputs(tmp_path)
    combo_dir = outdir / "run1"
    combo_dir.mkdir()
    (combo_dir / "manifest.toml").write_text("not = [valid")

    with pytest.raises(ValueError, match="cannot read result manifest"):
        _materialize_results(
            outdir, [("run1", pipeline, _resolve_request(pipeline, None))]
        )


def test_materialization_removes_stale_manifest_owned_results(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combo_dir = outdir / "run1"
    stale = combo_dir / "step2" / "abc123" / "run.log"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale")
    (combo_dir / "manifest.toml").write_text(
        '[outputs]\nlog = "step2/abc123/run.log"\n'
    )

    _materialize_results(outdir, [("run1", P, _resolve_request(P, None))])

    assert not stale.exists()
    assert not stale.parent.exists()
    assert (combo_dir / "log" / "run.log").is_file()
    assert not (combo_dir / "log" / "run.log").is_symlink()


# ── manifest ─────────────────────────────────────────────────────────────────


def test_manifest_created(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]
    _materialize_results(outdir, combos)
    assert (outdir / "run1" / "manifest.toml").exists()


def test_manifest_keys_are_requested_labels(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _resolve_request(P, None)
    combos = [("run1", P, sinks)]
    _materialize_results(outdir, combos)
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

    P.finish()
    request = _resolve_request(P, None)
    _materialize_results(tmp_path, [("run1", P, request)])

    assert P.primary is P.alias
    assert (tmp_path / "run1" / "primary" / "out.txt").is_file()
    assert not (tmp_path / "run1" / "primary" / "out.txt").is_symlink()
    assert (tmp_path / "run1" / "alias" / "out.txt").is_file()
    manifest = tomlkit.parse((tmp_path / "run1" / "manifest.toml").read_text())
    assert set(manifest["outputs"]) == {"primary", "alias"}


def test_non_identifier_label_is_quoted_in_manifest(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P["primary result"] = R_step1(P, v="hello")
    P["primary result"].path.parent.mkdir(parents=True, exist_ok=True)
    P["primary result"].path.touch()

    P.finish()
    _materialize_results(tmp_path, [("run1", P, _resolve_request(P, None))])

    manifest = tomlkit.parse((tmp_path / "run1" / "manifest.toml").read_text())
    assert manifest["outputs"]["primary result"]["path"] == "primary result/out.txt"


def test_manifest_only_sinks(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    sinks = _resolve_request(P, None)
    combos = [("run1", P, sinks)]
    _materialize_results(outdir, combos)
    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    keys = set(doc["outputs"].keys())
    # "out" is intermediate (P.out), "log" is the sink (P.log)
    assert "out" not in keys
    assert "log" in keys


def test_manifest_values_are_visible_result_paths(tmp_path):
    P, outdir = _make_pipeline_with_outputs(tmp_path)
    combos = [("run1", P, _resolve_request(P, None))]

    _materialize_results(outdir, combos)

    content = (outdir / "run1" / "manifest.toml").read_text()
    doc = tomlkit.parse(content)
    assert doc["outputs"]["log"]["path"] == "log/run.log"


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
def path_factory_file(tmp_path):
    f = tmp_path / "path_pipe.py"
    f.write_text(
        FACTORY_SRC.replace(
            "P.b = make_b(P, P.a)",
            'P["dataset/config"] = make_b(P, P.a)',
        )
    )
    return f


@pytest.fixture
def job_toml(tmp_path, factory_file):
    t = tmp_path / "job.toml"
    t.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    return t


def _real_output(outdir: Path, filename: str) -> Path:
    matches = [path for path in outdir.rglob(filename) if len(path.parent.name) == 64]
    assert len(matches) == 1
    return matches[0]


def test_pipeline_factory_return_value_errors_cleanly(tmp_path):
    """CLI pipeline factories must mutate the supplied Pipeline and return None."""

    factory = tmp_path / "returning.py"
    factory.write_text("def factory(pipeline, config):\n    return config\n")
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory}:factory"\nvalue = 1\n')

    with pytest.raises(
        SystemExit, match="must mutate the supplied Pipeline and return None"
    ):
        main(["outputs", "--outdir", str(tmp_path / "out"), str(job)])


def test_load_callable_rejects_non_callable_target(tmp_path):
    """A valid module path must still identify a callable target."""

    from necroflow.config import load_callable

    module = tmp_path / "values.py"
    module.write_text("target = 42\n")

    with pytest.raises(TypeError, match="target .* is not callable"):
        load_callable(f"{module}:target", kind="test")


def test_iter_job_configs_rejects_missing_job_file(tmp_path):
    """The Python config API must name a missing job file before parsing."""

    from necroflow.config import iter_job_configs

    missing = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError, match="job file not found"):
        list(iter_job_configs(missing))


def test_main_invalidate_parent_same_bytes_keeps_child_cached(tmp_path, factory_file):
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
    assert b_path.stat().st_mtime == b_mtime


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


def test_iter_job_configs_extends_one_table_into_multiple_configs(tmp_path):
    """Two stage configs may inherit one shared table without sharing a Node.

    Config inheritance happens while loading the job TOML, so factories receive
    two complete dictionaries that they can serialize into independently
    fingerprinted config-file Nodes.
    """
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        "[common]\n"
        'shared = "value"\n'
        "[first]\n"
        '".extends" = "common"\n'
        'specific = "first"\n'
        "[second]\n"
        '".extends" = "common"\n'
        'specific = "second"\n'
    )

    (loaded,) = iter_job_configs(job)

    assert loaded.config["first"] == {"shared": "value", "specific": "first"}
    assert loaded.config["second"] == {"shared": "value", "specific": "second"}


def test_iter_job_configs_extends_deep_merges_with_child_overrides(tmp_path):
    """Inherited nested tables are preserved while child values take precedence."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        "[base]\n"
        'value = "base"\n'
        "[base.nested]\n"
        'shared = "base"\n'
        'replaced = "base"\n'
        "[child]\n"
        '".extends" = "base"\n'
        'value = "child"\n'
        "[child.nested]\n"
        'replaced = "child"\n'
        'specific = "child"\n'
    )

    (loaded,) = iter_job_configs(job)

    assert loaded.config["child"] == {
        "value": "child",
        "nested": {
            "shared": "base",
            "replaced": "child",
            "specific": "child",
        },
    }


def test_iter_job_configs_extends_accepts_absolute_dotted_table_paths(tmp_path):
    """A nested base table can be named by its absolute dotted config path."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        "[defaults.common]\n"
        'shared = "value"\n'
        "[stage]\n"
        '".extends" = "defaults.common"\n'
        'specific = "stage"\n'
    )

    (loaded,) = iter_job_configs(job)

    assert loaded.config["stage"] == {"shared": "value", "specific": "stage"}


def test_iter_job_configs_extends_supports_inheritance_chains(tmp_path):
    """A table may extend another inherited table without leaking metadata."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        "[base]\n"
        "first = 1\n"
        "[middle]\n"
        '".extends" = "base"\n'
        "second = 2\n"
        "[leaf]\n"
        '".extends" = "middle"\n'
        "third = 3\n"
    )

    (loaded,) = iter_job_configs(job)

    assert loaded.config["middle"] == {"first": 1, "second": 2}
    assert loaded.config["leaf"] == {
        "first": 1,
        "second": 2,
        "third": 3,
    }


def test_iter_job_configs_extends_rejects_missing_base_table(tmp_path):
    """A misspelled base path fails with the extending table named in the error."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text("[stage]\n" '".extends" = "missing"\n')

    with pytest.raises(
        ValueError, match="config table 'stage' extends missing table 'missing'"
    ):
        list(iter_job_configs(job))


def test_iter_job_configs_extends_rejects_inheritance_cycles(tmp_path):
    """Inheritance cycles fail explicitly instead of recursing until overflow."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        "[first]\n" '".extends" = "second"\n' "[second]\n" '".extends" = "first"\n'
    )

    with pytest.raises(
        ValueError,
        match="config table inheritance cycle: first -> second -> first",
    ):
        list(iter_job_configs(job))


def test_iter_job_configs_extends_requires_a_dotted_path_string(tmp_path):
    """The inheritance directive rejects non-string values with a clear error."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text("[base]\n" "value = 1\n" "[stage]\n" '".extends" = 3\n')

    with pytest.raises(
        ValueError, match="config table 'stage' .extends must be a dotted path string"
    ):
        list(iter_job_configs(job))


def test_iter_job_configs_extends_rejects_non_table_target(tmp_path):
    """An inheritance path must identify a table rather than a scalar value."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text("base = 1\n" "[stage]\n" '".extends" = "base"\n')

    with pytest.raises(
        ValueError, match="config table 'stage' extends non-table 'base'"
    ):
        list(iter_job_configs(job))


def test_iter_job_configs_resolves_extends_after_grid_expansion(tmp_path):
    """A variant-specific grid leaves an inherited independent config unchanged."""
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        "[common]\n"
        'shared = "value"\n'
        "[independent]\n"
        '".extends" = "common"\n'
        'stage = "independent"\n'
        "[variant]\n"
        '".extends" = "common"\n'
        'method__grid = ["first", "second"]\n'
    )

    loaded = list(iter_job_configs(job))

    assert len(loaded) == 2
    assert [item.config["independent"] for item in loaded] == [
        {"shared": "value", "stage": "independent"},
        {"shared": "value", "stage": "independent"},
    ]
    assert [item.config["variant"]["method"] for item in loaded] == [
        "first",
        "second",
    ]


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
    result = tmp_path / "results" / "job" / "b" / "b.txt"
    assert result.is_file() and not result.is_symlink()


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
    assert not list((results_dir / "job").rglob("a.txt"))
    result = results_dir / "job" / "b" / "b.txt"
    assert result.is_file() and not result.is_symlink()
    assert result.read_bytes() == real_b.read_bytes()


def test_main_materializes_results_while_node_store_is_locked(
    tmp_path, job_toml, monkeypatch
):
    nodes_dir = tmp_path / "nodes"

    def assert_locked(results_dir, combos):
        lock_path = nodes_dir / ".rip" / "necroflow.lock"
        with lock_path.open("a") as handle:
            with pytest.raises(BlockingIOError):
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)

    monkeypatch.setattr(cli_core, "_materialize_results", assert_locked)

    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(tmp_path / "results"),
            str(job_toml),
        ]
    )


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


def test_main_path_request_creates_nested_result_and_manifest(
    tmp_path, path_factory_file, capsys
):
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{path_factory_file}:factory"\n'
        '".requests" = ["dataset/config"]\n'
        'v = "hello"\n'
    )
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"

    main(
        [
            "outputs",
            "--json",
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            str(job),
        ]
    )
    requested = _json_stdout(capsys)["jobs"][0]["requested"]
    assert requested[0]["label"] == "dataset/config"
    assert requested[0]["result_path"].endswith("/dataset/config/b.txt")

    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            str(job),
        ]
    )

    result = results_dir / "job" / "dataset" / "config" / "b.txt"
    assert result.is_file() and not result.is_symlink()
    assert result.read_bytes() == _real_output(nodes_dir, "b.txt").read_bytes()
    manifest = tomlkit.parse((results_dir / "job" / "manifest.toml").read_text())
    assert manifest["outputs"]["dataset/config"]["path"] == "dataset/config/b.txt"

    output = _real_output(nodes_dir, "b.txt")
    mtime = output.stat().st_mtime
    time.sleep(0.05)
    main(
        [
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            "--invalidate",
            "dataset/config",
            str(job),
        ]
    )
    assert output.stat().st_mtime > mtime

    capsys.readouterr()
    main(
        [
            "explain",
            "--json",
            "--node",
            "dataset/config",
            "--nodes-dir",
            str(nodes_dir),
            "--results-dir",
            str(results_dir),
            str(job),
        ]
    )
    payload = _json_stdout(capsys)
    assert [
        output["label"] for call in payload["calls"] for output in call["outputs"]
    ] == ["dataset/config"]


@pytest.mark.parametrize(
    "requests",
    [
        '"a"',
        '[["a"]]',
        '["a", 1]',
    ],
)
def test_job_requests_must_be_a_list_of_strings(tmp_path, factory_file, requests):
    from necroflow.config import iter_job_configs

    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\n'
        f'".requests" = {requests}\n'
        'v = "hello"\n'
    )

    with pytest.raises(ValueError, match="list of strings"):
        list(iter_job_configs(job))


def test_run_preflights_result_paths_before_execution(
    tmp_path, factory_file, monkeypatch
):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    def reject(_path):
        raise ValueError("simulated NAME_MAX failure")

    def unexpected_run(*_args, **_kwargs):
        pytest.fail("DAG.run was called before result-path preflight")

    monkeypatch.setattr(cli_core, "_check_path_limits", reject)
    monkeypatch.setattr(DAG, "run", unexpected_run)

    with pytest.raises(SystemExit, match="Pipeline label 'b' is invalid.*NAME_MAX"):
        main(["--outdir", str(tmp_path / "out"), str(job)])


def test_link_creation_defensively_validates_result_paths(tmp_path, monkeypatch):
    P, _outdir = _make_pipeline_with_outputs(tmp_path / "nodes")
    results_dir = tmp_path / "results"

    def reject(_path):
        raise ValueError("simulated PATH_MAX failure")

    monkeypatch.setattr(cli_core, "_check_path_limits", reject)

    with pytest.raises(ValueError, match="Pipeline label 'log' is invalid"):
        _materialize_results(results_dir, [("run1", P, _resolve_request(P, None))])
    assert not results_dir.exists()


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


def test_main_grid_expansion_uses_short_names_by_default(tmp_path, factory_file):
    """Job result labels default to short form; --long-names restores the full form."""
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\nv__grid = ["hello", "world"]\n'
    )

    short_dir = tmp_path / "short"
    main(["--outdir", str(short_dir), str(job)])
    assert (short_dir / "job__hello").is_dir()
    assert (short_dir / "job__world").is_dir()

    long_dir = tmp_path / "long"
    main(["--outdir", str(long_dir), "--long-names", str(job)])
    assert (long_dir / "job__v+hello").is_dir()
    assert (long_dir / "job__v+world").is_dir()


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

    Guards the regression where _materialize_results iterated all pipeline nodes
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

    P1.finish()
    P2.finish()
    combos = [
        ("combo_alpha", P1, _resolve_request(P1, None)),
        ("combo_beta", P2, _resolve_request(P2, None)),
    ]
    _materialize_results(tmp_path, combos)
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


def test_provenance_reports_the_stored_v4_identity_hashes(
    tmp_path, factory_file, capsys
):
    """provenance must surface both stored hashes, matching the node's own path.

    Both hashes remain stored even though only provenance_hash names the call
    directory. Tie output to metadata and path so schema changes cannot silently
    leave either value empty.
    """
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    main(
        ["--nodes-dir", str(nodes_dir), "--results-dir", str(tmp_path / "r"), str(job)]
    )
    output = _real_output(nodes_dir, "b.txt")
    _rule, provenance_hash, _filename = output.relative_to(nodes_dir).parts
    metadata = tomlkit.parse((output.parent / ".rip" / "dependencies.toml").read_text())
    rule_hash = metadata["identity"]["rule_hash"]

    main(["provenance", str(output)])

    captured = capsys.readouterr().out
    assert f"rule_hash = {rule_hash}" in captured
    assert f"provenance_hash = {provenance_hash}" in captured


def test_job_custom_fingerprint_is_rejected(tmp_path, factory_file):
    fingerprint_file = tmp_path / "fingerprint.py"
    fingerprint_file.write_text("def fingerprint(args):\n" "    return 'b' * 64\n")
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory_file}:factory"\n'
        f'".fingerprint" = "{fingerprint_file}:fingerprint"\n'
        'v = "hello"\n'
    )

    with pytest.raises(SystemExit, match="removed '.fingerprint'"):
        main(["outputs", "--nodes-dir", str(tmp_path / "nodes"), str(job)])


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
    """Execution summaries describe timed rule calls, not their output Nodes."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    results_dir = tmp_path / "results"

    main(["--nodes-dir", str(nodes_dir), "--results-dir", str(results_dir), str(job)])

    summary = results_dir / "job" / "execution.toml"
    assert summary.exists()
    doc = tomlkit.parse(summary.read_text())
    rules = {rule["name"]: rule for rule in doc["rules"]}
    assert set(rules) == {"make_a", "make_b"}
    assert rules["make_a"]["cached"] is False
    assert rules["make_b"]["cached"] is False
    assert rules["make_a"]["duration_seconds"] >= 0
    assert rules["make_a"]["output_size_bytes"] == 0
    assert rules["make_a"]["output_size_human"] == "0 B"
    assert rules["make_a"]["outputs"][0]["labels"] == ["a"]
    assert doc["total_duration_seconds"] == pytest.approx(
        sum(rule["duration_seconds"] for rule in rules.values())
    )


def test_main_execution_summary_counts_cooutput_rule_once(tmp_path):
    """Co-output Nodes share one execution and must not duplicate its runtime."""

    factory = tmp_path / "pipe.py"
    factory.write_text(textwrap.dedent("""\
        from necroflow import NodeType
        from necroflow.rules import Inputs, Outputs, Rule
        class A(NodeType): filename = "a.txt"
        class B(NodeType): filename = "b.txt"
        make_pair = Rule(
            "make_pair", Inputs(v=str), Outputs(a=A, b=B), "touch {a} {b}"
        )
        def factory(P, cfg):
            P.a, P.b = make_pair(P, v=cfg["v"])
    """))
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory}:factory"\nv = "hello"\n')
    results_dir = tmp_path / "results"

    main(
        [
            "--nodes-dir",
            str(tmp_path / "nodes"),
            "--results-dir",
            str(results_dir),
            str(job),
        ]
    )

    doc = tomlkit.parse((results_dir / "job" / "execution.toml").read_text())
    assert len(doc["rules"]) == 1
    rule = doc["rules"][0]
    assert rule["name"] == "make_pair"
    assert {output["name"] for output in rule["outputs"]} == {"a", "b"}
    assert doc["total_duration_seconds"] == pytest.approx(rule["duration_seconds"])


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
    rules = {rule["name"]: rule for rule in doc["rules"]}
    assert set(rules) == {"make_a", "make_b"}
    assert not Path(rules["make_a"]["outputs"][0]["path"]).exists()
    assert Path(rules["make_b"]["outputs"][0]["path"]).exists()


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
    rules = {rule["name"]: rule for rule in doc["rules"]}
    assert rules["fail_a"]["state"] == "failed"
    assert rules["fail_a"]["exit_code"] == 1
    assert rules["make_b"]["state"] == "up_to_date"
    assert rules["make_b"]["cached"] is False


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


def test_subpipeline_labels_are_requestable_from_cli(tmp_path, capsys):
    """The CLI must finish the root and resolve qualified subpipeline requests."""
    factory = tmp_path / "subpipelines.py"
    factory.write_text(textwrap.dedent("""\
            from necroflow import NodeType, command, output

            class Result(NodeType):
                filename = "result.txt"

            @command("touch {result}")
            def build(value: str):
                result = output(Result)
                return result

            def sample_pipeline(P, value):
                P.result = build(P, value=value)

            def factory(P, config):
                for sample in config["samples"]:
                    sample_pipeline(P.subpipeline(f"samples/{sample}"), sample)
            """))
    job = tmp_path / "job.toml"
    job.write_text(
        f'".pipeline" = "{factory}:factory"\n'
        '".requests" = ["samples/B/result"]\n'
        'samples = ["A", "B"]\n'
    )

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

    requested = _json_stdout(capsys)["jobs"][0]["requested"]
    assert [item["label"] for item in requested] == ["samples/B/result"]
    assert requested[0]["result_path"].endswith("/samples/B/result/result.txt")


def test_graph_json_lists_nodes_and_edges(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["graph", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    payload = _json_stdout(capsys)
    assert {node["label"] for node in payload["nodes"]} == {"a", "b"}
    assert len(payload["edges"]) == 1
    assert payload["jobs"][0]["label"] == "job"


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


def test_graph_output_writes_rendered_dag(tmp_path, factory_file):
    """Graph output files must contain the same TGF DAG without executing it."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    graph = tmp_path / "graph.tgf"

    main(["graph", "--output", str(graph), "--outdir", str(tmp_path / "out"), str(job)])

    rendered = graph.read_text()
    assert "make_a" in rendered
    assert "make_b" in rendered
    assert "\n#\n" in rendered
    assert not list((tmp_path / "out").rglob("a.txt"))


def test_provenance_missing_metadata_errors_cleanly(tmp_path):
    """Provenance must report the exact absent metadata path for unknown outputs."""

    output = tmp_path / "unknown.txt"

    with pytest.raises(SystemExit, match="provenance metadata not found"):
        main(["provenance", str(output)])


def test_explain_rejects_unknown_label(tmp_path, factory_file):
    """Explain node filters must name a label present in the selected jobs."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit, match="explain label not found: absent"):
        main(
            ["explain", "--node", "absent", "--outdir", str(tmp_path / "out"), str(job)]
        )


def test_doctor_json_reports_invalid_resource_cap(tmp_path, factory_file, capsys):
    """Doctor must assign a stable issue code to malformed resource constraints."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    with pytest.raises(SystemExit):
        main(
            [
                "doctor",
                "--json",
                "--constraint",
                "missing-separator",
                "--outdir",
                str(tmp_path / "out"),
                str(job),
            ]
        )

    payload = _json_stdout(capsys)
    assert payload["issues"][0]["code"] == "NF_RESOURCE_INVALID"


def test_doctor_json_reports_locked_node_store(tmp_path, factory_file, capsys):
    """Doctor must detect the same exclusive node-store lock used by execution."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    nodes_dir = tmp_path / "nodes"
    lock_path = nodes_dir / ".rip" / "necroflow.lock"
    lock_path.parent.mkdir(parents=True)

    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SystemExit):
            main(
                [
                    "doctor",
                    "--json",
                    "--nodes-dir",
                    str(nodes_dir),
                    "--results-dir",
                    str(tmp_path / "results"),
                    str(job),
                ]
            )

    payload = _json_stdout(capsys)
    assert payload["issues"][0]["code"] == "NF_NODESTORE_LOCKED"


def test_doctor_json_ok_for_valid_job(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["doctor", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    payload = _json_stdout(capsys)
    assert payload == {"issues": [], "ok": True}


def test_doctor_json_reports_multiple_pipeline_labels_as_info(tmp_path, capsys):
    """Aliased labels should be visible without making a runnable job invalid."""

    factory_file = tmp_path / "alias_pipe.py"
    factory_file.write_text(
        FACTORY_SRC.replace(
            "    P.b = make_b(P, P.a)",
            "    P.b = make_b(P, P.a)\n    P.alias = P.b",
        )
    )
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["doctor", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    payload = _json_stdout(capsys)
    assert payload["ok"] is True
    assert len(payload["issues"]) == 1
    issue = payload["issues"][0]
    assert issue["code"] == "NF_MULTIPLE_LABELS"
    assert issue["severity"] == "info"
    assert issue["job"] == "job"
    assert issue["labels"] == ["b", "alias"]
    assert issue["path"].endswith("/b.txt")


def test_doctor_text_prints_multiple_pipeline_labels_without_failing(tmp_path, capsys):
    """Informational findings must be printed while doctor still exits successfully."""

    factory_file = tmp_path / "alias_pipe.py"
    factory_file.write_text(
        FACTORY_SRC.replace(
            "    P.b = make_b(P, P.a)",
            "    P.b = make_b(P, P.a)\n    P.alias = P.b",
        )
    )
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["doctor", "--outdir", str(tmp_path / "out"), str(job)])

    output = capsys.readouterr().out
    assert "info: NF_MULTIPLE_LABELS" in output
    assert "doctor: ok" not in output


def test_doctor_json_reports_invalid_result_path(
    tmp_path, factory_file, monkeypatch, capsys
):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    def reject(_path):
        raise ValueError("simulated PATH_MAX failure")

    monkeypatch.setattr(cli_core, "_check_path_limits", reject)

    with pytest.raises(SystemExit) as excinfo:
        main(["doctor", "--json", "--outdir", str(tmp_path / "out"), str(job)])

    assert excinfo.value.code == 1
    payload = _json_stdout(capsys)
    assert payload["ok"] is False
    assert any(issue["code"] == "NF_RESULT_PATH_INVALID" for issue in payload["issues"])


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


def test_doctor_text_reports_success_and_errors(tmp_path, factory_file, capsys):
    """Doctor text mode must be concise while retaining stable issue codes."""

    valid = tmp_path / "valid.toml"
    valid.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    main(["doctor", "--outdir", str(tmp_path / "valid-out"), str(valid)])
    assert capsys.readouterr().out.strip() == "doctor: ok"

    invalid = tmp_path / "invalid.toml"
    invalid.write_text('v = "hello"\n')
    with pytest.raises(SystemExit):
        main(["doctor", "--outdir", str(tmp_path / "invalid-out"), str(invalid)])
    assert "NF_CONFIG_MISSING_PIPELINE" in capsys.readouterr().out


def test_explain_text_prints_state_and_reasons(tmp_path, factory_file, capsys):
    """Explain text mode must expose the same actionable state reasons as JSON."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')

    main(["explain", "--outdir", str(tmp_path / "out"), str(job)])

    output = capsys.readouterr().out
    assert "state: missing" in output
    assert "will_run: true" in output
    assert "reason: output_missing" in output
    assert "resources: threads=1" in output


def _explain_by_label(payload):
    return {
        output["label"]: call for call in payload["calls"] for output in call["outputs"]
    }


def test_explain_json_reports_stale_causes(tmp_path, factory_file, capsys):
    """Explain must distinguish forced, compromised, and changed-parent staleness."""

    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"
    main(["--outdir", str(outdir), str(job)])
    capsys.readouterr()

    main(["explain", "--json", "--invalidate", "b", "--outdir", str(outdir), str(job)])
    forced = _explain_by_label(_json_stdout(capsys))
    assert {reason["kind"] for reason in forced["b"]["reasons"]} == {
        "forced_invalidation"
    }

    b_output = _real_output(outdir, "b.txt")
    (b_output.parent / ".rip" / "state").write_text("running")
    main(["explain", "--json", "--outdir", str(outdir), str(job)])
    compromised = _explain_by_label(_json_stdout(capsys))
    assert "compromised_prior_state" in {
        reason["kind"] for reason in compromised["b"]["reasons"]
    }
    (b_output.parent / ".rip" / "state").write_text("up_to_date")

    time.sleep(0.05)
    _real_output(outdir, "a.txt").write_text("changed")
    main(["explain", "--json", "--outdir", str(outdir), str(job)])
    changed = _explain_by_label(_json_stdout(capsys))
    assert "parent_content_changed" in {
        reason["kind"] for reason in changed["b"]["reasons"]
    }


def test_explain_json_reports_missing_and_up_to_date(tmp_path, factory_file, capsys):
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    outdir = tmp_path / "out"

    main(["explain", "--json", "--outdir", str(outdir), str(job)])
    missing = _json_stdout(capsys)
    by_label = _explain_by_label(missing)
    assert by_label["a"]["will_run"] is True
    assert by_label["a"]["reasons"][0]["kind"] == "output_missing"

    main(["--outdir", str(outdir), str(job)])
    capsys.readouterr()
    main(["explain", "--json", "--outdir", str(outdir), str(job)])
    cached = _json_stdout(capsys)
    by_label = _explain_by_label(cached)
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
    assert [
        output["label"] for call in payload["calls"] for output in call["outputs"]
    ] == ["b"]
