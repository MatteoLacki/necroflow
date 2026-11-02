from datetime import date, datetime, time
from pathlib import Path

import pytest

from necroflow import DAG, Pipeline, Node, NamedValues, command, outputs, shell, run
from necroflow.fingerprints import (
    FingerprintValueError,
    canonical_bytes,
    python_identity,
)


class Result(Node):
    filename = "result.txt"


@command
def make(label: str = "x"):
    result = outputs(Result)
    shell("printf %s {label} > {result}")
    return result


def test_named_values_support_mapping_attribute_and_diagnostic_views():
    """NamedValues must preserve mapping semantics while offering readable attributes."""

    values = NamedValues({"sample": "S1", "items": "declared"})

    assert len(values) == 2
    assert list(values) == ["sample", "items"]
    assert values.sample == "S1"
    assert values["items"] == "declared"
    assert callable(values.items)
    assert repr(values) == "NamedValues(sample='S1', items='declared')"
    with pytest.raises(AttributeError, match="missing"):
        _ = values.missing


def test_framed_canonical_values_preserve_boundaries_and_order():
    assert canonical_bytes(["ab", "c"]) != canonical_bytes(["a", "bc"])
    assert canonical_bytes({"b": 2, "a": 1}) == canonical_bytes({"a": 1, "b": 2})
    assert canonical_bytes({3, 1, 2}) == canonical_bytes({2, 3, 1})


@pytest.mark.parametrize(
    "value",
    [
        None,
        1.25,
        float("nan"),
        float("inf"),
        float("-inf"),
        b"\x00payload",
        Path("relative/data.txt"),
        datetime(2026, 7, 27, 12, 34, 56),
        date(2026, 7, 27),
        time(12, 34, 56),
        ("ordered", 1),
    ],
)
def test_canonical_bytes_supports_declared_builtin_value_families(value):
    """Every documented builtin fingerprint value must encode deterministically."""

    assert canonical_bytes(value) == canonical_bytes(value)


def test_canonical_bytes_rejects_non_string_mapping_keys_with_value_path():
    """Invalid nested mapping keys must identify their location in job config."""

    with pytest.raises(
        FingerprintValueError,
        match=r"job\.options: fingerprint mappings require string keys, got int",
    ):
        canonical_bytes({"options": {1: "invalid"}}, path="job")


def test_explicit_shellpath_does_not_change_builtin_materializer_fingerprint(tmp_path):
    from necroflow import text_file_rule

    write_text = text_file_rule("write_text", Result)
    default = Pipeline(DAG(tmp_path / "default"))
    explicit = Pipeline(DAG(tmp_path / "explicit"), shellpath="/bin/sh")

    assert (
        write_text(default, text="same").provenance_hash
        == write_text(explicit, text="same").provenance_hash
    )


def test_executable_hash_tracks_python_version(monkeypatch, tmp_path):
    original = make(Pipeline(DAG(tmp_path / "first"))).provenance_hash
    monkeypatch.setattr(
        "necroflow.fingerprints.python_identity", lambda: python_identity() + "-other"
    )
    changed = make(Pipeline(DAG(tmp_path / "second"))).provenance_hash
    assert changed != original


def test_executable_hash_ignores_formatting_but_tracks_logic(tmp_path):
    from necroflow.config import load_module

    def load(name, body):
        path = tmp_path / f"{name}.py"
        path.write_text(
            "from necroflow import command, outputs, shell\n"
            "from test_fingerprints import Result\n"
            "@command\ndef build(label: str = 'x'):\n"
            "    result = outputs(Result)\n" + body + "\n    return result\n"
        )
        return load_module(path, kind="test").build

    compact = load("compact", "    shell('touch {result}')")
    commented = load("commented", "    # comment\n    shell( 'touch {result}' )")
    changed = load("changed", "    shell('printf changed > {result}')")
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    first = compact(pipeline)
    assert commented(pipeline) is first
    assert changed(pipeline).rule_hash != first.rule_hash


def test_explicit_shellpath_changes_executable_provenance(tmp_path):
    default = make(Pipeline(DAG(tmp_path / "default")))
    explicit = make(Pipeline(DAG(tmp_path / "explicit"), shellpath="/bin/bash"))
    assert explicit.rule_hash == default.rule_hash
    assert explicit.provenance_hash != default.provenance_hash


def test_executable_provenance_records_source_and_template(tmp_path):
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    pipeline.result = make(pipeline)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    metadata = (
        pipeline.result.rule_call.metadata_dir / "dependencies.toml"
    ).read_text()
    assert 'kind = "python-construction"' in metadata
    assert 'format = "v4"' in metadata
    assert pipeline.result.provenance_hash in metadata
    assert python_identity() in metadata
    assert "source = " in metadata
    assert "realized = " in metadata
