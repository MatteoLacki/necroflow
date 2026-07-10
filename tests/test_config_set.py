import json

import pytest
import tomlkit

from necroflow.tools.config_set import copy_config_with_field, main


def test_copy_toml_with_field_from_toml(tmp_path):
    template = tmp_path / "template.toml"
    source = tmp_path / "source.toml"
    output = tmp_path / "out.toml"
    template.write_text('[sage]\nthreads = 4\nmode = "old"\n')
    source.write_text('[derived]\nmode = "new"\n')

    copy_config_with_field(
        template,
        output,
        target_field="sage.mode",
        source_path=source,
        source_field="derived.mode",
    )

    doc = tomlkit.parse(output.read_text())
    assert doc["sage"]["threads"] == 4
    assert doc["sage"]["mode"] == "new"


def test_copy_json_with_field_from_json(tmp_path):
    template = tmp_path / "template.json"
    source = tmp_path / "source.json"
    output = tmp_path / "out.json"
    template.write_text(json.dumps({"sage": {"max_peaks": 100, "value": None}}))
    source.write_text(json.dumps({"derived": {"value": [1, 2, 3]}}))

    main(
        [
            str(template),
            str(output),
            "--target",
            "sage.value",
            "--source",
            str(source),
            "--source-field",
            "derived.value",
        ]
    )

    assert json.loads(output.read_text()) == {
        "sage": {"max_peaks": 100, "value": [1, 2, 3]}
    }


def test_creates_missing_target_tables(tmp_path):
    template = tmp_path / "template.toml"
    source = tmp_path / "source.json"
    output = tmp_path / "out.toml"
    template.write_text('name = "example"\n')
    source.write_text(json.dumps({"value": 0.25}))

    copy_config_with_field(
        template,
        output,
        target_field="sage.tolerances.precursor",
        source_path=source,
        source_field="value",
    )

    doc = tomlkit.parse(output.read_text())
    assert doc["sage"]["tolerances"]["precursor"] == 0.25


def test_rejects_output_extension_mismatch(tmp_path):
    template = tmp_path / "template.toml"
    source = tmp_path / "source.toml"
    output = tmp_path / "out.json"
    template.write_text("a = 1\n")
    source.write_text("b = 2\n")

    with pytest.raises(ValueError, match="output extension must match"):
        copy_config_with_field(
            template,
            output,
            target_field="a",
            source_path=source,
            source_field="b",
        )


def test_missing_source_field_exits(tmp_path):
    template = tmp_path / "template.json"
    source = tmp_path / "source.json"
    output = tmp_path / "out.json"
    template.write_text(json.dumps({"a": 1}))
    source.write_text(json.dumps({"b": 2}))

    with pytest.raises(SystemExit, match="field not found: missing"):
        main(
            [
                str(template),
                str(output),
                "--target",
                "a",
                "--source",
                str(source),
                "--source-field",
                "missing",
            ]
        )
