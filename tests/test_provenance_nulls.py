"""Nullable scalar settings must remain executable and inspectable."""

import json

import pytest
import tomlkit

from necroflow import DAG, Node, Pipeline, command, outputs, run
from necroflow.cli import _provenance_payload, main
from necroflow.rules import Inputs, Outputs, Command


class Out(Node):
    filename = "out.txt"


def _cmd(args):
    return f"echo {args.config.plugin!r} > {args.outputs.out}"


@command(_cmd)
def nullable(plugin: str | None = None):
    out = outputs(Out)
    return out


@pytest.mark.parametrize("plugin", [None, "", "None"])
def test_nullable_command_and_descendant_cache(tmp_path, plugin):
    """Null settings survive commands, ancestor merging, readers, and cached reruns."""
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    pipeline.parent = nullable(pipeline, plugin=plugin)
    copy = Command("copy", Inputs(source=Out), Outputs(out=Out), "cp {source} {out}")
    pipeline.child = copy(pipeline, pipeline.parent)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    identities = [(n.rule_hash, n.provenance_hash) for n in pipeline.nodes]
    run(pipeline.dag)
    mtimes = [n.path.stat().st_mtime_ns for n in pipeline.nodes]
    for node in pipeline.nodes:
        call = node.rule_call
        assert call.state_file.read_text().strip() == "up_to_date"
        doc = tomlkit.parse((call.metadata_dir / "dependencies.toml").read_text())
        assert doc.get("config_null_paths", []) == (
            [["plugin"]] if plugin is None else []
        )
        assert _provenance_payload(node.path)["config"]["plugin"] == plugin
        assert call.read_dependencies() is not None
        assert doc["identity"]["provenance_hash"] == call.provenance_hash
    run(pipeline.dag)
    assert [n.path.stat().st_mtime_ns for n in pipeline.nodes] == mtimes
    assert [(n.rule_hash, n.provenance_hash) for n in pipeline.nodes] == identities


def test_nested_null_config_and_cli_json(tmp_path, capsys):
    """Null paths distinguish keys from indices and preserve real marker-like values."""
    value = {"a.b": [None, "", "None", {"0": None}], "tuple": (None, "x")}
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    rule = Command("nested", Inputs(settings=dict), Outputs(out=Out), "touch {out}")
    pipeline.result = rule(pipeline, settings=value)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    main(["provenance", "--json", str(pipeline.result.path)])
    payload = json.loads(capsys.readouterr().out)
    assert payload["config"]["settings"] == {**value, "tuple": [None, "x"]}
    assert value["a.b"][0] is None
    assert value["tuple"] == (None, "x")


def test_unserializable_provenance_fails_before_runner_or_cleanup(tmp_path):
    """Fingerprint-supported but TOML-unsupported values fail before side effects."""
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    rule = Command("unsupported", Inputs(setting=set), Outputs(out=Out), "touch {out}")
    pipeline.result = rule(pipeline, setting={1, 2})
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    pipeline.result.path.parent.mkdir(parents=True)
    pipeline.result.path.write_text("previous output")
    called = []
    with pytest.raises(ValueError, match="Cannot serialize provenance"):
        run(
            pipeline.dag,
            forced_stale_call_keys={pipeline.result.rule_call.relative_path},
            rule_call_runner=lambda *args: called.append(args),
        )
    assert not called
    assert pipeline.result.path.read_text() == "previous output"


def test_default_and_mixed_none_values(tmp_path):
    """Omitted scalar defaults and explicit non-Node None inputs remain distinct metadata."""
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    pipeline.default = nullable(pipeline)
    mixed = Command("mixed", Inputs(source=Out | None), Outputs(out=Out), "touch {out}")
    pipeline.mixed = mixed(pipeline, None)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert _provenance_payload(pipeline.default.path)["config"]["plugin"] is None
    doc = tomlkit.parse(
        (pipeline.mixed.rule_call.metadata_dir / "dependencies.toml").read_text()
    )
    assert doc["input_values"][0]["type"] == "builtins.NoneType"
    assert doc["input_values"][0]["value"] == "None"


def test_mcp_provenance_restores_null(tmp_path):
    """MCP uses the same null-decoding contract as CLI provenance."""
    pytest.importorskip("mcp")
    from necroflow.mcp_server import provenance

    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    pipeline.result = nullable(pipeline)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert provenance(str(pipeline.result.path))["config"]["plugin"] is None
