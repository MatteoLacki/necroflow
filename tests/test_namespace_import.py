"""Namespace imports must work throughout typed workflow declarations."""

from types import ModuleType

import pytest

import necroflow as nf


class Text(nf.Node):
    filename = "text.txt"


artifacts = ModuleType("artifact_types")
artifacts.Text = Text


@nf.text_file
def text_source(text: str):
    source = nf.outputs(Text)
    return source


@nf.command
def uppercase(source: Text, threads: nf.Resource[int] = 1):
    result = nf.outputs(artifacts.Text)
    nf.shell("tr '[:lower:]' '[:upper:]' < {source} > {result}")
    return result


def test_namespace_declarations_execute_and_cache(tmp_path):
    """Qualified decorators, outputs, types, resources, and ingestion execute together."""
    external = tmp_path / "external.txt"
    external.write_text("external")

    @nf.workflow
    def build(P):
        P.text = text_source(text="lowercase")
        P.upper = uppercase(P.text)
        P.external = nf.symlink(Text, external)

    P = nf.Pipeline(nf.DAG(tmp_path / "nodes"))
    build(P)
    P.finish()
    P.dag.require(P.sinks())
    P.dag.run()
    assert P.upper.path.read_text() == "LOWERCASE"
    assert P.external.path.is_symlink()
    P.dag.run()
    assert all(event.cached for event in P.dag.last_execution_report.values())


def test_qualified_outputs_still_require_fixed_prefix():
    """Module qualification cannot hide a late output declaration from validation."""
    with pytest.raises(ValueError, match="unconditional top-level prefix"):

        @nf.command
        def late():
            first = nf.outputs(Text)
            nf.shell("touch {first}")
            second = nf.outputs(Text)
            return first, second


def test_output_lookup_does_not_execute_object_properties():
    """Static declaration lookup must not invoke an arbitrary attribute descriptor."""

    class Impostor:
        @property
        def outputs(self):
            pytest.fail("declaration lookup executed a property")

    impostor = Impostor()
    with pytest.raises(ValueError, match="body may contain only"):

        @nf.command("touch {result}")
        def bad():
            result = impostor.outputs(Text)
            return result
