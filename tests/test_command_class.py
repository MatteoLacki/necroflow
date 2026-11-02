"""Only executable declarations form the public command API."""

import pytest

import necroflow
from necroflow import Command, DAG, Pipeline, Node, command, outputs, shell
from necroflow import rules


class Result(Node):
    filename = "result.txt"


@command
def make():
    result = outputs(Result)
    shell("touch {result}")
    return result


def test_decorator_returns_public_command_class(tmp_path):
    assert type(make) is Command
    pipeline = Pipeline(DAG(tmp_path))
    assert make(pipeline) is make(pipeline)


def test_removed_exports():
    assert not hasattr(rules, "Rule")
    assert not hasattr(necroflow, "CommandArgs")


@pytest.mark.parametrize("template", ["touch {result}", ["touch", "{result}"], 42])
def test_template_decorator_removed(template):
    with pytest.raises(TypeError, match="Use @command"):
        command(template)


def test_factory_removed():
    with pytest.raises(TypeError):
        command(
            "touch {result}", rules.Inputs(), rules.Outputs(result=Result), name="old"
        )


@pytest.mark.parametrize("option", ["cmd", "name", "doc"])
def test_removed_options_cannot_become_resources(option):
    with pytest.raises(TypeError, match="removed command options"):
        command(**{option: "old"})


def test_direct_constructor_removed():
    with pytest.raises(TypeError):
        Command("old", rules.Inputs(), rules.Outputs(result=Result), "touch {result}")
    with pytest.raises(TypeError, match="Use @command"):
        Command()


def test_callback_decorator_removed():
    def callback(args):
        return "touch {result}"

    with pytest.raises(TypeError, match="missing type annotations"):
        command(callback)


def test_builtins_still_work(tmp_path):
    from necroflow import text_file_rule, symlink_file_rule, run

    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    source = tmp_path / "source.txt"
    source.write_text("source")
    pipeline.text = text_file_rule("text", Result)(pipeline, text="hello")
    pipeline.link = symlink_file_rule("link", Result)(pipeline, path=str(source))
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert pipeline.text.path.read_text() == "hello"
    assert pipeline.link.path.read_text() == "source"
