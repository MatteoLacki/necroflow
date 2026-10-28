"""The declaration name changes without breaking old imports or identities."""

from necroflow import Command, DAG, Pipeline, NodeType, command, output
from necroflow.rules import Rule, Inputs, Outputs


class Result(NodeType):
    filename = "result.txt"


@command
def make():
    result = output(Result)
    command = "touch {result}"
    return result


def test_decorators_return_public_command_class():
    """Command is the actual runtime class, with Rule retained as the same alias."""
    assert type(make) is Command
    assert Command.__name__ == "Command"
    assert Rule is Command
    assert isinstance(make, Rule)


def test_legacy_alias_preserves_identity_and_interning(tmp_path):
    """Legacy and new constructors produce identical recipes and canonical Nodes."""
    old = Rule("make", Inputs(), Outputs(result=Result), "touch {result}")
    new = Command("make", Inputs(), Outputs(result=Result), "touch {result}")
    pipeline = Pipeline(DAG(tmp_path))
    a = old(pipeline)
    b = new(pipeline)
    c = make(pipeline)
    assert a is b is c
    assert a.rule_hash == b.rule_hash == c.rule_hash
    assert a.provenance_hash == b.provenance_hash == c.provenance_hash
