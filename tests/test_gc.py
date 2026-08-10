import builtins

import pytest

import necroflow.cli as cli_core
from necroflow import DAG, Inputs, NodeType, Outputs, Pipeline
from necroflow.cli import main
from necroflow.rules import Rule


class Result(NodeType):
    filename = "result.txt"


class Source(NodeType):
    filename = "source.txt"


CURRENT_CONSUMER = Rule(
    "consume",
    Inputs(source=Source),
    Outputs(result=Result),
    "cp {source} {result}",
)

CURRENT_PRODUCER = Rule(
    "produce",
    Inputs(value=str),
    Outputs(result=Result),
    "printf current > {result}",
)


def _run_cached_rule(nodes_dir, command="printf old > {result}", *, mutable=False):
    rule = Rule(
        "produce",
        Inputs(value=str),
        Outputs(result=Result),
        command,
        mutable=mutable,
    )
    dag = DAG(nodes_dir)
    pipeline = Pipeline(dag)
    pipeline.result = rule(pipeline, value="same")
    dag.require([pipeline.result])
    dag.run()
    return pipeline.result.path.parent


def _run_source_and_consumer(nodes_dir):
    """Build an obsolete-capable parent rule feeding the current consumer."""
    old_source = Rule(
        "source",
        Inputs(value=str),
        Outputs(source=Source),
        "printf old > {source}",
    )
    dag = DAG(nodes_dir)
    pipeline = Pipeline(dag)
    source = old_source(pipeline, value="same")
    pipeline.result = CURRENT_CONSUMER(pipeline, source)
    dag.require([pipeline.result])
    dag.run()
    return source.path.parent, pipeline.result.path.parent


def _write_gc_scope(path, command="printf new > {result}"):
    path.write_text(
        "from necroflow import NodeType, command, output\n"
        "class Result(NodeType):\n"
        "    filename = 'result.txt'\n"
        f"@command({command!r})\n"
        "def produce(value: str):\n"
        "    result = output(Result)\n"
        "    return result\n"
        "rules = [produce]\n"
    )


def _gc(nodes_dir, scope, *extra):
    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-rules-script",
            str(scope),
            *extra,
        ]
    )


def test_gc_yes_deletes_cache_from_obsolete_rule(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    obsolete_call = _run_cached_rule(nodes_dir)
    scope = tmp_path / "gc_rules.py"
    _write_gc_scope(scope)

    _gc(nodes_dir, scope, "-y")

    assert not obsolete_call.exists()
    assert str(obsolete_call) in capsys.readouterr().out


def test_gc_never_deletes_obsolete_mutable_state(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    mutable_call = _run_cached_rule(nodes_dir, mutable=True)
    scope = tmp_path / "gc_rules.py"
    _write_gc_scope(scope)

    _gc(nodes_dir, scope, "-y")

    assert mutable_call.exists()
    assert "Protected mutable state: 1" in capsys.readouterr().out


def test_gc_recursively_deletes_descendants_of_obsolete_rules(tmp_path):
    """A current recipe cannot reuse provenance produced by an obsolete parent recipe."""
    nodes_dir = tmp_path / "nodes"
    source_call, consumer_call = _run_source_and_consumer(nodes_dir)

    scope = tmp_path / "gc_rules.py"
    scope.write_text(
        "from test_gc import CURRENT_CONSUMER, Source\n"
        "from necroflow import Inputs, Outputs\n"
        "from necroflow.rules import Rule\n"
        "CURRENT_SOURCE = Rule('source', Inputs(value=str), Outputs(source=Source), "
        "'printf new > {source}')\n"
        "rules = [CURRENT_SOURCE, CURRENT_CONSUMER]\n"
    )

    _gc(nodes_dir, scope, "-y")

    assert not source_call.exists()
    assert not consumer_call.exists()


def test_gc_keeps_nodes_from_current_rules(tmp_path):
    nodes_dir = tmp_path / "nodes"
    dag = DAG(nodes_dir)
    pipeline = Pipeline(dag)
    pipeline.result = CURRENT_PRODUCER(pipeline, value="same")
    dag.require([pipeline.result])
    dag.run()
    call_dir = pipeline.result.path.parent
    scope = tmp_path / "gc_rules.py"
    scope.write_text(
        "from test_gc import CURRENT_PRODUCER\nrules = [CURRENT_PRODUCER]\n"
    )

    _gc(nodes_dir, scope, "-y")

    assert call_dir.exists()


def test_gc_preserves_nodes_whose_rule_name_the_script_never_declares(tmp_path, capsys):
    """A rule missing from the script is a forgotten import far more often than a deletion.

    Silently collecting its nodes is the one failure mode that destroys live
    data, so an undeclared rule name must be reported and preserved.
    """
    nodes_dir = tmp_path / "nodes"
    call_dir = _run_cached_rule(nodes_dir)
    scope = tmp_path / "gc_rules.py"
    scope.write_text(
        "from test_gc import CURRENT_CONSUMER\nrules = [CURRENT_CONSUMER]\n"
    )

    _gc(nodes_dir, scope, "-y")

    assert call_dir.exists()
    output = capsys.readouterr().out
    assert "Rules absent from" in output
    assert "(preserved)" in output
    assert "produce" in output


def test_gc_preserves_descendants_of_undeclared_rules(tmp_path):
    """Preserving an undeclared parent must preserve what descends from it.

    Collecting the child while protecting the parent would delete live data
    through the ancestry walk rather than directly.
    """
    nodes_dir = tmp_path / "nodes"
    source_call, consumer_call = _run_source_and_consumer(nodes_dir)
    scope = tmp_path / "gc_rules.py"
    scope.write_text(
        "from test_gc import CURRENT_CONSUMER\nrules = [CURRENT_CONSUMER]\n"
    )

    _gc(nodes_dir, scope, "-y")

    assert source_call.exists()
    assert consumer_call.exists()


def test_gc_prunes_undeclared_rules_when_requested(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    source_call, consumer_call = _run_source_and_consumer(nodes_dir)
    scope = tmp_path / "gc_rules.py"
    scope.write_text(
        "from test_gc import CURRENT_CONSUMER\nrules = [CURRENT_CONSUMER]\n"
    )

    _gc(nodes_dir, scope, "--prune-unknown-rules", "-y")

    assert not source_call.exists()
    assert not consumer_call.exists()
    assert "(pruning)" in capsys.readouterr().out


def test_gc_accepts_rules_held_in_a_module_level_container(tmp_path):
    """An explicit list covers factory-built rules that a namespace scan would miss."""
    nodes_dir = tmp_path / "nodes"
    call_dir = _run_cached_rule(nodes_dir, command="printf current > {result}")
    scope = tmp_path / "gc_rules.py"
    scope.write_text(
        "from test_gc import Result\n"
        "from necroflow import Inputs, Outputs\n"
        "from necroflow.rules import Rule\n"
        "REGISTRY = {\n"
        "    'produce': Rule('produce', Inputs(value=str), Outputs(result=Result), "
        "'printf current > {result}'),\n"
        "}\n"
        "rules = list(REGISTRY.values())\n"
    )

    _gc(nodes_dir, scope, "-y")

    assert call_dir.exists()


@pytest.mark.parametrize(
    "body",
    [
        "rules = []",
        "rules = 'produce'",
        "from test_gc import CURRENT_PRODUCER\nrules = CURRENT_PRODUCER",
        "def pipeline(P, config):\n    pass\npipelines = [pipeline]",
    ],
    ids=["empty", "string", "bare-rule", "legacy-pipelines-list"],
)
def test_gc_rejects_a_script_without_a_rules_list(tmp_path, body):
    nodes_dir = tmp_path / "nodes"
    _run_cached_rule(nodes_dir)
    scope = tmp_path / "gc_rules.py"
    scope.write_text(body + "\n")

    with pytest.raises(SystemExit) as excinfo:
        _gc(nodes_dir, scope, "-y")

    assert "rules list of Rule objects" in str(excinfo.value)


def test_gc_deletes_unreadable_current_layout_in_non_current_batch(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    call_dir = _run_cached_rule(nodes_dir)
    (call_dir / ".rip" / "dependencies.toml").write_text("not = [toml")
    scope = tmp_path / "gc_rules.py"
    _write_gc_scope(scope)

    _gc(nodes_dir, scope, "-y")

    assert not call_dir.exists()
    output = capsys.readouterr().out
    assert "Non-current layout:" in output
    assert str(call_dir) in output


def test_gc_deletes_legacy_one_hash_layout_in_non_current_batch(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    legacy_call = nodes_dir / "produce" / ("a" * 64)
    legacy_call.mkdir(parents=True)
    (legacy_call / "result.txt").write_text("legacy")
    scope = tmp_path / "gc_rules.py"
    _write_gc_scope(scope)

    _gc(nodes_dir, scope, "-y")

    assert not legacy_call.exists()
    output = capsys.readouterr().out
    assert "Non-current layout:" in output
    assert str(legacy_call) in output


def test_gc_interactive_refusal_deletes_nothing(tmp_path, monkeypatch, capsys):
    nodes_dir = tmp_path / "nodes"
    call_dir = _run_cached_rule(nodes_dir)
    scope = tmp_path / "gc_rules.py"
    _write_gc_scope(scope)
    monkeypatch.setattr(cli_core.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(builtins, "input", lambda _prompt: "n")

    _gc(nodes_dir, scope)

    assert call_dir.exists()
    assert "No directories deleted." in capsys.readouterr().out
