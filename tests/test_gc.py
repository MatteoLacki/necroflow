import builtins

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
    output_type = Result
    if mutable:

        class MutableResult(NodeType):
            filename = "result.txt"
            mutable = True

        output_type = MutableResult
    rule = Rule("produce", Inputs(value=str), Outputs(result=output_type), command)
    dag = DAG(nodes_dir)
    pipeline = Pipeline(dag)
    pipeline.result = rule(pipeline, value="same")
    dag.require([pipeline.result])
    dag.execute()
    return pipeline.result.path.parent


def _write_gc_scope(path, command="printf new > {result}"):
    path.write_text(
        "from necroflow import NodeType, command, output\n"
        "class Result(NodeType):\n"
        "    filename = 'result.txt'\n"
        f"@command({command!r})\n"
        "def produce(value: str):\n"
        "    result = output(Result)\n"
        "    return result\n"
        "def pipeline(P, config):\n"
        "    P.result = produce(P, value=config['value'])\n"
        "pipelines = [pipeline]\n"
    )


def test_gc_yes_deletes_cache_from_obsolete_rule(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    obsolete_call = _run_cached_rule(nodes_dir)
    scope = tmp_path / "gc_pipelines.py"
    _write_gc_scope(scope)

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
            "-y",
        ]
    )

    assert not obsolete_call.exists()
    assert str(obsolete_call) in capsys.readouterr().out


def test_gc_never_deletes_obsolete_mutable_state(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    mutable_call = _run_cached_rule(nodes_dir, mutable=True)
    scope = tmp_path / "gc_pipelines.py"
    _write_gc_scope(scope)

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
            "-y",
        ]
    )

    assert mutable_call.exists()
    assert "Protected mutable state: 1" in capsys.readouterr().out


def test_gc_recursively_deletes_descendants_of_obsolete_rules(tmp_path):
    """A current recipe cannot reuse provenance produced by an obsolete parent recipe."""
    nodes_dir = tmp_path / "nodes"
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
    dag.execute()
    source_call = source.path.parent
    consumer_call = pipeline.result.path.parent

    scope = tmp_path / "gc_pipelines.py"
    scope.write_text(
        "from test_gc import CURRENT_CONSUMER, Source\n"
        "from necroflow import Inputs, Outputs\n"
        "from necroflow.rules import Rule\n"
        "CURRENT_SOURCE = Rule('source', Inputs(value=str), Outputs(source=Source), "
        "'printf new > {source}')\n"
        "def pipeline(P, config):\n"
        "    source = CURRENT_SOURCE(P, value=config['value'])\n"
        "    P.result = CURRENT_CONSUMER(P, source)\n"
        "pipelines = [pipeline]\n"
    )

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
            "-y",
        ]
    )

    assert not source_call.exists()
    assert not consumer_call.exists()


def test_gc_keeps_nodes_from_current_rules(tmp_path):
    nodes_dir = tmp_path / "nodes"
    dag = DAG(nodes_dir)
    pipeline = Pipeline(dag)
    pipeline.result = CURRENT_PRODUCER(pipeline, value="same")
    dag.require([pipeline.result])
    dag.execute()
    call_dir = pipeline.result.path.parent
    scope = tmp_path / "gc_pipelines.py"
    scope.write_text(
        "from test_gc import CURRENT_PRODUCER\n"
        "def pipeline(P, config):\n"
        "    P.result = CURRENT_PRODUCER(P, value=config['value'])\n"
        "pipelines = [pipeline]\n"
    )

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
            "-y",
        ]
    )

    assert call_dir.exists()


def test_gc_deletes_unreadable_current_layout_in_non_current_batch(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    call_dir = _run_cached_rule(nodes_dir)
    (call_dir / ".rip" / "dependencies.toml").write_text("not = [toml")
    scope = tmp_path / "gc_pipelines.py"
    _write_gc_scope(scope)

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
            "-y",
        ]
    )

    assert not call_dir.exists()
    output = capsys.readouterr().out
    assert "Non-current layout:" in output
    assert str(call_dir) in output


def test_gc_deletes_legacy_one_hash_layout_in_non_current_batch(tmp_path, capsys):
    nodes_dir = tmp_path / "nodes"
    legacy_call = nodes_dir / "produce" / ("a" * 64)
    legacy_call.mkdir(parents=True)
    (legacy_call / "result.txt").write_text("legacy")
    scope = tmp_path / "gc_pipelines.py"
    _write_gc_scope(scope)

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
            "-y",
        ]
    )

    assert not legacy_call.exists()
    output = capsys.readouterr().out
    assert "Non-current layout:" in output
    assert str(legacy_call) in output


def test_gc_interactive_refusal_deletes_nothing(tmp_path, monkeypatch, capsys):
    nodes_dir = tmp_path / "nodes"
    call_dir = _run_cached_rule(nodes_dir)
    scope = tmp_path / "gc_pipelines.py"
    _write_gc_scope(scope)
    monkeypatch.setattr(cli_core.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(builtins, "input", lambda _prompt: "n")

    main(
        [
            "gc",
            "--nodes-dir",
            str(nodes_dir),
            "--gc-pipelines-script",
            str(scope),
        ]
    )

    assert call_dir.exists()
    assert "No directories deleted." in capsys.readouterr().out
