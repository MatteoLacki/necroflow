import tomlkit

from necroflow import DAG, Inputs, NodeType, Outputs, Pipeline
from necroflow.config import load_module
from necroflow.fingerprints import declared_rule_hash
from necroflow.rules import Rule


class Result(NodeType):
    filename = "result.txt"


def test_config_variants_share_rule_hash_and_have_distinct_provenance_paths(tmp_path):
    """Configuration selects an invocation below one stable local recipe identity."""

    rule = Rule(
        "produce",
        Inputs(value=str),
        Outputs(result=Result),
        "printf {value} > {result}",
    )
    pipeline = Pipeline(DAG(tmp_path))

    first = rule(pipeline, value="first")
    second = rule(pipeline, value="second")

    assert first.rule_hash == second.rule_hash
    assert first.provenance_hash != second.provenance_hash
    assert len(first.rule_hash) == 64
    assert len(first.provenance_hash) == 64
    assert first.relative_path == first.path.relative_to(tmp_path)
    assert first.relative_path == first.rule_call.relative_path / "result.txt"
    assert first.rule_call.relative_path.parts == (
        "produce",
        first.rule_hash,
        first.provenance_hash,
    )


def test_command_change_changes_rule_hash(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = Rule(
        "produce", Inputs(value=str), Outputs(result=Result), "touch {result}"
    )(pipeline, value="same")
    second = Rule(
        "produce",
        Inputs(value=str),
        Outputs(result=Result),
        "printf changed > {result}",
    )(pipeline, value="same")

    assert first.rule_hash != second.rule_hash


def test_output_mutability_changes_rule_hash(tmp_path):
    class StatefulResult(NodeType):
        filename = "state.sqlite3"

    rule = Rule(
        "produce_state",
        Inputs(value=str),
        Outputs(result=StatefulResult),
        "touch {result}",
    )
    pipeline = Pipeline(DAG(tmp_path))
    immutable = rule(pipeline, value="same")

    StatefulResult.mutable = True
    mutable = rule(pipeline, value="same")

    assert immutable.rule_hash != mutable.rule_hash


def test_success_metadata_records_outputs_and_exact_parent_keys(tmp_path):
    class MutableState(NodeType):
        filename = "state.sqlite3"
        mutable = True

    source_rule = Rule(
        "state", Inputs(value=str), Outputs(state=MutableState), "touch {state}"
    )
    consume_rule = Rule(
        "consume", Inputs(state=MutableState), Outputs(result=Result), "touch {result}"
    )
    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)
    pipeline.state = source_rule(pipeline, value="x")
    pipeline.result = consume_rule(pipeline, pipeline.state)
    dag.require([pipeline.result])

    dag.execute()

    source_metadata = tomlkit.parse(
        (pipeline.state.path.parent / ".rip" / "dependencies.toml").read_text()
    )
    result_metadata = tomlkit.parse(
        (pipeline.result.path.parent / ".rip" / "dependencies.toml").read_text()
    )
    assert source_metadata["outputs"][0]["mutable"] is True
    assert source_metadata["outputs"][0]["filename"] == "state.sqlite3"
    assert result_metadata["parents"][0]["node_key"] == (
        pipeline.state.relative_path.as_posix()
    )


def test_rule_hash_is_stable_across_loader_purposes(tmp_path):
    pipeline_file = tmp_path / "pipeline.py"
    pipeline_file.write_text(
        "from necroflow import Inputs, NodeType, Outputs\n"
        "from necroflow.rules import Rule\n"
        "class Result(NodeType):\n"
        "    filename = 'result.txt'\n"
        "produce = Rule('produce', Inputs(value=str), Outputs(result=Result), "
        "'touch {result}')\n"
    )

    runtime = load_module(pipeline_file, kind="pipeline")
    collection = load_module(pipeline_file, kind="gc-pipelines")

    assert runtime.Result.__module__ == "pipeline"
    assert collection.Result.__module__ == "pipeline"
    assert declared_rule_hash(runtime.produce) == declared_rule_hash(collection.produce)
