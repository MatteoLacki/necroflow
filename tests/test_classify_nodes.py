"""RuleCall cache policy regression tests."""

import os
import shutil
from pathlib import Path

import tomlkit

from necroflow import (
    DAG,
    Inputs,
    NodeType,
    Outputs,
    Pipeline,
    RuleCallState,
    run,
)
from necroflow.planning import plan_execution
from necroflow.rules import Rule


class Source(NodeType):
    filename = "source.txt"


class Result(NodeType):
    filename = "result.txt"


class Log(NodeType):
    filename = "result.log"


class Bundle(NodeType):
    filename = "bundle"


SOURCE = Rule("source", Inputs(), Outputs(source=Source), "printf stable > {source}")
CONSUME = Rule(
    "consume", Inputs(source=Source), Outputs(result=Result), "cp {source} {result}"
)
PAIR = Rule(
    "pair",
    Inputs(),
    Outputs(result=Result, log=Log),
    "touch {result} {log}",
)
BUNDLE_SOURCE = Rule(
    "bundle_source",
    Inputs(),
    Outputs(bundle=Bundle),
    "mkdir {bundle}; printf a > {bundle}/a; printf b > {bundle}/b",
)
BUNDLE_CONSUME = Rule(
    "bundle_consume",
    Inputs(bundle=Bundle),
    Outputs(result=Result),
    "cat {bundle}/a {bundle}/b > {result}",
)


def _pipeline(outdir: Path, *, source_rule=SOURCE):
    dag = DAG(outdir)
    pipeline = Pipeline(dag)
    pipeline.source = source_rule(pipeline)
    pipeline.result = CONSUME(pipeline, pipeline.source)
    pipeline.finish()
    dag.require([pipeline.result])
    return pipeline


def test_requesting_one_cooutput_activates_and_materializes_whole_call(tmp_path):
    """One selected Node activates every declared output of its RuleCall."""
    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)
    outputs = PAIR(pipeline)
    pipeline.result = outputs.result
    pipeline.finish()
    dag.require([pipeline.result])

    plan = plan_execution(dag)
    assert plan.active == [outputs.result.rule_call]
    report = run(dag)

    assert outputs.result.path.exists()
    assert outputs.log.path.exists()
    event = report[outputs.result.rule_call.relative_path.as_posix()]
    assert set(event.output_node_keys) == {
        outputs.result.relative_path.as_posix(),
        outputs.log.relative_path.as_posix(),
    }


def test_forced_parent_same_bytes_keeps_immutable_child_cached(tmp_path):
    """Rebuilt parent with identical bytes does not invalidate consumer."""
    first = _pipeline(tmp_path)
    run(first.dag)
    second = _pipeline(tmp_path)

    report = run(
        second.dag,
        forced_stale_call_keys={second.source.rule_call.relative_path},
    )

    assert report[second.source.rule_call.relative_path.as_posix()].cached is False
    assert report[second.result.rule_call.relative_path.as_posix()].cached is True


def test_forced_parent_changed_bytes_replays_immutable_child(tmp_path):
    """Consumed SHA disagreement invalidates consumer after parent settles."""
    first = _pipeline(tmp_path)
    run(first.dag)
    second = _pipeline(tmp_path)

    def changed_runner(call, log_path):
        if call.rule.__name__ == "source":
            call.outputs[0].path.write_text("changed")
        else:
            shutil.copyfile(call.parents[0].path, call.outputs[0].path)

    report = run(
        second.dag,
        forced_stale_call_keys={second.source.rule_call.relative_path},
        rule_call_runner=changed_runner,
    )

    assert report[second.result.rule_call.relative_path.as_posix()].cached is False
    assert second.result.path.read_text() == "changed"


def test_external_parent_edit_invalidates_hash_fast_path(tmp_path):
    """Newer parent mtime invalidates stored hash and exposes changed bytes."""
    first = _pipeline(tmp_path)
    run(first.dag)
    hash_file = first.source.rule_call.workdir / ".rip" / "source.txt.hash"
    first.source.path.write_text("external")
    newer = hash_file.stat().st_mtime_ns + 1_000_000
    os.utime(first.source.path, ns=(newer, newer))

    second = _pipeline(tmp_path)
    plan = plan_execution(second.dag)

    assert second.source.rule_call.state == RuleCallState.UP_TO_DATE
    assert second.result.rule_call.state == RuleCallState.STALE
    assert plan.reasons[second.result.rule_call.relative_path][0]["kind"] == (
        "parent_content_changed"
    )


def test_directory_entry_rename_invalidates_stored_hash_fast_path(tmp_path):
    """Directory metadata changes must invalidate hash trust even if file mtimes do not."""
    first_dag = DAG(tmp_path)
    first_pipeline = Pipeline(first_dag)
    first_pipeline.bundle = BUNDLE_SOURCE(first_pipeline)
    first_pipeline.result = BUNDLE_CONSUME(first_pipeline, first_pipeline.bundle)
    first_pipeline.finish()
    first_dag.require([first_pipeline.result])
    run(first_dag)

    first_pipeline.bundle.path.joinpath("a").rename(
        first_pipeline.bundle.path / "renamed"
    )

    second_dag = DAG(tmp_path)
    second_pipeline = Pipeline(second_dag)
    second_pipeline.bundle = BUNDLE_SOURCE(second_pipeline)
    second_pipeline.result = BUNDLE_CONSUME(second_pipeline, second_pipeline.bundle)
    second_pipeline.finish()
    second_dag.require([second_pipeline.result])
    plan_execution(second_dag)

    assert second_pipeline.bundle.rule_call.state == RuleCallState.UP_TO_DATE
    assert second_pipeline.result.rule_call.state == RuleCallState.STALE


def test_missing_consumed_hash_marks_consumer_stale(tmp_path):
    """Missing consumed SHA is unsafe, so consumer must rerun."""
    first = _pipeline(tmp_path)
    run(first.dag)
    metadata_path = first.result.rule_call.workdir / ".rip" / "dependencies.toml"
    metadata = tomlkit.parse(metadata_path.read_text())
    del metadata["parents"][0]["consumed_sha256"]
    metadata_path.write_text(tomlkit.dumps(metadata))

    second = _pipeline(tmp_path)
    plan = plan_execution(second.dag)
    assert second.result.rule_call.state == RuleCallState.STALE
    assert plan.reasons[second.result.rule_call.relative_path][0]["kind"] == (
        "consumed_hash_missing"
    )


def test_missing_one_cooutput_marks_whole_call_missing(tmp_path):
    """Partial cooutput cache cannot satisfy atomic RuleCall."""
    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)
    outputs = PAIR(pipeline)
    pipeline.finish()
    dag.require([outputs.result])
    run(dag)
    outputs.log.path.unlink()

    second_dag = DAG(tmp_path)
    second_pipeline = Pipeline(second_dag)
    second = PAIR(second_pipeline)
    second_pipeline.finish()
    second_dag.require([second.result])
    plan_execution(second_dag)

    assert second.result.rule_call.state == RuleCallState.MISSING
