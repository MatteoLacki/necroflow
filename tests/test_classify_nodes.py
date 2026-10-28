"""RuleCall cache policy regression tests."""

import os
import shutil
from pathlib import Path

import tomlkit

from necroflow import (
    DAG,
    Inputs,
    Node,
    Outputs,
    Pipeline,
    Verdict,
    run,
)
from necroflow.hashers import load_hasher
from necroflow.planning import plan_execution
from necroflow.rules import Command


class Source(Node):
    filename = "source.txt"


class Result(Node):
    filename = "result.txt"


class Log(Node):
    filename = "result.log"


class Bundle(Node):
    filename = "bundle"


SOURCE = Command("source", Inputs(), Outputs(source=Source), "printf stable > {source}")
CONSUME = Command(
    "consume", Inputs(source=Source), Outputs(result=Result), "cp {source} {result}"
)
PAIR = Command(
    "pair",
    Inputs(),
    Outputs(result=Result, log=Log),
    "touch {result} {log}",
)
BUNDLE_SOURCE = Command(
    "bundle_source",
    Inputs(),
    Outputs(bundle=Bundle),
    "mkdir {bundle}; printf a > {bundle}/a; printf b > {bundle}/b",
)
BUNDLE_CONSUME = Command(
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
    """Opting into content reuse preserves consumers of identical rebuilt bytes."""
    first = _pipeline(tmp_path)
    run(first.dag)
    second = _pipeline(tmp_path)

    report = run(
        second.dag,
        forced_stale_call_keys={second.source.rule_call.relative_path},
        reuse_unchanged=True,
    )

    assert report[second.source.rule_call.relative_path.as_posix()].cached is False
    assert report[second.result.rule_call.relative_path.as_posix()].cached is True


def test_forced_parent_changed_bytes_replays_immutable_child(tmp_path):
    """Even with content reuse, changed rebuilt bytes invalidate the consumer."""
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
        reuse_unchanged=True,
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

    assert plan.verdicts[second.source.rule_call.relative_path] is Verdict.UP_TO_DATE
    assert plan.verdicts[second.result.rule_call.relative_path] is Verdict.STALE
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
    plan = plan_execution(second_dag)

    assert (
        plan.verdicts[second_pipeline.bundle.rule_call.relative_path]
        is Verdict.UP_TO_DATE
    )
    assert (
        plan.verdicts[second_pipeline.result.rule_call.relative_path] is Verdict.STALE
    )


def test_missing_consumed_hash_marks_consumer_stale(tmp_path):
    """Missing consumed hash is unsafe, so consumer must rerun."""
    first = _pipeline(tmp_path)
    run(first.dag)
    metadata_path = first.result.rule_call.workdir / ".rip" / "dependencies.toml"
    metadata = tomlkit.parse(metadata_path.read_text())
    del metadata["parents"][0]["consumed_hash"]
    metadata_path.write_text(tomlkit.dumps(metadata))

    second = _pipeline(tmp_path)
    plan = plan_execution(second.dag)
    assert plan.verdicts[second.result.rule_call.relative_path] is Verdict.STALE
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
    plan = plan_execution(second_dag)

    assert plan.verdicts[second.result.rule_call.relative_path] is Verdict.MISSING


def test_switching_hasher_makes_consumers_rerun_once(tmp_path):
    """A consumed hash from another hasher is never compared, only replaced."""
    first = _pipeline(tmp_path)
    run(first.dag, hasher="sha256")

    second = _pipeline(tmp_path)
    plan = plan_execution(second.dag, hasher="blake3")
    assert plan.verdicts[second.result.rule_call.relative_path] is Verdict.STALE
    reason = plan.reasons[second.result.rule_call.relative_path][0]
    assert reason["kind"] == "consumed_hash_other_hasher"
    assert reason["hasher"] == "blake3"
    assert reason["consumed_hash"].startswith("sha256:")

    run(second.dag, hasher="blake3")
    third = _pipeline(tmp_path)
    plan = plan_execution(third.dag, hasher="blake3")
    assert plan.verdicts[third.result.rule_call.relative_path] is Verdict.UP_TO_DATE


class _CountingHasher:
    """BLAKE3 under its own name, recording which files it reads in full."""

    name = "blake3"

    def __init__(self):
        self.files: list[str] = []
        self._blake3 = load_hasher("blake3")

    def hash_path(self, path, threads):
        self.files.append(Path(path).name)
        return self._blake3.hash_path(path, threads)


class _EditingHasher(_CountingHasher):
    """Simulates an external edit landing while the file is being hashed."""

    def hash_path(self, path, threads):
        digest = super().hash_path(path, threads)
        later = Path(path).stat().st_mtime_ns + 1_000_000
        os.utime(path, ns=(later, later))
        return digest


def _edit_source(pipeline, text: str) -> Path:
    """Rewrite the source output externally, newer than its stored hash."""
    hash_file = pipeline.source.rule_call.workdir / ".rip" / "source.txt.hash"
    pipeline.source.path.write_text(text)
    newer = hash_file.stat().st_mtime_ns + 1_000_000
    os.utime(pipeline.source.path, ns=(newer, newer))
    return hash_file


def test_edited_parent_is_hashed_once_then_trusted_again(tmp_path):
    """An externally edited parent is read in full once, not on every run.

    The source call never reruns, so its own completion never refreshes its
    stored hash. The run that notices the edit stores the fresh digest, so
    later runs trust it again instead of re-reading unchanged bytes forever.
    """
    run(_pipeline(tmp_path).dag)
    _edit_source(_pipeline(tmp_path), "external")

    noticing = _CountingHasher()
    run(_pipeline(tmp_path).dag, hasher=noticing)
    later = _CountingHasher()
    run(_pipeline(tmp_path).dag, hasher=later)

    assert noticing.files.count("source.txt") == 1
    assert later.files == []


def test_hasher_switch_retags_root_parent_hash_once(tmp_path):
    """After a hasher switch, a parent that never reruns is rehashed once.

    The consumer's rerun reads the source with the new hasher; storing that
    digest stops every later run from reading the source again.
    """
    run(_pipeline(tmp_path).dag, hasher="sha256")
    run(_pipeline(tmp_path).dag, hasher="blake3")

    pipeline = _pipeline(tmp_path)
    hash_file = pipeline.source.rule_call.workdir / ".rip" / "source.txt.hash"
    later = _CountingHasher()
    run(pipeline.dag, hasher=later)

    assert hash_file.read_text().startswith("blake3:")
    assert later.files == []


def test_parent_edited_while_hashing_keeps_its_old_stored_hash(tmp_path):
    """A fresh digest is stored only if the output did not change meanwhile.

    Otherwise the stored hash would be newer than the edit and vouch for
    bytes that are no longer there.
    """
    run(_pipeline(tmp_path).dag)
    hash_file = _edit_source(_pipeline(tmp_path), "external")
    stored = hash_file.read_text()

    run(_pipeline(tmp_path).dag, hasher=_EditingHasher())

    assert hash_file.read_text() == stored


def test_dry_run_stores_refreshed_hash_for_explain(tmp_path):
    """A dry run holds the node-store lock, so it may store refreshed hashes.

    Explain never reads whole outputs; after a dry run has stored the edited
    parent's digest, explain-style planning decides the consumer again.
    """
    run(_pipeline(tmp_path).dag)
    _edit_source(_pipeline(tmp_path), "external")

    run(_pipeline(tmp_path).dag, dry_run=True)
    explained = _pipeline(tmp_path)
    plan = plan_execution(explained.dag, hash_untrusted_parents=False)

    assert plan.verdicts[explained.result.rule_call.relative_path] is Verdict.STALE
    assert plan.reasons[explained.result.rule_call.relative_path][0]["kind"] == (
        "parent_content_changed"
    )
