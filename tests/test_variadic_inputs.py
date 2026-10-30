from __future__ import annotations

from pathlib import Path
import shlex
from typing import Annotated, Union

import pytest

from necroflow import (
    command,
    CommandArgs,
    DAG,
    Inputs,
    Many,
    Node,
    outputs,
    Outputs,
    Pipeline,
    run,
    shell,
    text_file,
)
from necroflow.planning import plan_execution
from necroflow.rules import Command


class Bam(Node):
    filename = "input.bam"


class OtherBam(Node):
    filename = "other.bam"


class Reference(Node):
    filename = "reference.fa"


class Merged(Node):
    filename = "merged.bam"


@command("merge {bams} > {merged}")
def decorated_merge(bams: Annotated[tuple[Bam, ...], Many()]):
    merged = outputs(Merged)
    return merged


LAST_COMMAND_ARGS: CommandArgs | None = None


def variadic_command(args: CommandArgs) -> str:
    global LAST_COMMAND_ARGS
    LAST_COMMAND_ARGS = args
    inputs = " ".join(shlex.quote(str(path)) for path in args.inputs.bams)
    return f"merge {inputs} > {shlex.quote(str(args.outputs.merged))}"


def _source(
    pipeline: Pipeline,
    label: str,
    node_type: type[Node] = Bam,
):
    return Command(
        "source",
        Inputs(label=str),
        Outputs(source=node_type),
        "touch {source}",
    )(pipeline, label=label)


def test_plain_variadic_tuple_accepts_zero_or_more_nodes(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    merge = Command(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    result = merge(pipeline, (first, second))
    empty = merge(pipeline, ())

    assert result.rule_call.inputs.bams == (first, second)
    assert result.parents == [first, second]
    assert result.rule_call.resolve() == (
        f"merge {shlex.quote(str(first.path))} {shlex.quote(str(second.path))} "
        f"> {result.path}"
    )
    assert empty.parents == []
    assert empty.rule_call.resolve() == f"merge  > {empty.path}"


def test_decorated_rule_accepts_variadic_tuple(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")

    result = decorated_merge(pipeline, (first,))

    assert result.rule_call.inputs.bams == (first,)
    assert result.parents == [first]


def test_decorated_rule_accepts_variadic_tuple_by_keyword(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")

    by_position = decorated_merge(pipeline, (first,))
    by_keyword = decorated_merge(pipeline, bams=(first,))

    assert by_position is by_keyword


def test_variadic_static_command_quotes_each_path_independently(tmp_path):
    pipeline = Pipeline(DAG(tmp_path / "root with spaces"))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    merge = Command(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    result = merge(pipeline, (first, second))

    assert result.rule_call.resolve() == (
        f"merge {shlex.quote(str(first.path))} {shlex.quote(str(second.path))} "
        f"> {shlex.quote(str(result.path))}"
    )


def test_callable_command_receives_tuple_of_resolved_paths(tmp_path):
    global LAST_COMMAND_ARGS
    LAST_COMMAND_ARGS = None
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    merge = Command(
        "merge_callback",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        variadic_command,
    )

    result = merge(pipeline, (first, second))
    result.rule_call.resolve()

    assert LAST_COMMAND_ARGS is not None
    assert LAST_COMMAND_ARGS.inputs.bams == (first.path, second.path)
    assert isinstance(LAST_COMMAND_ARGS.inputs.bams, tuple)


def test_many_default_and_inclusive_bounds(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    nodes = tuple(_source(pipeline, str(index)) for index in range(3))
    at_least_one = Command(
        "at_least_one",
        Inputs(bams=Annotated[tuple[Bam, ...], Many()]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    bounded = Command(
        "bounded",
        Inputs(bams=Annotated[tuple[Bam, ...], Many(min=2, max=3)]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    with pytest.raises(ValueError, match="between 1 and unbounded"):
        at_least_one(pipeline, ())
    at_least_one(pipeline, nodes[:1])
    bounded(pipeline, nodes[:2])
    bounded(pipeline, nodes[:3])
    with pytest.raises(ValueError, match="between 2 and 3"):
        bounded(pipeline, nodes[:1])
    with pytest.raises(ValueError, match="between 2 and 3"):
        bounded(pipeline, (*nodes, nodes[0]))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min": True},
        {"min": -1},
        {"max": False},
        {"max": "3"},
        {"min": 3, "max": 2},
    ],
)
def test_many_rejects_invalid_bounds(kwargs):
    with pytest.raises(ValueError):
        Many(**kwargs)


def test_variadic_input_rejects_non_tuple_wrong_elements_and_foreign_dag(tmp_path):
    pipeline = Pipeline(DAG(tmp_path / "one"))
    foreign_pipeline = Pipeline(DAG(tmp_path / "two"))
    bam = _source(pipeline, "bam")
    reference = _source(pipeline, "reference", Reference)
    foreign = _source(foreign_pipeline, "foreign")
    merge = Command(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    with pytest.raises(TypeError, match="expected tuple"):
        merge(pipeline, [bam])
    with pytest.raises(TypeError, match="bams\\[0\\].*expected Bam"):
        merge(pipeline, (reference,))
    with pytest.raises(ValueError, match="different DAG"):
        merge(pipeline, (foreign,))


def test_invalid_variadic_annotations_are_rejected():
    accepted_union = Command(
        "accepted_union",
        Inputs(bams=tuple[Union[Bam, OtherBam], ...]),
        Outputs(merged=Merged),
        "touch {merged}",
    )
    assert accepted_union is not None
    with pytest.raises(TypeError, match="tuple element union"):
        Command(
            "mixed",
            Inputs(bams=tuple[Bam | str, ...]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="fixed-length Node tuple"):
        Command(
            "fixed",
            Inputs(bams=tuple[Bam, Bam]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="element type is not a Node"):
        Command(
            "strings",
            Inputs(values=Annotated[tuple[str, ...], Many()]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="non-variadic Node tuple"):
        Command(
            "scalar",
            Inputs(bam=Annotated[Bam, Many()]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="more than one Many"):
        Command(
            "duplicate",
            Inputs(bams=Annotated[tuple[Bam, ...], Many(), Many()]),
            Outputs(merged=Merged),
            "touch {merged}",
        )


def test_multiple_groups_and_fixed_inputs_keep_logical_and_flat_order(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    left = tuple(_source(pipeline, f"left-{index}") for index in range(2))
    reference = _source(pipeline, "reference", Reference)
    right = tuple(_source(pipeline, f"right-{index}") for index in range(2))
    rule = Command(
        "compare",
        Inputs(
            left=tuple[Bam, ...],
            reference=Reference,
            right=Annotated[tuple[Bam, ...], Many()],
        ),
        Outputs(merged=Merged),
        "compare {left} {reference} {right} > {merged}",
    )

    result = rule(pipeline, left, reference, right)

    assert result.rule_call.inputs.left == left
    assert result.rule_call.inputs.reference is reference
    assert result.rule_call.inputs.right == right
    assert result.parents == [*left, reference, *right]


def test_variadic_union_accepts_each_declared_node_type(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    bam = _source(pipeline, "bam")
    other = _source(pipeline, "other", OtherBam)
    merge = Command(
        "union_merge",
        Inputs(bams=tuple[Bam | OtherBam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    result = merge(pipeline, (bam, other))

    assert result.parents == [bam, other]


def test_variadic_fingerprint_tracks_order_grouping_and_many_bounds(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first, second, third = (
        _source(pipeline, "first"),
        _source(pipeline, "second"),
        _source(pipeline, "third"),
    )
    plain = Command(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    at_least_one = Command(
        "merge",
        Inputs(bams=Annotated[tuple[Bam, ...], Many()]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    at_least_two = Command(
        "merge",
        Inputs(bams=Annotated[tuple[Bam, ...], Many(min=2)]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    grouped = Command(
        "grouped",
        Inputs(left=tuple[Bam, ...], right=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "touch {merged}",
    )

    forward = plain(pipeline, (first, second))
    duplicate = plain(pipeline, (first, second))
    reverse = plain(pipeline, (second, first))

    assert duplicate is forward
    assert reverse.provenance_hash != forward.provenance_hash
    assert (
        at_least_one(pipeline, (first, second)).provenance_hash
        != forward.provenance_hash
    )
    assert (
        at_least_two(pipeline, (first, second)).provenance_hash
        != at_least_one(pipeline, (first, second)).provenance_hash
    )
    assert (
        grouped(pipeline, (first,), (second, third)).provenance_hash
        != grouped(pipeline, (first, second), (third,)).provenance_hash
    )


def test_fixed_input_v4_hashes_are_deterministic(tmp_path):
    source_rule = Command(
        "source", Inputs(text=str), Outputs(source=Bam), "touch {source}"
    )
    consume_rule = Command(
        "consume", Inputs(source=Bam), Outputs(result=Merged), "cp {source} {result}"
    )

    pipeline = Pipeline(DAG(tmp_path / "first"))
    source = source_rule(pipeline, text="x")
    consumed = consume_rule(pipeline, source)

    other = Pipeline(DAG(tmp_path / "second"))
    other_source = source_rule(other, text="x")
    other_consumed = consume_rule(other, other_source)

    assert source.rule_hash == other_source.rule_hash
    assert source.provenance_hash == other_source.provenance_hash
    assert consumed.rule_hash == other_consumed.rule_hash
    assert consumed.provenance_hash == other_consumed.provenance_hash


def test_shell_log_tuple_concatenates_quoted_paths_in_order(tmp_path):
    """Execute the paper's collider with shell-sensitive paths and reversed inputs."""

    class Log(Node):
        filename = "run {literal}'s $value.log"

    @text_file
    def make_log(text: str):
        log = outputs(Log)
        return log

    @command
    def combine_logs(logs: tuple[Log, ...]) -> Log:
        combined_log = outputs(Log)
        if not logs:
            raise ValueError("at least one log is required")
        shell("cat {logs} > {combined_log}")
        return combined_log

    pipeline = Pipeline(DAG(tmp_path / "nodes {braces} 'quotes' $value; spaces"))
    pipeline.first = make_log(pipeline, text="first\n")
    pipeline.second = make_log(pipeline, text="second\n")
    logs = (pipeline.first, pipeline.second)
    pipeline.forward = combine_logs(pipeline, logs)
    pipeline.reverse = combine_logs(pipeline, tuple(reversed(logs)))

    with pytest.raises(TypeError):
        " ".join(logs)

    for node, ordered in (
        (pipeline.forward, logs),
        (pipeline.reverse, tuple(reversed(logs))),
    ):
        assert shlex.split(node.rule_call.resolve()) == [
            "cat",
            *(str(log.path) for log in ordered),
            ">",
            str(node.path),
        ]
        assert node.parents == list(ordered)

    before = set(pipeline.dag.calls)
    with pytest.raises(ValueError, match="at least one log"):
        combine_logs(pipeline, ())
    assert set(pipeline.dag.calls) == before

    pipeline.finish()
    pipeline.dag.require([pipeline.forward, pipeline.reverse])
    run(pipeline.dag)
    assert pipeline.forward.path.read_text() == "first\nsecond\n"
    assert pipeline.reverse.path.read_text() == "second\nfirst\n"


@pytest.mark.parametrize("mode", ["args", "flag", "sep"])
@pytest.mark.parametrize("empty", [False, True])
def test_tuple_formats_execute_with_exact_argv(tmp_path, mode, empty):
    """A real process receives each repeated flag/path or one joined value intact."""
    import ast
    import sys

    @command
    def capture(files: tuple[Bam, ...], mode: str, interpreter: str, script: str):
        merged = outputs(Merged)
        templates = {
            "args": "{interpreter} -c {script} {merged} {files}",
            "flag": "{interpreter} -c {script} {merged} {files:flag=-f}",
            "sep": "{interpreter} -c {script} {merged} --files {files:sep=,}",
        }
        shell(templates[mode])
        return merged

    pipeline = Pipeline(DAG(tmp_path / "nodes {literal} 'quoted' $value; spaces"))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    files = () if empty else (second, first, second)
    script = "import sys;open(sys.argv[1], 'w').write(repr(sys.argv[2:]))"
    pipeline.result = capture(pipeline, files, mode, sys.executable, script)
    pipeline.finish()
    pipeline.dag.require([pipeline.result])
    run(pipeline.dag)
    paths = [str(f.path) for f in files]
    expected = paths if mode == "args" else [part for p in paths for part in ("-f", p)]
    if mode == "sep":
        expected = ["--files", ",".join(paths)]
    assert ast.literal_eval(pipeline.result.path.read_text()) == expected


@pytest.mark.parametrize(
    "spec", ["flag=", "sep=", "flag=word", "flag=-f -g", "wat=,", "flag=-f,sep=,"]
)
def test_bad_tuple_formats_fail_before_registration(tmp_path, spec):
    """Malformed tuple formats are rejected when a Shell is constructed."""

    @command
    def invalid(files: tuple[Bam, ...], spec: str):
        merged = outputs(Merged)
        shell("cat {files:" + spec + "} > {merged}")
        return merged

    pipeline = Pipeline(DAG(tmp_path))
    with pytest.raises(ValueError):
        invalid(pipeline, (), spec)
    assert not pipeline.dag.calls
