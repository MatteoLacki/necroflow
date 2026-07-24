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
    NodeType,
    output,
    Outputs,
    Pipeline,
)
from necroflow.dag import resolve_command
from necroflow.rules import Rule


class Bam(NodeType):
    filename = "input.bam"


class OtherBam(NodeType):
    filename = "other.bam"


class Reference(NodeType):
    filename = "reference.fa"


class Merged(NodeType):
    filename = "merged.bam"


@command("merge {bams} > {merged}")
def decorated_merge(bams: Annotated[tuple[Bam, ...], Many()]):
    merged = output(Merged)
    return merged


LAST_COMMAND_ARGS: CommandArgs | None = None


def variadic_command(args: CommandArgs) -> str:
    global LAST_COMMAND_ARGS
    LAST_COMMAND_ARGS = args
    inputs = " ".join(shlex.quote(str(path)) for path in args.inputs.bams)
    return f"merge {inputs} > {shlex.quote(str(args.outputs.merged))}"


def _source(pipeline: Pipeline, label: str, node_type: type[NodeType] = Bam):
    return Rule(
        "source",
        Inputs(label=str),
        Outputs(source=node_type),
        "touch {source}",
    )(pipeline, label=label)


def test_plain_variadic_tuple_accepts_zero_or_more_nodes(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    merge = Rule(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    result = merge(pipeline, (first, second))
    empty = merge(pipeline, ())

    assert result.rule_call.inputs.bams == (first, second)
    assert result.parents == [first, second]
    assert resolve_command(result) == (
        f"merge {shlex.quote(str(first.path))} {shlex.quote(str(second.path))} "
        f"> {result.path}"
    )
    assert empty.parents == []
    assert resolve_command(empty) == f"merge  > {empty.path}"


def test_decorated_rule_accepts_variadic_tuple(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")

    result = decorated_merge(pipeline, (first,))

    assert result.rule_call.inputs.bams == (first,)
    assert result.parents == [first]


def test_variadic_static_command_quotes_each_path_independently(tmp_path):
    pipeline = Pipeline(DAG(tmp_path / "root with spaces"))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    merge = Rule(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )

    result = merge(pipeline, (first, second))

    assert resolve_command(result) == (
        f"merge {shlex.quote(str(first.path))} {shlex.quote(str(second.path))} "
        f"> {shlex.quote(str(result.path))}"
    )


def test_callable_command_receives_tuple_of_resolved_paths(tmp_path):
    global LAST_COMMAND_ARGS
    LAST_COMMAND_ARGS = None
    pipeline = Pipeline(DAG(tmp_path))
    first = _source(pipeline, "first")
    second = _source(pipeline, "second")
    merge = Rule(
        "merge_callback",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        variadic_command,
    )

    result = merge(pipeline, (first, second))
    resolve_command(result)

    assert LAST_COMMAND_ARGS is not None
    assert LAST_COMMAND_ARGS.inputs.bams == (first.path, second.path)
    assert isinstance(LAST_COMMAND_ARGS.inputs.bams, tuple)


def test_many_default_and_inclusive_bounds(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    nodes = tuple(_source(pipeline, str(index)) for index in range(3))
    at_least_one = Rule(
        "at_least_one",
        Inputs(bams=Annotated[tuple[Bam, ...], Many()]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    bounded = Rule(
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
    merge = Rule(
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
    accepted_union = Rule(
        "accepted_union",
        Inputs(bams=tuple[Union[Bam, OtherBam], ...]),
        Outputs(merged=Merged),
        "touch {merged}",
    )
    assert accepted_union is not None
    with pytest.raises(TypeError, match="tuple element union"):
        Rule(
            "mixed",
            Inputs(bams=tuple[Bam | str, ...]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="fixed-length Node tuple"):
        Rule(
            "fixed",
            Inputs(bams=tuple[Bam, Bam]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="element type is not a NodeType"):
        Rule(
            "strings",
            Inputs(values=Annotated[tuple[str, ...], Many()]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="non-variadic Node tuple"):
        Rule(
            "scalar",
            Inputs(bam=Annotated[Bam, Many()]),
            Outputs(merged=Merged),
            "touch {merged}",
        )
    with pytest.raises(TypeError, match="more than one Many"):
        Rule(
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
    rule = Rule(
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
    merge = Rule(
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
    plain = Rule(
        "merge",
        Inputs(bams=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    at_least_one = Rule(
        "merge",
        Inputs(bams=Annotated[tuple[Bam, ...], Many()]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    at_least_two = Rule(
        "merge",
        Inputs(bams=Annotated[tuple[Bam, ...], Many(min=2)]),
        Outputs(merged=Merged),
        "merge {bams} > {merged}",
    )
    grouped = Rule(
        "grouped",
        Inputs(left=tuple[Bam, ...], right=tuple[Bam, ...]),
        Outputs(merged=Merged),
        "touch {merged}",
    )

    forward = plain(pipeline, (first, second))
    duplicate = plain(pipeline, (first, second))
    reverse = plain(pipeline, (second, first))

    assert duplicate is forward
    assert reverse.fingerprint != forward.fingerprint
    assert at_least_one(pipeline, (first, second)).fingerprint != forward.fingerprint
    assert (
        at_least_two(pipeline, (first, second)).fingerprint
        != at_least_one(pipeline, (first, second)).fingerprint
    )
    assert (
        grouped(pipeline, (first,), (second, third)).fingerprint
        != grouped(pipeline, (first, second), (third,)).fingerprint
    )


def test_fixed_input_fingerprint_wire_shape_is_unchanged(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    source = Rule("source", Inputs(text=str), Outputs(source=Bam), "touch {source}")(
        pipeline, text="x"
    )
    consumed = Rule(
        "consume", Inputs(source=Bam), Outputs(result=Merged), "cp {source} {result}"
    )(pipeline, source)

    assert (
        source.fingerprint
        == "e882c24a2498894ad925fca9f3be2d00de73df96ab498dcc2747254f1f99814c"
    )
    assert (
        consumed.fingerprint
        == "3df1a418360cf70b1b6f5656f9f3fd4a2429a01b881445423619f2676105b88b"
    )
