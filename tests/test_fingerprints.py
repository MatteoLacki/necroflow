from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date, datetime, time
from pathlib import Path
import shlex

import pytest

from necroflow import (
    CommandArgs,
    Constraints,
    DAG,
    Inputs,
    NamedValues,
    NodeType,
    Outputs,
    Pipeline,
    command,
    run,
    output,
)
from necroflow.planning import plan_execution
from necroflow.rules import Rule
from necroflow.fingerprints import (
    FingerprintValueError,
    canonical_bytes,
    command_ast,
    python_identity,
)


class Source(NodeType):
    filename = "source.txt"


class Result(NodeType):
    filename = "result.txt"


class Log(NodeType):
    filename = "command.log"


CALL_COUNT = 0
LAST_ARGS = None


def dynamic_command(args: CommandArgs) -> str:
    global CALL_COUNT, LAST_ARGS
    CALL_COUNT += 1
    LAST_ARGS = args
    source = shlex.quote(str(args.inputs.source))
    result = shlex.quote(str(args.outputs.result))
    return f"cp {source} {result}"


def multi_output_command(args: CommandArgs) -> str:
    global CALL_COUNT
    CALL_COUNT += 1
    left = shlex.quote(str(args.outputs.result))
    right = shlex.quote(str(args.outputs.log))
    return f"touch {left} {right}"


def shellpath_command(args: CommandArgs) -> str:
    return f"touch {shlex.quote(str(args.outputs.result))}"


def invalid_result_command(args: CommandArgs) -> str:
    return ["touch", str(args.outputs.result)]


def semantic_command_a(args: CommandArgs) -> str:
    return f"touch {args.outputs.result}"


def semantic_command_b(args: CommandArgs) -> str:
    return f"printf changed > {args.outputs.result}"


LAMBDA_COMMAND = lambda args: f"touch {args.outputs.result}"


@command(dynamic_command, threads=2)
def decorated_dynamic(source: Source, force: bool):
    result = output(Result)
    return result


def _source_rule(name: str = "source") -> Rule:
    return Rule(name, Inputs(text=str), Outputs(source=Source), "touch {source}")


def test_named_values_support_mapping_attribute_and_diagnostic_views():
    """NamedValues must preserve mapping semantics while offering readable attributes."""

    values = NamedValues({"sample": "S1", "items": "declared"})

    assert len(values) == 2
    assert list(values) == ["sample", "items"]
    assert values.sample == "S1"
    assert values["items"] == "declared"
    assert callable(values.items)
    assert repr(values) == "NamedValues(sample='S1', items='declared')"
    with pytest.raises(AttributeError, match="missing"):
        _ = values.missing


def test_command_args_are_resolved_named_immutable_views(tmp_path):
    global LAST_ARGS
    LAST_ARGS = None
    pipeline = Pipeline(DAG(tmp_path))
    source = _source_rule()(pipeline, text="x")
    rule = Rule(
        "dynamic",
        Inputs(source=Source, force=bool),
        Outputs(result=Result),
        dynamic_command,
        Constraints(threads=3),
    )
    result = rule(pipeline, source, force=True)
    realized = result.rule_call.resolve()

    assert (
        realized
        == f"cp {shlex.quote(str(source.path))} {shlex.quote(str(result.path))}"
    )
    assert LAST_ARGS.inputs.source == source.path
    assert LAST_ARGS.inputs["source"] == source.path
    assert LAST_ARGS.config.force is True
    assert LAST_ARGS.outputs.result == result.path
    assert LAST_ARGS.constraints.threads == 3
    assert LAST_ARGS.workdir == result.path.parent
    with pytest.raises((AttributeError, TypeError, FrozenInstanceError)):
        LAST_ARGS.workdir = Path("elsewhere")


def test_callable_command_is_realized_once_per_rule_call(tmp_path):
    global CALL_COUNT
    CALL_COUNT = 0
    rule = Rule(
        "multi",
        Inputs(label=str),
        Outputs(result=Result, log=Log),
        multi_output_command,
    )
    pipeline = Pipeline(DAG(tmp_path))
    outputs = rule(pipeline, label="x")
    first = outputs.result.rule_call.resolve()
    assert outputs.log.rule_call.resolve() == first
    assert CALL_COUNT == 1
    assert outputs.result.path.is_absolute()


def test_callable_stays_lazy_through_dedup_and_cached_execution(tmp_path):
    global CALL_COUNT
    CALL_COUNT = 0
    source_rule = _source_rule()
    dynamic_rule = Rule(
        "dynamic",
        Inputs(source=Source),
        Outputs(result=Result),
        dynamic_command,
    )
    dag = DAG(tmp_path)

    def build(owner: DAG = dag) -> Pipeline:
        pipeline = Pipeline(owner)
        pipeline.source = source_rule(pipeline, text="x")
        pipeline.result = dynamic_rule(pipeline, pipeline.source)
        return pipeline

    first = build()
    duplicate = build()
    first.finish()
    duplicate.finish()
    dag.require(first.sinks())
    dag.require(duplicate.sinks())

    assert CALL_COUNT == 0
    dag.run()
    assert CALL_COUNT == 1

    cached_dag = DAG(tmp_path)
    cached = build(cached_dag)
    cached.finish()
    cached_dag.require(cached.sinks())
    run(cached_dag)
    assert CALL_COUNT == 1


def test_callable_command_must_return_nonempty_string(tmp_path):
    rule = Rule(
        "invalid_result",
        Inputs(label=str),
        Outputs(result=Result),
        invalid_result_command,
    )
    result = rule(Pipeline(DAG(tmp_path)), label="x")

    with pytest.raises(TypeError, match="must return a non-empty shell string"):
        result.rule_call.resolve()


def test_lambda_command_with_unique_source_is_supported(tmp_path):
    rule = Rule(
        "lambda_command",
        Inputs(label=str),
        Outputs(result=Result),
        LAMBDA_COMMAND,
    )
    result = rule(Pipeline(DAG(tmp_path)), label="x")
    assert result.rule_call.resolve() == f"touch {result.path}"


def test_callable_command_decorator_uses_declared_rule_shape(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    source = _source_rule()(pipeline, text="x")
    result = decorated_dynamic(pipeline, source, force=False)

    assert result.rule.__name__ == "decorated_dynamic"
    assert result.rule.constraints == {"threads": 2}
    assert result.rule_call.resolve().endswith(
        f"{shlex.quote(str(source.path))} {shlex.quote(str(result.path))}"
    )


def test_closures_and_nested_callbacks_are_rejected():
    captured = "touch"

    def nested(args):
        return f"{captured} {args.outputs.result}"

    with pytest.raises(TypeError, match="must not close over values"):
        Rule(
            "closed",
            Inputs(label=str),
            Outputs(result=Result),
            nested,
        )


def test_semantic_ast_change_and_python_version_change_fingerprint(monkeypatch):
    pipeline = Pipeline(DAG("/tmp/necroflow-fingerprint-semantic"))
    first = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        semantic_command_a,
    )(pipeline, label="x")
    second = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        semantic_command_b,
    )(pipeline, label="x")

    assert first.provenance_hash != second.provenance_hash
    assert len(first.provenance_hash) == 64
    assert first.path.parent.name == first.provenance_hash
    tree, source = command_ast(semantic_command_a)
    assert "FunctionDef" in tree
    assert source == Path(__file__).resolve()
    original = first.provenance_hash
    monkeypatch.setattr(
        "necroflow.fingerprints.python_identity", lambda: python_identity() + "-other"
    )
    changed = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        semantic_command_a,
    )(pipeline, label="x")
    assert changed.provenance_hash != original


def test_framed_canonical_values_preserve_boundaries_and_order():
    assert canonical_bytes(["ab", "c"]) != canonical_bytes(["a", "bc"])
    assert canonical_bytes({"b": 2, "a": 1}) == canonical_bytes({"a": 1, "b": 2})
    assert canonical_bytes({3, 1, 2}) == canonical_bytes({2, 3, 1})


@pytest.mark.parametrize(
    "value",
    [
        None,
        1.25,
        float("nan"),
        float("inf"),
        float("-inf"),
        b"\x00payload",
        Path("relative/data.txt"),
        datetime(2026, 7, 27, 12, 34, 56),
        date(2026, 7, 27),
        time(12, 34, 56),
        ("ordered", 1),
    ],
)
def test_canonical_bytes_supports_declared_builtin_value_families(value):
    """Every documented builtin fingerprint value must encode deterministically."""

    assert canonical_bytes(value) == canonical_bytes(value)


def test_canonical_bytes_rejects_non_string_mapping_keys_with_value_path():
    """Invalid nested mapping keys must identify their location in job config."""

    with pytest.raises(
        FingerprintValueError,
        match=r"job\.options: fingerprint mappings require string keys, got int",
    ):
        canonical_bytes({"options": {1: "invalid"}}, path="job")


def test_command_callback_must_be_a_source_inspectable_function():
    """Callable objects cannot hide command identity in instance state."""

    class StatefulCommand:
        def __call__(self, args):
            return f"touch {args.outputs.result}"

    with pytest.raises(TypeError, match="source-inspectable functions or lambdas"):
        Rule(
            "stateful",
            Inputs(label=str),
            Outputs(result=Result),
            StatefulCommand(),
        )


def test_nested_command_without_captured_values_is_rejected():
    """A command callback must remain importable at module scope."""

    def nested(args):
        return f"touch {args.outputs.result}"

    with pytest.raises(TypeError, match="must be defined at module scope"):
        Rule("nested", Inputs(label=str), Outputs(result=Result), nested)


@pytest.mark.parametrize(
    "callback",
    [
        lambda: "touch ignored",
        lambda first, second: f"touch {first.outputs.result} {second}",
        lambda *args: "touch ignored",
    ],
)
def test_command_callback_requires_one_positional_argument(callback):
    """Command callbacks must accept exactly one positional CommandArgs value."""

    with pytest.raises(TypeError, match="exactly one positional CommandArgs argument"):
        Rule("wrong_arity", Inputs(label=str), Outputs(result=Result), callback)


def test_generated_command_callback_without_source_is_rejected():
    """Generated functions cannot provide reproducible source-based identity."""

    namespace = {"__name__": "generated_command_test"}
    exec(
        compile("def build(args):\n    return 'touch output'\n", "<generated>", "exec"),
        namespace,
    )

    with pytest.raises(TypeError, match="has no inspectable source"):
        Rule(
            "generated",
            Inputs(label=str),
            Outputs(result=Result),
            namespace["build"],
        )


def test_multiple_lambdas_on_one_source_line_are_rejected(tmp_path):
    """A lambda command must map to exactly one AST node on its source line."""

    from necroflow.config import load_callable

    callbacks = tmp_path / "ambiguous.py"
    callbacks.write_text(
        "first, second = lambda args: 'first', lambda args: 'second'\n"
    )
    callback = load_callable(f"{callbacks}:first", kind="test-command")

    with pytest.raises(TypeError, match="is ambiguous"):
        Rule("ambiguous", Inputs(label=str), Outputs(result=Result), callback)


def test_ast_formatting_and_comments_do_not_change_identity(tmp_path):
    from necroflow.config import load_callable

    pipeline = Pipeline(DAG(tmp_path))
    compact = tmp_path / "compact.py"
    commented = tmp_path / "commented.py"
    compact.write_text(
        "def build(args):\n" "    return f'touch {args.outputs.result}'\n"
    )
    commented.write_text(
        "def build(args):  # formatting-only comment\n"
        "\n"
        "    # another comment\n"
        "    return f'touch {args.outputs.result}'\n"
    )
    first_callback = load_callable(f"{compact}:build", kind="test-command")
    second_callback = load_callable(f"{commented}:build", kind="test-command")
    first = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        first_callback,
    )(pipeline, label="x")
    second = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        second_callback,
    )(pipeline, label="x")

    assert first.provenance_hash == second.provenance_hash


def test_framework_hashing_rejects_custom_config_values(tmp_path):
    class Options:
        pass

    with pytest.raises(FingerprintValueError, match="config.options"):
        Rule(
            "custom_config",
            Inputs(options=Options),
            Outputs(result=Result),
            "touch {result}",
        )(Pipeline(DAG(tmp_path)), options=Options())


def test_v3_split_hash_cache_is_not_reused(tmp_path):
    old_output = tmp_path / "source" / ("a" * 64) / ("b" * 64) / "source.txt"
    old_output.parent.mkdir(parents=True)
    old_output.touch()
    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)
    node = _source_rule()(pipeline, text="x")
    dag.require([node])

    plan_execution(dag)

    assert len(node.path.relative_to(tmp_path).parts) == 3
    assert node.rule_call.state.value == "missing"


def test_constraints_and_repeat_remain_outside_framework_hashes(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    first = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        "touch {result}",
        Constraints(threads=1),
        repeat=1,
    )(pipeline, label="x")
    second = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        "touch {result}",
        Constraints(threads=8),
        repeat=4,
    )(pipeline, label="x")

    assert first.provenance_hash == second.provenance_hash


def test_explicit_shellpath_changes_callable_fingerprint(tmp_path):
    rule = Rule(
        "shellpath",
        Inputs(label=str),
        Outputs(result=Result),
        shellpath_command,
    )
    default_pipeline = Pipeline(DAG(tmp_path / "default"))
    explicit_pipeline = Pipeline(DAG(tmp_path / "explicit"), shellpath="/bin/bash")
    default_result = rule(default_pipeline, label="x")
    explicit_result = rule(explicit_pipeline, label="x")
    default_digest = default_result.provenance_hash

    assert explicit_result.provenance_hash != default_digest
    expected_shell = str(Path("/bin/bash").resolve())
    assert explicit_result.rule_call.shellpath == expected_shell


def test_explicit_shellpath_does_not_change_builtin_materializer_fingerprint(tmp_path):
    from necroflow import text_file_rule

    write_text = text_file_rule("write_text", Result)
    default = Pipeline(DAG(tmp_path / "default"))
    explicit = Pipeline(DAG(tmp_path / "explicit"), shellpath="/bin/sh")

    assert (
        write_text(default, text="same").provenance_hash
        == write_text(explicit, text="same").provenance_hash
    )


def test_command_factory_rejects_argv_lists():
    with pytest.raises(TypeError, match="argv list commands are unsupported"):
        command(
            ["touch", "{result}"],
            Inputs(label=str),
            Outputs(result=Result),
            name="argv",
        )


def test_callable_provenance_separates_command_and_fingerprint(tmp_path):
    pipeline = Pipeline(DAG(tmp_path))
    source = _source_rule()(pipeline, text="x")
    pipeline.source = source
    pipeline.result = Rule(
        "dynamic",
        Inputs(source=Source, force=bool),
        Outputs(result=Result),
        dynamic_command,
    )(pipeline, source, force=False)

    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)

    metadata = (pipeline.result.path.parent / ".rip" / "dependencies.toml").read_text()
    assert "[identity]" in metadata
    assert 'format = "v4"' in metadata
    assert f'rule_hash = "{pipeline.result.rule_hash}"' in metadata
    assert f'provenance_hash = "{pipeline.result.provenance_hash}"' in metadata
    assert "[command]" in metadata
    assert 'kind = "python"' in metadata
    assert "realized = " in metadata
    assert "source = " in metadata
    assert python_identity() in metadata
    pipeline = Pipeline(DAG(tmp_path))
