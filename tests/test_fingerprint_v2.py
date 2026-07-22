from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
import shlex

import pytest

from necroflow import (
    CommandArgs,
    Constraints,
    DAG,
    FingerprintArgs,
    Inputs,
    NodeType,
    Outputs,
    Pipeline,
    command,
    default_fingerprint,
    execute,
    output,
    resolve_command,
)
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


def constant_fingerprint(args: FingerprintArgs) -> str:
    return "a" * 64


def composed_fingerprint(args: FingerprintArgs) -> str:
    digest = default_fingerprint(args)
    return digest[:-1] + ("0" if digest[-1] != "0" else "1")


def invalid_fingerprint(args: FingerprintArgs) -> str:
    return "not-a-digest"


def _source_rule(name: str = "source") -> Rule:
    return Rule(name, Inputs(text=str), Outputs(source=Source), "touch {source}")


def test_command_args_are_resolved_named_immutable_views(tmp_path):
    global LAST_ARGS
    LAST_ARGS = None
    pipeline = Pipeline(tmp_path)
    source = _source_rule()(pipeline, text="x")
    rule = Rule(
        "dynamic",
        Inputs(source=Source, force=bool),
        Outputs(result=Result),
        dynamic_command,
        Constraints(threads=3),
    )
    result = rule(pipeline, source, force=True)
    realized = resolve_command(result)

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
    pipeline = Pipeline(tmp_path)
    outputs = rule(pipeline, label="x")
    first = resolve_command(outputs.result)
    assert resolve_command(outputs.log) == first
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

    def build() -> Pipeline:
        pipeline = Pipeline(tmp_path)
        pipeline.source = source_rule(pipeline, text="x")
        pipeline.result = dynamic_rule(pipeline, pipeline.source)
        return pipeline

    first = build()
    duplicate = build()
    dag = DAG(tmp_path)
    dag.add(first)
    dag.add(duplicate)

    assert CALL_COUNT == 0
    dag.execute()
    assert CALL_COUNT == 1

    cached = build()
    execute(cached)
    assert CALL_COUNT == 1


def test_callable_command_must_return_nonempty_string(tmp_path):
    rule = Rule(
        "invalid_result",
        Inputs(label=str),
        Outputs(result=Result),
        invalid_result_command,
    )
    result = rule(Pipeline(tmp_path), label="x")

    with pytest.raises(TypeError, match="must return a non-empty shell string"):
        resolve_command(result)


def test_lambda_command_with_unique_source_is_supported(tmp_path):
    rule = Rule(
        "lambda_command",
        Inputs(label=str),
        Outputs(result=Result),
        LAMBDA_COMMAND,
    )
    result = rule(Pipeline(tmp_path), label="x")
    assert resolve_command(result) == f"touch {result.path}"


def test_callable_command_decorator_uses_declared_rule_shape(tmp_path):
    pipeline = Pipeline(tmp_path)
    source = _source_rule()(pipeline, text="x")
    result = decorated_dynamic(pipeline, source, force=False)

    assert result.rule.__name__ == "decorated_dynamic"
    assert result.rule.constraints == {"threads": 2}
    assert resolve_command(result).endswith(
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
    pipeline = Pipeline("/tmp/necroflow-fingerprint-semantic")
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

    assert first.full_fingerprint != second.full_fingerprint
    assert len(first.full_fingerprint) == 64
    assert first.fingerprint == first.full_fingerprint[:16]
    tree, source = command_ast(semantic_command_a)
    assert "FunctionDef" in tree
    assert source == Path(__file__).resolve()
    original = first.full_fingerprint
    monkeypatch.setattr(
        "necroflow.fingerprints.python_identity", lambda: python_identity() + "-other"
    )
    changed = Rule(
        "same",
        Inputs(label=str),
        Outputs(result=Result),
        semantic_command_a,
    )(pipeline, label="x")
    assert changed.full_fingerprint != original


def test_framed_canonical_values_preserve_boundaries_and_order():
    assert canonical_bytes(["ab", "c"]) != canonical_bytes(["a", "bc"])
    assert canonical_bytes({"b": 2, "a": 1}) == canonical_bytes({"a": 1, "b": 2})
    assert canonical_bytes({3, 1, 2}) == canonical_bytes({2, 3, 1})


def test_ast_formatting_and_comments_do_not_change_identity(tmp_path):
    from necroflow.config import load_callable

    pipeline = Pipeline(tmp_path)
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

    assert first.full_fingerprint == second.full_fingerprint


def test_default_fingerprint_rejects_custom_config_values(tmp_path):
    class Options:
        pass

    with pytest.raises(FingerprintValueError, match="config.options"):
        Rule(
            "custom_config",
            Inputs(options=Options),
            Outputs(result=Result),
            "touch {result}",
        )(Pipeline(tmp_path), options=Options())


def test_project_fingerprint_replaces_default_and_can_handle_custom_values(tmp_path):
    class Options:
        pass

    pipeline = Pipeline(
        tmp_path,
        fingerprint_function=constant_fingerprint,
        fingerprint_provider="test:constant",
    )
    pipeline.result = Rule(
        "custom_config",
        Inputs(options=Options),
        Outputs(result=Result),
        "touch {result}",
    )(pipeline, options=Options())

    assert pipeline.result.full_fingerprint == "a" * 64
    assert pipeline.result.rule_call.fingerprint_provider == "test:constant"


def test_project_fingerprint_is_installed_recursively_and_can_compose(tmp_path):
    pipeline = Pipeline(
        tmp_path,
        fingerprint_function=composed_fingerprint,
        fingerprint_provider="test:composed",
    )
    source = _source_rule()(pipeline, text="x")
    result = Rule(
        "consume",
        Inputs(source=Source),
        Outputs(result=Result),
        "cp {source} {result}",
    )(pipeline, source)
    pipeline.result = result

    assert result.rule_call.fingerprint_provider == "test:composed"
    assert source.rule_call.fingerprint_provider == "test:composed"
    assert result.full_fingerprint != default_fingerprint(
        result.rule_call.fingerprint_args()
    )


def test_invalid_project_fingerprint_result_fails_during_rule_call(tmp_path):
    pipeline = Pipeline(
        tmp_path,
        fingerprint_function=invalid_fingerprint,
        fingerprint_provider="test:invalid",
    )
    with pytest.raises(TypeError, match="64 lowercase hexadecimal"):
        _source_rule()(pipeline, text="x")


def test_constraints_and_repeat_remain_outside_default_fingerprint(tmp_path):
    pipeline = Pipeline(tmp_path)
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

    assert first.full_fingerprint == second.full_fingerprint


def test_explicit_shellpath_changes_callable_fingerprint(tmp_path):
    rule = Rule(
        "shellpath",
        Inputs(label=str),
        Outputs(result=Result),
        shellpath_command,
    )
    default_pipeline = Pipeline(tmp_path / "default")
    explicit_pipeline = Pipeline(tmp_path / "explicit", shellpath="/bin/bash")
    default_result = rule(default_pipeline, label="x")
    explicit_result = rule(explicit_pipeline, label="x")
    default_digest = default_result.full_fingerprint

    assert explicit_result.full_fingerprint != default_digest
    assert explicit_result.rule_call.execution_context["shellpath"] == str(
        Path("/bin/bash").resolve()
    )


def test_explicit_shellpath_does_not_change_builtin_materializer_fingerprint(tmp_path):
    from necroflow import text_file_rule

    write_text = text_file_rule("write_text", Result)
    default = Pipeline(tmp_path / "default")
    explicit = Pipeline(tmp_path / "explicit", shellpath="/bin/sh")

    assert (
        write_text(default, text="same").fingerprint
        == write_text(explicit, text="same").fingerprint
    )


def test_command_factory_rejects_argv_lists():
    with pytest.raises(TypeError, match="argv list commands were removed"):
        command(
            ["touch", "{result}"],
            Inputs(label=str),
            Outputs(result=Result),
            name="argv",
        )


def test_callable_provenance_separates_command_and_fingerprint(tmp_path):
    pipeline = Pipeline(tmp_path)
    source = _source_rule()(pipeline, text="x")
    pipeline.source = source
    pipeline.result = Rule(
        "dynamic",
        Inputs(source=Source, force=bool),
        Outputs(result=Result),
        dynamic_command,
    )(pipeline, source, force=False)

    execute(pipeline)

    metadata = (pipeline.result.path.parent / ".rip" / "dependencies.toml").read_text()
    assert "[fingerprint]" in metadata
    assert f'digest = "{pipeline.result.full_fingerprint}"' in metadata
    assert 'provider = "necroflow.default_fingerprint/v2"' in metadata
    assert "[command]" in metadata
    assert 'kind = "python"' in metadata
    assert "realized = " in metadata
    assert "source = " in metadata
    assert python_identity() in metadata
    pipeline = Pipeline(tmp_path)
