"""Inline declarations preserve identity and defer command execution."""

from necroflow import Shell

import importlib.util
import shlex
import sys
from pathlib import Path

import pytest
import tomlkit

from necroflow import DAG, Pipeline, Node, Resource, command, outputs, run
from necroflow.rules import Command, Inputs, Outputs, Constraints


class Result(Node):
    filename = "result.txt"


@command
def inline(text: str = "hello", threads: Resource[int] = 2):
    result = outputs(Result)
    Shell("printf %s {text} > {result}")
    return result


@command(repeat=2)
def nested(text: str = "hello", threads: Resource[int] = 2):
    result = outputs(Result)

    def build_command():
        return f"printf %s {shlex.quote(text)} > {shlex.quote(str(result.path))}"

    Shell(build_command())

    return result


def load_rule(tmp_path, body, name="definition"):
    """Load genuine source so declaration source inspection is exercised."""
    path = tmp_path / f"{name}.py"
    path.write_text(
        "from necroflow import command, outputs, Resource, Shell\n"
        "from test_inline_commands import Result\n" + body
    )
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_executable_recipe_has_distinct_identity(tmp_path):
    """Executing declaration bodies has its own recipe format, distinct from templates."""
    legacy = Command(
        "inline",
        Inputs(text=str),
        Outputs(result=Result),
        "printf %s {text} > {result}",
        Constraints(threads=19),
        input_defaults={"text": "hello"},
    )
    a = inline(Pipeline(DAG(tmp_path / "a")), threads=4)
    b = legacy(Pipeline(DAG(tmp_path / "b")))
    assert a.rule_hash != b.rule_hash
    assert a.provenance_hash != b.provenance_hash
    assert a.config == {"text": "hello"}
    assert a.rule_call.resources == {"threads": 4}
    assert inline.constraints == {"threads": 2}


@pytest.mark.parametrize("rule", [inline, nested])
def test_commands_execute_with_quoting_and_reuse(tmp_path, rule):
    """Strings and nested builders safely handle values and paths with spaces."""
    pipeline = Pipeline(DAG(tmp_path / "nodes with spaces"))
    pipeline.result = rule(pipeline, text="hello '; $stuff")
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    assert not pipeline.result.path.exists()
    run(pipeline.dag)
    assert pipeline.result.path.read_text() == "hello '; $stuff"
    metadata = pipeline.result.rule_call.metadata_dir / "dependencies.toml"
    before = metadata.read_bytes()
    run(pipeline.dag)
    assert metadata.read_bytes() == before
    assert tomlkit.parse(before.decode())["resources"]["threads"] == 2


def test_resource_overrides_dedupe_and_cache(tmp_path):
    """File policy resolves call conflicts without changing identity or cached metadata."""
    dag = DAG(tmp_path / "nodes", resources={"inline": {"threads": 3}})
    pipeline = Pipeline(dag)
    pipeline.a = inline(pipeline, threads=4)
    pipeline.b = inline(pipeline, threads=8)
    assert pipeline.a is pipeline.b
    assert pipeline.a.rule_call.resources == {"threads": 3}
    pipeline.finish()
    dag.require(pipeline.sinks())
    run(dag)
    metadata = pipeline.a.rule_call.metadata_dir / "dependencies.toml"
    before = metadata.read_bytes()
    other = Pipeline(DAG(tmp_path / "nodes", resources={"inline": {"threads": 7}}))
    other.result = inline(other)
    other.finish()
    other.dag.require(other.sinks())
    run(other.dag)
    assert other.result.relative_path == pipeline.a.relative_path
    assert metadata.read_bytes() == before


def test_new_resource_conflicts_fail(tmp_path):
    """Identical new-style calls cannot silently use an arbitrary resource request."""
    pipeline = Pipeline(DAG(tmp_path))
    inline(pipeline, threads=3)
    with pytest.raises(ValueError, match="conflicting resource requests"):
        inline(pipeline, threads=4)


@pytest.mark.parametrize(
    "body",
    [
        '    command = "touch {result}"\n    result = outputs(Result)\n    return result\n',
        '    result = outputs(Result)\n    command = f"touch {result.path}"\n    return result\n',
        '    result = outputs(Result)\n    command = "touch {result}"\n    command = "echo no"\n    return result\n',
        "    result = outputs(Result)\n    return result\n",
        '    result = outputs(Result)\n    def command(x):\n        return "true"\n    return result\n',
        '    result = outputs(Result)\n    async def command():\n        return "true"\n    return result\n',
    ],
)
def test_invalid_inline_declarations_rejected(tmp_path, body):
    """Malformed inline declarations fail at import, without running their bodies."""
    with pytest.raises((ValueError, TypeError)):
        rule = load_rule(tmp_path, "@command\ndef bad():\n" + body).bad
        rule(Pipeline(DAG(tmp_path)))


def test_resource_types_and_defaults_checked(tmp_path):
    """Only positive integer resource quantities with defaults are accepted."""
    for annotation, default in [("str", "'x'"), ("int", "0"), ("int", "True")]:
        with pytest.raises((TypeError, ValueError)):
            load_rule(
                tmp_path,
                f"@command\ndef bad(threads: Resource[{annotation}] = {default}):\n"
                '    result = outputs(Result)\n    command = "touch {result}"\n    return result\n',
            )
    with pytest.raises(TypeError, match="needs a default"):
        load_rule(
            tmp_path,
            'from necroflow import Shell\n@command\ndef bad(threads: Resource[int]):\n    result = outputs(Result)\n    Shell("touch {result}")\n    return result\n',
        )


def test_nested_resource_binding_and_all_cap(tmp_path):
    """Bodies see final resource overrides; execution caps cannot replay their branches."""

    @command
    def build(label: str, threads: Resource[int] = 1):
        result = outputs(Result)

        def build_command():
            return f"echo {threads} {label} > {result.path}"

        Shell(build_command())

        return result

    pipeline = Pipeline(DAG(tmp_path, resources={"build": {"threads": 3}}))
    a = build(pipeline, label="a").rule_call
    b = build(pipeline, label="b").rule_call
    assert "echo 3 a" in a.resolve()
    assert "echo 3 b" in b.resolve()
    pipeline.dag.bind_thread_cap(7)
    assert "echo 3 a" in a.resolve()
    unresolved = Pipeline(
        DAG(tmp_path / "all", resources={"build": {"threads": "all"}})
    )
    with pytest.raises(ValueError, match="concrete threads"):
        build(unresolved, label="a")
    assert not unresolved.dag.calls


def test_nested_undeclared_closure_rejected():
    """Nested builders cannot hide arbitrary outer closure state from identity."""
    hidden = "secret"
    with pytest.raises(TypeError, match="undeclared"):

        @command
        def bad():
            result = outputs(Result)

            def build_command():
                return f"echo {hidden} > {result.path}"

            Shell(build_command())

            return result


def test_nested_inputs_outputs_and_failure_preflight(tmp_path):
    """Builders get resolved variadic/mixed paths and fail before invoking a runner."""

    @command
    def combine(
        sources: tuple[Result, ...], optional: Result | str | None, label: str = "x"
    ):
        result = outputs(Result)

        def build_command():
            assert all(isinstance(path, Result) for path in sources)
            assert optional is None
            return (
                "cat "
                + " ".join(shlex.quote(str(p.path)) for p in sources)
                + " > "
                + shlex.quote(str(result.path))
            )

        Shell(build_command())

        return result

    pipeline = Pipeline(DAG(tmp_path))
    a = inline(pipeline, text="a")
    b = inline(pipeline, text="b")
    pipeline.result = combine(pipeline, (a, b), None)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert pipeline.result.path.read_text() == "ab"

    @command
    def invalid():
        result = outputs(Result)

        def build_command():
            return None

        Shell(build_command())

        return result

    other = Pipeline(DAG(tmp_path / "invalid"))
    with pytest.raises(TypeError, match="non-empty shell string"):
        invalid(other)
    assert not other.dag.calls
    assert not other.dag.nodes_dir.exists()


def callback_with_command_local(args):
    command = f"touch {args.outputs.result}"
    return command


def test_legacy_callback_with_command_local(tmp_path):
    """A callback's local command variable must not dispatch as an inline declaration."""

    @command(callback_with_command_local)
    def legacy():
        result = outputs(Result)
        return result

    node = legacy(Pipeline(DAG(tmp_path)))
    assert node.rule_call.resolve() == f"touch {node.path}"


def test_nested_recipe_hash_and_resource_default_independence(tmp_path):
    """Nested source/config edits alter identity; resource defaults do not."""
    body = 'from necroflow import Shell\n@command\ndef build(label: str = "a", threads: Resource[int] = DEFAULT):\n    result = outputs(Result)\n    def build_command():\n        return f"echo {label} > {result.path}"\n    Shell(build_command())\n    return result\n'
    a = load_rule(tmp_path, body.replace("DEFAULT", "2"), "version_a").build
    b = load_rule(tmp_path, body.replace("DEFAULT", "7"), "version_b").build
    c = load_rule(
        tmp_path,
        body.replace("DEFAULT", "2").replace("echo {label}", "printf {label}"),
        "version_c",
    ).build
    na = a(Pipeline(DAG(tmp_path / "a")))
    nb = b(Pipeline(DAG(tmp_path / "b")))
    nc = c(Pipeline(DAG(tmp_path / "c")))
    assert (na.rule_hash, na.provenance_hash) == (nb.rule_hash, nb.provenance_hash)
    assert nc.rule_hash != na.rule_hash
    changed = a(Pipeline(DAG(tmp_path / "d")), label="changed")
    assert changed.rule_hash == na.rule_hash
    assert changed.provenance_hash != na.provenance_hash


def test_scheduler_sees_overridden_resources(tmp_path):
    """Scheduling, builder bindings, and saved execution resources agree."""
    from necroflow import fifo_scheduler

    @command
    def allocated(label: str, threads: Resource[int] = 1, ram: Resource[int] = 100):
        result = outputs(Result)
        Shell("echo {threads} {ram} > {result}")
        return result

    pipeline = Pipeline(
        DAG(tmp_path, resources={"allocated": {"threads": 2, "ram": "2Ki"}})
    )
    pipeline.a = allocated(pipeline, label="a")
    pipeline.b = allocated(pipeline, label="b")
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    seen = []

    def scheduler(ready, remaining, available):
        seen.extend(call.resources for call in ready)
        return fifo_scheduler(ready, remaining, available)

    run(pipeline.dag, scheduler=scheduler, resource_caps={"threads": 2, "ram": 2048})
    assert seen and all(r == {"threads": 2, "ram": 2048} for r in seen)
    assert pipeline.a.path.read_text().strip() == "2 2048"
    assert pipeline.b.path.read_text().strip() == "2 2048"


def test_same_named_resource_contracts_rejected(tmp_path):
    """A file table cannot silently target incompatible declarations sharing a name."""
    dag = DAG(tmp_path, resources={"same": {"threads": 2}})
    a = Command(
        "same",
        Inputs(),
        Outputs(result=Result),
        "touch {result}",
        Constraints(threads=1),
    )
    b = Command(
        "same",
        Inputs(),
        Outputs(result=Result),
        "echo hi > {result}",
        Constraints(threads=1, ram=3),
    )
    a(Pipeline(dag))
    with pytest.raises(ValueError, match="incompatible resource declarations"):
        b(Pipeline(dag))


def test_conditional_outputs_fail_at_declaration(tmp_path):
    """Bare decorator dispatch still rejects outputs hidden in conditionals."""
    with pytest.raises(ValueError, match="output"):
        load_rule(
            tmp_path,
            'from necroflow import Shell\n@command\ndef bad(enabled: bool):\n    if enabled:\n        result = outputs(Result)\n    Shell("touch {result}")\n    return result\n',
        )


def test_nested_docker_and_multioutput(tmp_path):
    """Nested builders preserve Docker-prefix selection and named output returns."""
    from necroflow import Docker

    class Other(Node):
        filename = "other.txt"

    @command
    def container(env: Docker, threads: Resource[int] = 2):
        result = outputs(Result)
        other = outputs(Other)

        def build_command():
            return f"{{env}}:echo {threads} > {result.path}; touch {other.path}"

        Shell(build_command())

        return result, other

    env = Docker("registry.example/tool@sha256:" + "a" * 64, platform="linux/amd64")
    pipeline = Pipeline(DAG(tmp_path, resources={"container": {"threads": 3}}))
    nodes = container(pipeline, env=env)
    assert nodes.result.rule_call is nodes.other.rule_call
    call = nodes.result.rule_call
    assert call.container_input == "env"
    assert call.container == env
    assert call.resolve() == f"echo 3 > {nodes.result.path}; touch {nodes.other.path}"


def test_builder_runs_only_at_realization_and_retries(tmp_path):
    """Declaration/interning never run builders; retry policy stays independent."""

    @command(repeat=2)
    def retry():
        result = outputs(Result)

        def build_command():
            marker = result.path.parent / "attempted"
            return f"test -e {marker} && echo done > {result.path} || (touch {marker}; exit 1)"

        Shell(build_command())

        return result

    pipeline = Pipeline(DAG(tmp_path))
    pipeline.result = retry(pipeline)
    assert pipeline.result.rule_call._realized is None
    assert retry(pipeline) is pipeline.result
    assert pipeline.result.rule_call._realized is None
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert pipeline.result.path.read_text().strip() == "done"


@command
def returned_template(text: str, threads: Resource[int] = 2):
    result = outputs(Result)

    def build_command():
        options = "{text}" if threads > 1 else "single"
        return f"printf %s {options} > {{result}}"

    Shell(build_command())

    return result


def external_template(args):
    return "printf %s {text} > {result}"


def test_callable_templates_quote_values_once(tmp_path):
    """Both callable forms quote placeholders, including values containing braces."""
    text = "spaces {literal} $HOME; 'quoted'"
    external = Command(
        "external_template", Inputs(text=str), Outputs(result=Result), external_template
    )
    pipeline = Pipeline(DAG(tmp_path / "nodes {literal} with spaces"))
    pipeline.nested = returned_template(pipeline, text)
    pipeline.external = external(pipeline, text=text)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert pipeline.nested.path.read_text() == text
    assert pipeline.external.path.read_text() == text


def test_callable_template_literal_shell_braces(tmp_path):
    """F-string braces preserve placeholders; doubled template braces stay literal."""

    @command
    def shell_braces(text: str):
        result = outputs(Result)

        def build_command():
            program = "awk"
            return (
                f"printf '%s\\n' {{text}} | {program} '{{{{print $1}}}}' > {{result}}"
            )

        Shell(build_command())

        return result

    pipeline = Pipeline(DAG(tmp_path))
    pipeline.result = shell_braces(pipeline, "first second")
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    run(pipeline.dag)
    assert pipeline.result.path.read_text() == "first\n"


def test_callable_template_unknown_placeholder(tmp_path):
    """Invalid returned placeholders fail during realization, before execution."""

    @command
    def typo():
        result = outputs(Result)

        def build_command():
            return "touch {reslt}"

        Shell(build_command())

        return result

    pipeline = Pipeline(DAG(tmp_path))
    with pytest.raises(ValueError, match="unknown placeholders.*reslt"):
        typo(pipeline)
    assert not pipeline.dag.calls
