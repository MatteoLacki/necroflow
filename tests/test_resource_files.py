"""Execution-resource files apply uniformly without changing data identity."""

import json

import pytest
import tomlkit

from necroflow import DAG, Pipeline, run
from necroflow.cli import main, _build_parser, _build_dag_from_jobs
from test_inline_commands import inline


@pytest.fixture
def job(tmp_path):
    factory = tmp_path / "workflow.py"
    factory.write_text("""from necroflow import command, output, Resource, Node
class Result(Node):
    filename = "result.txt"
@command
def make(threads: Resource[int] = 1):
    result = output(Result)
    command = "echo {threads} > {result}"
    return result

def build(P, config):
    assert "resources" not in config
    P.result = make(P)
""")
    job = tmp_path / "job.toml"
    job.write_text(f'".pipeline" = "{factory}:build"\n')
    return job


def args(tmp_path, job, resources, command):
    return [
        command,
        "--resources",
        str(resources),
        "--nodes-dir",
        str(tmp_path / "nodes"),
        "--results-dir",
        str(tmp_path / "results"),
        "-c",
        "3",
        str(job),
    ]


@pytest.mark.parametrize("subcommand", ["graph", "explain", "outputs", "doctor"])
def test_inspection_commands_accept_resource_files(tmp_path, job, capsys, subcommand):
    """All inspection paths compile with the same override/cap policy as execution."""
    resources = tmp_path / "resources.toml"
    resources.write_text('[make]\nthreads = "all"\n')
    main([*args(tmp_path, job, resources, subcommand), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert not list((tmp_path / "nodes").glob("*/*/result.txt"))
    if subcommand == "graph":
        assert payload["nodes"][0]["resources"] == {"threads": 3}
    elif subcommand == "explain":
        assert "echo 3" in payload["calls"][0]["command"]
    parsed = _build_parser().parse_args(args(tmp_path, job, resources, subcommand))
    dag, _, _ = _build_dag_from_jobs(parsed, nodes_dir=tmp_path / "nodes")
    assert next(iter(dag.calls.values())).resources == {"threads": 3}


def test_run_resource_file_and_historical_provenance(tmp_path, job):
    """Changing resource policy reuses outputs and leaves historical resources intact."""
    resources = tmp_path / "resources.toml"
    resources.write_text("[make]\nthreads = 2\n")
    main(args(tmp_path, job, resources, "run"))
    output = next((tmp_path / "nodes").glob("*/*/result.txt"))
    assert output.read_text().strip() == "2"
    metadata = output.parent / ".rip" / "dependencies.toml"
    before = metadata.read_bytes()
    resources.write_text("[make]\nthreads = 3\n")
    main(args(tmp_path, job, resources, "run"))
    assert metadata.read_bytes() == before
    assert tomlkit.parse(before.decode())["resources"]["threads"] == 2


@pytest.mark.parametrize(
    "content,match",
    [
        ("[typo]\nthreads=2\n", "unknown rules"),
        ("[make]\nram=2\n", "unknown resources"),
        ("[make]\nthreads=false\n", "positive integer"),
        ("[make]\nthreads=0\n", "positive"),
        ("make=2\n", "tables"),
        ("[make", "resources"),
    ],
)
def test_bad_resource_files_fail_before_execution(tmp_path, job, content, match):
    """Typos and invalid resource values fail before commands create outputs."""
    resources = tmp_path / "bad.toml"
    resources.write_text(content)
    with pytest.raises(SystemExit, match=match):
        main(args(tmp_path, job, resources, "run"))
    assert not list((tmp_path / "nodes").glob("*/*/result.txt"))


def test_python_policy_unknown_rules_fail_before_run(tmp_path):
    """Python callers get the same complete-policy validation as the CLI."""
    pipeline = Pipeline(DAG(tmp_path, resources={"typo": {"threads": 2}}))
    pipeline.result = inline(pipeline)
    pipeline.finish()
    pipeline.dag.require(pipeline.sinks())
    with pytest.raises(ValueError, match="unknown rules"):
        run(pipeline.dag)
    assert not pipeline.result.path.exists()


def test_mcp_resource_parity(tmp_path, job):
    """MCP compilation accepts the same resource file and exposes effective requests."""
    pytest.importorskip("mcp")
    from necroflow import mcp_server

    resources = tmp_path / "resources.toml"
    resources.write_text("[make]\nthreads=2\n")
    kwargs = dict(
        jobs=[str(job)],
        resources=str(resources),
        nodes_dir=str(tmp_path / "nodes"),
        results_dir=str(tmp_path / "results"),
    )
    assert mcp_server.graph(**kwargs)["nodes"][0]["resources"] == {"threads": 2}
    assert "echo 2" in mcp_server.explain(**kwargs)["calls"][0]["command"]
    mcp_server.outputs(**kwargs)
    mcp_server.doctor(**kwargs)
