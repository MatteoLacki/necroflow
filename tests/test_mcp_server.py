"""Tests for the optional necroflow MCP server: read-only tool wrappers over CLI payloads."""

import textwrap

import pytest

pytest.importorskip("mcp")

import necroflow.cli as cli_core
import necroflow.mcp_server as mcp_server

FACTORY_SRC = textwrap.dedent("""\
    from necroflow import Pipeline, NodeType, command, output
    class A(NodeType): filename = "a.txt"
    class B(NodeType): filename = "b.txt"
    @command("echo {v} > {a}")
    def make_a(v: str):
        a = output(A)
        return a
    @command("cat {a} > {b}")
    def make_b(a: A):
        b = output(B)
        return b
    def factory(P, cfg):
        P.a = make_a(P, v=cfg["v"])
        P.b = make_b(P, P.a)
""")


@pytest.fixture
def factory_file(tmp_path):
    f = tmp_path / "pipe.py"
    f.write_text(FACTORY_SRC)
    return f


@pytest.fixture
def job_toml(tmp_path, factory_file):
    t = tmp_path / "job.toml"
    t.write_text(f'".pipeline" = "{factory_file}:factory"\nv = "hello"\n')
    return t


@pytest.fixture
def roots(tmp_path):
    return str(tmp_path / "nodes"), str(tmp_path / "results")


def test_graph_matches_cli_payload(job_toml, roots):
    """The MCP graph tool must return the same shape as `necroflow graph --json`."""
    nodes_dir, results_dir = roots
    payload = mcp_server.graph(
        [str(job_toml)], nodes_dir=nodes_dir, results_dir=results_dir
    )
    rules = sorted(node["rule"] for node in payload["nodes"])
    assert rules == ["make_a", "make_b"]
    assert payload["edges"] == [
        {
            "from": next(n for n in payload["nodes"] if n["rule"] == "make_a")["key"],
            "to": next(n for n in payload["nodes"] if n["rule"] == "make_b")["key"],
        }
    ]


def test_outputs_lists_requested_result_path(job_toml, roots):
    """outputs() must report the requested node's node-store and result paths."""
    nodes_dir, results_dir = roots
    payload = mcp_server.outputs(
        [str(job_toml)], nodes_dir=nodes_dir, results_dir=results_dir
    )
    [job] = payload["jobs"]
    [requested] = job["requested"]
    assert requested["rule"] == "make_b"
    assert requested["result_path"].endswith("b/b.txt")


def test_doctor_reports_ok_for_valid_job(job_toml, roots):
    """A runnable job TOML must produce doctor(ok=True) with no error-severity issues."""
    nodes_dir, results_dir = roots
    payload = mcp_server.doctor(
        [str(job_toml)], nodes_dir=nodes_dir, results_dir=results_dir
    )
    assert payload["ok"] is True


def test_explain_lists_calls_that_would_run(job_toml, roots):
    """Before any run, explain() must mark the root as will_run=True (output missing)
    and the descendant as will_run=None (unknown until the parent's real bytes exist).
    """
    nodes_dir, results_dir = roots
    payload = mcp_server.explain(
        [str(job_toml)], nodes_dir=nodes_dir, results_dir=results_dir
    )
    calls = {call["rule"]: call for call in payload["calls"]}
    assert set(calls) == {"make_a", "make_b"}
    assert calls["make_a"]["will_run"] is True
    assert calls["make_b"]["will_run"] is None


def test_explain_node_filters_to_one_label(job_toml, roots):
    """--node semantics: explain(node=...) must narrow to the single matching label."""
    nodes_dir, results_dir = roots
    payload = mcp_server.explain(
        [str(job_toml)], node="a", nodes_dir=nodes_dir, results_dir=results_dir
    )
    assert [call["rule"] for call in payload["calls"]] == ["make_a"]


def test_provenance_reads_metadata_after_run(job_toml, roots):
    """provenance() must read back the rule and hash recorded by a real run."""
    nodes_dir, results_dir = roots
    cli_core.main(
        ["run", "--nodes-dir", nodes_dir, "--results-dir", results_dir, str(job_toml)]
    )
    graph_payload = mcp_server.graph(
        [str(job_toml)], nodes_dir=nodes_dir, results_dir=results_dir
    )
    b_node = next(n for n in graph_payload["nodes"] if n["rule"] == "make_b")
    payload = mcp_server.provenance(b_node["path"])
    assert payload["rule"] == "make_b"
    assert payload["provenance_hash"]


def test_graph_missing_job_raises_runtime_error_not_systemexit(roots):
    """A bad job path must surface as RuntimeError, never SystemExit, so the MCP
    server process stays alive across a single bad tool call."""
    nodes_dir, results_dir = roots
    with pytest.raises(RuntimeError, match="job file not found"):
        mcp_server.graph(
            ["/no/such/job.toml"], nodes_dir=nodes_dir, results_dir=results_dir
        )


def test_provenance_missing_output_raises_runtime_error(tmp_path):
    """provenance() on a path with no .rip metadata must raise RuntimeError cleanly."""
    missing = tmp_path / "nowhere.txt"
    with pytest.raises(RuntimeError, match="provenance metadata not found"):
        mcp_server.provenance(str(missing))
