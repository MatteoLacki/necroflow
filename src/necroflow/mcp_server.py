"""necroflow MCP server — read-only pipeline introspection over stdio.

Exposes the same JSON payloads as `necroflow graph|outputs|explain|doctor|provenance
--json`, without a subprocess/parse round-trip. Deliberately excludes `run` and `gc`:
those execute or delete, and belong behind a visible shell command an operator
approves, not an opaque MCP tool call.

Optional install: `pip install necroflow[mcp]`. Run with `necroflow-mcp` (stdio
transport) and register it with an MCP client such as Claude Code.
"""

from __future__ import annotations

from pathlib import Path

from mcp.server.mcpserver import MCPServer

from necroflow.cli import (
    _build_dag_from_jobs,
    _build_parser,
    _doctor_payload,
    _explain_payload,
    _graph_payload,
    _outputs_payload,
    _preflight_result_paths,
    _provenance_payload,
    _resolve_roots,
)

mcp = MCPServer(
    "necroflow",
    instructions=(
        "Read-only introspection for necroflow pipelines. Prefer these tools over "
        "shelling out to `necroflow graph|outputs|explain|doctor|provenance --json` "
        "for the same job TOML files — identical data, already parsed. None of "
        "these tools execute rules, write to the node store, or delete anything; "
        "use the `necroflow run`/`gc` CLI commands via a shell for that, so the "
        "operator sees the real command before approving it."
    ),
)


def _root_args(
    command: str,
    jobs: list[str],
    *,
    nodes_dir: str | None = None,
    results_dir: str | None = None,
    outdir: str | None = None,
    extra: list[str] | None = None,
):
    """Parse CLI args for one subcommand, mirroring `necroflow COMMAND ...`."""
    argv = [command]
    if nodes_dir is not None:
        argv += ["--nodes-dir", nodes_dir]
    if results_dir is not None:
        argv += ["--results-dir", results_dir]
    if outdir is not None:
        argv += ["--outdir", outdir]
    argv += extra or []
    argv += list(jobs)
    return _build_parser().parse_args(argv)


def _tool_error(exc: SystemExit) -> RuntimeError:
    """Turn a CLI-style SystemExit into a normal MCP tool error."""
    message = str(exc.code if exc.code is not None else exc)
    return RuntimeError(message.removeprefix("error: "))


@mcp.tool()
def graph(
    jobs: list[str],
    nodes_dir: str | None = None,
    results_dir: str | None = None,
    outdir: str | None = None,
) -> dict:
    """Return DAG nodes, edges, and per-job requests for the given job TOML files."""
    try:
        args = _root_args(
            "graph", jobs, nodes_dir=nodes_dir, results_dir=results_dir, outdir=outdir
        )
        roots_nodes_dir, _results_dir = _resolve_roots(args)
        dag, combos, _forced = _build_dag_from_jobs(args, nodes_dir=roots_nodes_dir)
        return _graph_payload(dag, combos)
    except SystemExit as exc:
        raise _tool_error(exc) from exc


@mcp.tool()
def outputs(
    jobs: list[str],
    nodes_dir: str | None = None,
    results_dir: str | None = None,
    outdir: str | None = None,
) -> dict:
    """List requested node-store and result paths for the given job TOML files."""
    try:
        args = _root_args(
            "outputs", jobs, nodes_dir=nodes_dir, results_dir=results_dir, outdir=outdir
        )
        roots_nodes_dir, roots_results_dir = _resolve_roots(args)
        dag, combos, _forced = _build_dag_from_jobs(args, nodes_dir=roots_nodes_dir)
        _preflight_result_paths(roots_results_dir, combos)
        return _outputs_payload(combos, results_dir=roots_results_dir)
    except SystemExit as exc:
        raise _tool_error(exc) from exc


@mcp.tool()
def explain(
    jobs: list[str],
    node: str | None = None,
    nodes_dir: str | None = None,
    results_dir: str | None = None,
    outdir: str | None = None,
) -> dict:
    """Explain which RuleCalls would run and why, without executing them."""
    try:
        extra = ["--node", node] if node is not None else None
        args = _root_args(
            "explain",
            jobs,
            nodes_dir=nodes_dir,
            results_dir=results_dir,
            outdir=outdir,
            extra=extra,
        )
        return _explain_payload(args)
    except SystemExit as exc:
        raise _tool_error(exc) from exc


@mcp.tool()
def doctor(
    jobs: list[str],
    nodes_dir: str | None = None,
    results_dir: str | None = None,
    outdir: str | None = None,
) -> dict:
    """Run preflight checks for the given job TOML files and report NF_* issues."""
    try:
        args = _root_args(
            "doctor", jobs, nodes_dir=nodes_dir, results_dir=results_dir, outdir=outdir
        )
        return _doctor_payload(args)
    except SystemExit as exc:
        raise _tool_error(exc) from exc


@mcp.tool()
def provenance(path: str) -> dict:
    """Read stored provenance metadata for one cached node-store output."""
    try:
        return _provenance_payload(Path(path))
    except SystemExit as exc:
        raise _tool_error(exc) from exc


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
