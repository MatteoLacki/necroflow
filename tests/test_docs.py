"""Drift guards: CLAUDE.md and docs/ must not contradict the code.

CLAUDE.md is auto-loaded by AI agents and trusted over the source. Stale
signatures there caused agents to write code against APIs that no longer
exist (e.g. the 2-argument scheduler protocol). These tests fail whenever
the documented surface falls behind the code, so the pre-commit hook keeps
the docs honest.
"""

import inspect
import re
from pathlib import Path

from necroflow import executor
from necroflow.schedulers import fifo_scheduler

REPO = Path(__file__).resolve().parent.parent
CLAUDE_MD = (REPO / "CLAUDE.md").read_text(encoding="utf-8")


def test_execute_parameters_are_documented_in_claude_md():
    """Every execute() parameter name must appear in CLAUDE.md.

    Agents read CLAUDE.md before the code. If execute() grows a parameter that
    CLAUDE.md never mentions, agents cannot discover it; if CLAUDE.md lists the
    parameters, at minimum the list must be complete. The full semantics live
    in the docstring — CLAUDE.md only needs the names.
    """
    params = inspect.signature(executor.execute).parameters
    missing = [name for name in params if name not in CLAUDE_MD]
    assert not missing, f"execute() params absent from CLAUDE.md: {missing}"


def test_scheduler_protocol_in_claude_md_matches_code():
    """The scheduler protocol documented in CLAUDE.md must match the built-ins.

    The protocol grew a third argument (available_resources); the old 2-argument
    form survived in CLAUDE.md and taught agents to write schedulers that raise
    TypeError at runtime. Guard: every parameter of the reference built-in
    scheduler must be named in CLAUDE.md.
    """
    params = list(inspect.signature(fifo_scheduler).parameters)
    assert params == ["ready", "remaining", "available_resources"]
    missing = [name for name in params if name not in CLAUDE_MD]
    assert not missing, f"scheduler protocol params absent from CLAUDE.md: {missing}"


def test_file_map_covers_all_modules():
    """Every top-level module and subpackage of necroflow must appear in CLAUDE.md.

    The file map is how agents route to the right module. Modules added after
    the map was written (config.py, graphviz_render.py, ...) were invisible to
    agents, which then re-derived or duplicated their functionality.
    """
    pkg = REPO / "src" / "necroflow"
    missing = []
    for path in sorted(pkg.iterdir()):
        if path.name.startswith("_"):  # __init__.py, _compat.py — internal
            continue
        if path.is_file() and path.suffix == ".py":
            if path.name not in CLAUDE_MD:
                missing.append(path.name)
        elif path.is_dir() and path.name != "__pycache__":
            if f"{path.name}/" not in CLAUDE_MD:
                missing.append(f"{path.name}/")
    assert not missing, f"modules absent from CLAUDE.md file map: {missing}"


def test_cli_subcommands_are_documented():
    """Every CLI subcommand must be mentioned in docs/cli.md and CLAUDE.md.

    Subcommands like doctor/explain exist specifically so agents can verify
    behavior against the live code; an undocumented subcommand defeats that
    purpose.
    """
    cli_source = (REPO / "src" / "necroflow" / "cli.py").read_text(encoding="utf-8")
    subcommands = re.findall(r"add_parser\(\s*\"(\w+)\"", cli_source)
    assert subcommands, "no subcommands found — extraction regex is stale"
    cli_md = (REPO / "docs" / "cli.md").read_text(encoding="utf-8")
    undocumented = [
        name for name in subcommands if name not in cli_md or name not in CLAUDE_MD
    ]
    assert not undocumented, f"CLI subcommands absent from docs: {undocumented}"


def test_claude_requires_rule_call_lifecycle_updates():
    """Pipeline-internal changes must route maintainers to the lifecycle doc."""
    assert "docs/rule-call-lifecycle.md" in CLAUDE_MD
    assert "Keep the lifecycle document synchronized" in CLAUDE_MD
    assert "must\nupdate `docs/rule-call-lifecycle.md`" in CLAUDE_MD
