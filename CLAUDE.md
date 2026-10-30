# necroflow

Python-defined file pipelines: typed Nodes describe artifacts; rules declare
outputs and shell commands; workflows connect rule calls and label results.
Job TOMLs supply config and requests. A shared DAG deduplicates equivalent work;
the filesystem stores cached outputs and provenance.

## Use Necroflow

Read `src/necroflow/skills/use-necroflow/SKILL.md` for a complete, tested example,
inspection/run commands, cache behavior, and common mistakes. Installed-package
users can retrieve the same skill with `necroflow guide`.

Prefer connected Necroflow MCP tools (`graph`, `outputs`, `explain`, `doctor`,
`provenance`) for structured inspection; fall back to CLI `--json` commands.
MCP setup: `pip install 'necroflow[mcp]'`, client launches `necroflow-mcp`.
Execute jobs with `necroflow run`; cache deletion uses `necroflow gc`.
Inspection invokes workflow Python; doctor probes filesystem writability;
dry runs can refresh stored hashes. See the usage skill for these boundaries.

## Work on the framework

Before version 1.0.0, prefer simpler APIs over backwards compatibility. Breaking
API changes are allowed; migrate affected callers, examples, docs, and tests
together rather than adding compatibility wrappers solely to preserve old syntax.

**Code is authoritative.** Read the relevant docstrings and focused docs before
asserting behavior. Detailed invariants, module map, scheduler/run protocols,
testing conventions, and human review markers live in `docs/agent-internals.md`.

| Task | Read |
|---|---|
| Add/edit a workflow rule | `.claude/skills/add-a-rule/SKILL.md`, `docs/rules.md` |
| Migrate a legacy Rules-container pipeline | `skills/update.md` |
| Debug cache classification | `.claude/skills/debug-stale-classification/SKILL.md`, `docs/caching.md` |
| Edit/refactor framework source | `.claude/skills/necroflow-style/SKILL.md`, `docs/agent-internals.md` |
| Write a scheduler | `.claude/skills/write-a-scheduler/SKILL.md`, `docs/schedulers.md` |
| Construction, identity, labels, requests | `docs/rule-call-lifecycle.md` |
| Execution, resources, failure handling | `docs/executor.md` |
| CLI: guide, init, run, graph, outputs, provenance, doctor, explain, gc | `docs/cli.md` |
| Job TOML/grids and validation | `docs/job-toml.md`, `docs/config-validation.md` |
| Generated config files | `docs/generated-config-files.md` |
| Feature map and recent notes | `features.txt`, `AI.md` (read as needed) |
| Setup, tests, releases | `docs/development.md`, `docs/release.md` |
| MCP tools and transport | `src/necroflow/mcp_server.py` |

Skills under `.claude/skills/` are available to Claude Code; other agents read
those markdown files directly. The usage skill links to the packaged original.
`AGENTS.md` is a symlink to this file; keep one entry point for all agents.

**Keep the lifecycle document synchronized with pipeline internals.** Changes to
Pipeline/DAG construction, rule-call compilation, identity/path derivation,
interning, labels, requests, or execution handoff must
update `docs/rule-call-lifecycle.md` in the same change.

`tests/test_docs.py` guards selected documented interfaces; the usage skill's
example is exercised by `tests/test_agent_guide.py`. Update guidance with code.
