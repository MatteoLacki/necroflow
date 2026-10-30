---
name: use-necroflow
description: Write, inspect, run, and debug downstream Necroflow pipelines with typed artifacts, Python workflows, and job TOMLs. Use when creating or operating a Necroflow workflow; framework internals have separate contributor guidance.
---

# Use Necroflow

Necroflow runs Python-defined file pipelines. Typed Nodes describe artifacts;
rules declare outputs and shell commands; workflows connect rule calls and
label results. Job TOMLs supply config and requested labels. A shared DAG
deduplicates equivalent work; the filesystem stores cached outputs and provenance.

Retrieve this guide from any installed copy with `necroflow guide`.

## Minimal complete workflow

Install with `pip install necroflow` in your Python environment (Python >=3.11;
Linux/macOS, or WSL). The example uses the POSIX `tr` utility.
In a new working directory, save this as `pipeline.py`:

```python
from necroflow import Node, Pipeline, command, outputs, shell, symlink_file, workflow


class Source(Node):
    filename = "source.txt"


class Uppercase(Node):
    filename = "uppercase.txt"


@symlink_file
def source(path: str):
    text = outputs(Source)
    return text


@command
def uppercase(text: Source):
    result = outputs(Uppercase)
    shell("tr '[:lower:]' '[:upper:]' < {text} > {result}")
    return result


@workflow
def build(P: Pipeline, config: dict) -> None:
    P.source = source(path=config["input"])
    P.uppercase = uppercase(P.source)
```

Save this as `job.toml`:

```toml
".pipeline" = "pipeline.py:build"
".requests" = ["uppercase"]
input = "input.txt"
```

Run these commands from that directory:

```bash
printf 'hello necroflow\n' > input.txt
necroflow doctor --json job.toml
necroflow graph --json job.toml
necroflow outputs --json job.toml
necroflow explain --json job.toml
necroflow run -c 2 job.toml
cat results/job/uppercase/uppercase.txt
necroflow run -c 2 job.toml
```

Expected text: `HELLO NECROFLOW`. The second run reuses both cached rule calls.
`results/job/execution.toml` records `cached = true` for each call on that run.
`nodes/` holds canonical cached artifacts; `results/job/` contains requested
output copies, `manifest.toml` (origins/content hashes), and `execution.toml`.
Set `--nodes-dir` and `--results-dir` explicitly when choosing other roots.

For a larger starting example, use `necroflow init my-workflow`, then read the
generated README. Use `necroflow run --help` for runtime options.

## Rules for writing workflows

- Give every output Node subclass a `filename`. Start rule bodies with
  `name = outputs(Type)` declarations, register exactly one `shell(template)`,
  and return every declared output exactly once. Write every output at runtime;
  exit status zero with a missing output still fails.
- Node inputs form dependencies. Plain config values enter call identity.
  A file path string tracks only its text: ingest files with `@symlink_file`
  when their contents must invalidate consumers. Symlinks preserve no snapshot;
  timestamp-preserving external edits are unsupported.
- `@workflow` supplies rule ownership. Publish outputs with `P.name = node` or
  `P["sample/result"] = node`. `.requests` selects those labels, not filenames.
  Omitting `.requests` requests labelled sinks. Returning a Node does not label it.
- Use `P.subpipeline("sample")` for reusable labelled fragments. Labels do not
  affect fingerprints. CLI finishes construction automatically; direct Python
  callers use `P.finish()`, `dag.require(...)`, then `dag.run()`.
- Rule bodies execute during graph construction, including inspection. Put
  expensive work in shell commands; keep workflow/rule construction deterministic
  and free of external side effects. Source hashing does not discover arbitrary
  helper, global, or environment changes.
- Declare execution quantities with `Resource[int]`, e.g.
  `threads: Resource[int] = 4`, then use `{threads}` in the shell template.
  Resources stay outside fingerprints: changing them must preserve semantic
  results and output schema. Ordinary config values remain fingerprinted.
- Generated artifact placeholders are shell-quoted. Double literal braces in
  shell templates (`{{...}}`); select Bash explicitly if the command needs it.

## Inspect and debug

Prefer connected Necroflow MCP tools for structured inspection; otherwise use
the CLI commands above. MCP is optional: `pip install 'necroflow[mcp]'`, then
configure your MCP client to launch `necroflow-mcp` over stdio. Its tools are
`graph`, `outputs`, `explain`, `doctor`, and `provenance`; execute via the CLI.

| Need | Command |
|---|---|
| Config, request, or environment preflight | `necroflow doctor --json job.toml` |
| Dependencies and selected results | `necroflow graph --json job.toml` |
| Canonical and visible result paths | `necroflow outputs --json job.toml` |
| Cache decision for one label | `necroflow explain --json --node uppercase job.toml` |
| Settle undecided parent content hashes | `necroflow run --dry-run job.toml` |
| Stored lineage for a cached output | `necroflow provenance --json nodes/RULE/HASH/FILE` |

Inspection builds workflows by executing Python. Doctor also creates missing
root directories and temporary writability probes. `explain` does not hash full
outputs or write the node store; an unchecked parent hash can leave a verdict
undecided. Dry runs may hash outputs and store refreshed digests. Doctor does
not guarantee that external tools are installed or that shell commands succeed.

A rebuilt call reruns active descendants by default. `--reuse-unchanged` allows
consumers to remain cached when rebuilt parent bytes match recorded hashes.
Failed-command logs live in the call workdir's `.rip/job.log`.

## Further reference

In the source repository, consult `docs/rules.md`, `docs/job-toml.md`,
`docs/caching.md`, `docs/doctor.md`, and `docs/cli.md` for detailed semantics.
Repository entry point `AGENTS.md` routes contributors to framework internals
and task-specific skills. Use installed code/docstrings to resolve API drift.
