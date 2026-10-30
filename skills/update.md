---
name: migrate-main-pipeline
description: Migrate legacy Necroflow Rules-container pipelines to typed outputs(), executable @command declarations with shell(), Resource parameters, and @workflow. Use for Rules(), @R.command, Type[name] outputs, resolve_paths(), dag.add(), or execute(Pipeline).
---

# Migrate a legacy pipeline

This guide targets the old `Rules` registry API, not a particular branch name.
Use the installed code as authority; `main` also contains the current API.
For a new workflow, start with `necroflow guide` instead.

Preserve rule names, artifact types/filenames, command behavior, resource policy,
config values, labels, and requested outputs. Identity can change during migration;
do not promise reuse of existing cache directories.

## Before: legacy registry

Reference only; `Source` and `Result` denote the old artifact declarations.
This block requires the historical API and is not runnable with current Necroflow.

```python
# Legacy API (reference only)
from necroflow import Pipeline, Rules

R = Rules()
R.symlink_file("source", Source)

@R.command("cat {src} > {dst}", threads=4)
def convert(src: Source):
    return Result[dst]

def factory(config) -> Pipeline:
    P = Pipeline()
    P.source = R.symlink(Source, config["path"])
    P.result = R.convert(P.source)
    return P
```

## After: complete current workflow

Save as `pipeline.py`. Keep the original workflow function name so existing
job TOMLs can keep their `.pipeline` reference.

```python
# pipeline.py
from necroflow import (
    Node, Pipeline, Resource, command, outputs, shell, symlink, workflow,
)


class Source(Node):
    filename = "source.txt"


class Result(Node):
    filename = "result.txt"


@command
def convert(src: Source, threads: Resource[int] = 4):
    dst = outputs(Result)
    shell("cat {src} > {dst}")
    return dst


@workflow
def factory(P: Pipeline, config: dict) -> None:
    P.source = symlink(Source, config["path"])
    P.result = convert(P.source)
```

This example retains the legacy four-thread reservation even though `cat`
does not use a thread flag. For tools that do, keep `{threads}` in the command.
`threads: Resource[int] = 4` declares scheduling capacity; an ordinary
`threads: int` is not a resource declaration.

Save as `job.toml`:

```toml
".pipeline" = "pipeline.py:factory"
".requests" = ["result"]
path = "input.txt"
```

Run from the directory containing both files:

```bash
printf 'migration example\n' > input.txt
necroflow doctor --json job.toml
necroflow outputs --json job.toml
necroflow graph --json job.toml
necroflow explain --json job.toml
necroflow run -c 4 job.toml
cat results/job/result/result.txt
necroflow run -c 4 job.toml
```

Expected text: `migration example`. The generated ingestion rule is
`symlink_Source`. The second run records both calls as cached
in `results/job/execution.toml`. Only label `result` is copied into results;
the source remains a dependency in the node store.

## Migration procedure

1. Read current `docs/rules.md`, `docs/rule-call-lifecycle.md`, and
   `docs/job-toml.md`. Inventory registry calls, `Type[name]`, singular
   `output(...)`, workflows creating their own Pipeline, late path resolution,
   and argv-list commands.
2. Remove the `Rules` import and registry instance. Declare each artifact as a
   `Node` subclass with its explicit output filename. Declare rules at module
   scope and call their names directly instead of `R.name(...)`.
3. Prefer bare `@command`. Put fixed `name = outputs(Type)` assignments first,
   then register exactly one `shell(template)`, and return all declared artifacts
   exactly once in declaration order. Move command text from `@R.command(...)`,
   `@command("...")`, or old local command assignments into `shell(...)`.
   Singular `output(...)` is not the current API. Existing command-string and
   factory forms remain supported, but are not this guide's migration target.
4. Annotate every input. Node annotations create dependencies; ordinary types
   describe config. Use `Resource[int]`, `Resource[float]`, or `Resource[str]`
   with positive defaults for execution quantities. Executable declarations require concrete integer
   threads, not `threads="all"`. Preserve retry policy with `@command(repeat=N)`
   if needed: at most N command attempts, stopping at the first success.
5. Replace `@symlink_file` declarations and registry symlink rules with `symlink(Type, path)` in the workflow;
   replace text rules with `@text_file` plus `outputs(...)`. Explicitly named
   rules can use the corresponding `*_rule(...)` factories. Ingest external
   files as Nodes when their bytes must trigger stale detection; plain path
   strings track only their text. Preserve existing ingestion semantics deliberately.
6. Decorate workflows with `@workflow` and change their signature to
   `factory(P: Pipeline, config: dict) -> None`. Remove internal `Pipeline()`
   creation and `return P`. The CLI supplies ownership and finishes construction.
   Keep `P.name = ...` / `P["path/name"] = ...`; returning a Node does not label it.
7. Within workflows, use `rule(node_inputs..., config_name=value)`. Explicit
   `rule(P, ...)` remains supported. Executable declarations use ordinary Python
   parameter binding; Node inputs are not universally positional-only, nor config
   inputs universally keyword-only. Pass variadic Node groups as tuples; use
   `Annotated[tuple[Type, ...], Many(...)]` only for cardinality bounds.
8. Preserve `.pipeline` and exact `.requests` labels when their meaning remains
   valid. Labels select results, not artifact filenames, and stay outside identity.
   Reusable fragments can use `P.subpipeline(prefix)` to qualify labels.
9. For argv lists, register a shell string with the same command behavior.
   Keep dynamic values as placeholders so Necroflow can quote substitutions;
   double literal shell braces. Complex existing `CommandArgs -> str` callbacks
   can retain their supported callback/factory form; they have a separate lifecycle.
10. Run Black, configured type checks, focused pipeline tests, and inspection
    against a representative job. Verify requested filenames/content, resource
    requests, initial execution, and a second cached run. Search remaining legacy
    constructs in executable code, distinguishing historical examples from targets.

## Direct Python callers

With `pipeline.py` and `input.txt` from above, this is a complete alternative to
the CLI. Save as `run_pipeline.py` and run `python run_pipeline.py`:

```python
# run_pipeline.py
from necroflow import DAG, Pipeline
from pipeline import factory

dag = DAG("python-nodes")
P = Pipeline(dag)
factory(P, {"path": "input.txt"})
P.finish()
dag.require([P.result])
dag.run(resource_caps={"threads": 4})
```

The direct API writes canonical artifacts under `python-nodes/`; result copies
and manifests are CLI behavior. For default sink selection use
`dag.require(P.sinks())` after finishing. Remove `resolve_paths()`, `dag.add()`,
and `execute(Pipeline)`: paths and interning are eager; execution is DAG-owned.

## Guardrails

- Use `node.relative_path` for stable identity and Pipeline label lookups for
  presentation. Do not restore `node.key`, `node.pipeline_label`, short hashes,
  late path resolution, or compatibility registries.
- Rule bodies run at graph construction, including inspection and cached calls.
  Keep them deterministic and free of external side effects; put work in shell.
  Workflows compose rules; command bodies cannot invoke other commands.
- Resources stay outside fingerprints and must not change semantic results or
  output schema. Explicitly declare any result-affecting value as config/input.
  Arbitrary helper/global/environment changes are not automatically tracked.
- Source recipe identity and fingerprint formats have changed across APIs.
  Plan for new cache addresses; preserve the old store until migrated results
  are verified. Do not delete caches as part of an API migration by default.
- Doctor invokes workflow Python and performs filesystem probes. Explain may
  leave parent hashes undecided; dry runs can refresh stored content hashes.
- Update `docs/rule-call-lifecycle.md` if migration work changes framework
  internals. Downstream-only pipeline migration does not require that update.

`tests/test_migration_guide.py` executes the current examples from this file.
