---
name: migrate-main-pipeline
description: Migrates necroflow pipelines written for the main-branch Rules-container API to the current explicit Pipeline/DAG and module-level rule API. Use when porting code containing Rules(), @R.command, Type[name] outputs, workflows constructing Pipeline(), resolve_paths(), dag.add(), or execute(Pipeline).
---

# Migrate a main-branch pipeline

## Quick start

Preserve rule names, NodeTypes, commands, constraints, config values, labels,
and requested outputs. Change the API shape, then run the pipeline tests.

```python
# main
from necroflow import Pipeline, Rules

R = Rules()
R.symlink_file("source", Source)

@R.command("tool {src} > {dst}", threads=4)
def convert(src: Source):
    return Result[dst]

def factory(config) -> Pipeline:
    P = Pipeline()
    P.source = R.source(path=config["path"])
    P.result = R.convert(P.source)
    return P

# current
from necroflow import Pipeline, command, output, symlink_file, workflow

@symlink_file
def source(path: str):
    src = output(Source)
    return src

@command("tool {src} > {dst}", threads=4)
def convert(src: Source):
    dst = output(Result)
    return dst

@workflow
def factory(P: Pipeline, config) -> None:
    P.source = source(path=config["path"])
    P.result = convert(P.source)
```

## Workflow

1. Read current `docs/rules.md`, `docs/rule-call-lifecycle.md`, and
   `docs/job-toml.md`. Treat current code as authoritative.
2. Inventory old constructs before editing:
   `Rules`, `@R.command`, `@R.rule`, `R.register`, `R.text_file`,
   `R.symlink_file`, `Type[name]`, `Pipeline()`, `resolve_paths`, `dag.add`,
   `execute(Pipeline)`, and list-valued commands.
3. Replace `from necroflow import Rules` and registry instances with imports of
   `command`, `output`, and any needed `text_file`, `symlink_file`,
   `text_file_rule`, or `symlink_file_rule`.
4. Convert `@R.command(...)` to `@command(...)`. Move every output from
   `Type[name]` into a real `name = output(Type)` assignment and return those
   names exactly once. Move an `@R.rule` local `command = ...` expression into
   the `@command(...)` argument.
5. Replace `R.text_file(...)` and `R.symlink_file(...)` with decorated built-in
   declarations when possible, or the corresponding `*_rule(...)` value.
   Replace every `R.name(...)` call with the module-level rule value `name(...)`.
6. Decorate workflow functions with `@workflow`; pass their owning `Pipeline`
   as the first positional workflow argument. Rules then use
   `rule(node_inputs..., config_name=value)`. Explicit `rule(P, ...)` remains
   supported outside or inside workflows. Keep Node inputs positional and
   config inputs keyword-only. Pass variadic Node inputs as one tuple; use
   `Annotated[tuple[Type, ...], Many(...)]` only when bounds are required.
7. Convert CLI workflows from `factory(config) -> Pipeline` into
   `factory(P: Pipeline, config) -> None`. Remove `P = Pipeline()` and
   `return P`; keep explicit `P.name = ...` or `P["path/name"] = ...` labels.
   Job TOML `.pipeline` and string `.requests` entries normally remain valid.
8. For direct Python entry points, create one `DAG(nodes_dir)`, construct each
   `Pipeline(dag)`, call its workflow, finish the root with `P.finish()`, then select
   `P.sinks()` or explicit `P[label]`
   Nodes with `dag.require(...)`, then call `dag.run()`. Delete
   `resolve_paths()` and `dag.add()`; Nodes already have final fingerprints,
   relative paths, and absolute paths when rules return.
9. Convert argv-list commands to a shell string or a module-level,
   closure-free `CommandArgs -> str` callback. Preserve shell behavior and quote
   callback-controlled values explicitly.
10. Search again for every old construct. Run Black, Pyright where configured,
    the focused pipeline tests, and one dry run or `necroflow doctor`,
    `outputs --json`, and `graph --json` against a representative job.

## Guardrails

- Do not preserve `node.key`, `node.pipeline_label`, 16-character hashes, or
  late path resolution. Use `node.relative_path`, Pipeline label lookups, and
  the full fingerprint.
- Do not add compatibility wrappers or recreate a `Rules` registry.
- Do not change command text, config semantics, output filenames, labels, or
  request selection unless the old behavior cannot be represented.
- Inspect cache migration separately: fingerprint v2 intentionally changes
  addresses, so old main-branch node directories are not reusable by default.
- Update `docs/rule-call-lifecycle.md` if the migration exposes or changes
  necroflow internals rather than only user pipeline code.
