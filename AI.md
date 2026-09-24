# necroflow AI Notes

For a compact map of the current software surface, start with `features.txt`.

## Dependency decision

Keep `tomlkit`; do not remove or replace it solely to reduce dependency count.
It has no third-party runtime dependencies and supplies TOML writing, grid
semantics, and formatting-preserving config edits. Read the
[decision and removal-cost assessment](docs/development.md#keep-the-tomlkit-dependency)
before revisiting this choice.

## Workflow context

`@workflow` accepts an open Pipeline as the first positional argument and activates
it for synchronous rule calls. Nested decorated calls restore the caller's context,
including after exceptions. Explicit `rule(P, ...)` remains supported and takes
precedence per call. Attribute/item assignments still register all public labels;
return values do not. CLI workflows retain `(P, config) -> None`.

## Subpipelines and finished construction

`P.subpipeline(prefix)` returns a prefixed view over the same root Pipeline.
Attribute and item assignments through the view register qualified labels on
the root, while all views expose the same complete labels and nodes. Prefixes
are canonical non-empty relative POSIX request paths, compose when nested, and
remain outside both fingerprints. Equivalent rule calls through different
views therefore intern to the same canonical Nodes. External input Nodes are
passed explicitly to reusable subworkflow functions.

`P.finish()` freezes the root and every view. Later rule calls fail before
fingerprinting or DAG interning; later bindings and subpipeline creation also
fail. Only the root may finish, root finishing is idempotent, and `P.sinks()`
requires finished construction. The CLI calls `finish()` automatically after
each successful workflow return; direct Python callers call it before selecting
sinks. Pipeline sections were removed; PNG graphs use dependency-depth groups.

## Rule command placeholders

Rule commands are validated when a `Rule` is registered. Placeholders are limited to declared input names, declared output names, and built-in command placeholders.

Built-in placeholders:

- `{workdir}` resolves to the rule-call output directory inside the node store, `nodes/{rule}/{provenance_hash}` by default. Use it for tools that need to write side directories or scratch files that should be retained with the cached result. The name `workdir` is reserved and cannot be used as an input or output name.

`{workdir}` is created before the command subprocess starts. Its contents are kept by default. With `autoclean=True`, intermediate rule-call directories are removed as whole directories once all active children are up to date, so `{workdir}` side files are cleaned together with declared outputs.

## Command input defaults

Decorated `@command` rules honor Python defaults on scalar/config parameters;
fixed mixed Node/value inputs may also default to one of their non-Node arms.
Explicit factory and direct `Rule` construction use `input_defaults`. Defaults
are type-checked when the `Rule` is declared and expanded before call validation.
Omitted and explicitly equal values intern to the same Node. Pure fixed and
variadic Node inputs, plus managed Node values themselves, must not be defaults.
Built-in `text_file` and `symlink_file` inputs remain explicit.

## Mixed Node/value inputs

A fixed union such as `Bam | str | None` is one positional logical input. A
managed Node selects a NodeType arm, creates a parent edge, and contributes
lineage. A matching plain value creates no edge and contributes a named canonical
`input_values` entry plus positional order to provenance. `CommandArgs.inputs`
exposes a resolved Path for the Node branch or the unchanged plain value for the
value branch. Mixed variadic element unions remain unsupported.

## Callable commands and fingerprint v4

`command()` accepts a static shell string or a module-level, closure-free,
source-inspectable Python function/lambda with one `CommandArgs` argument.
Callbacks receive resolved named input paths or mixed values, output paths,
config, constraints, and `workdir`; they return one complete valid shell string.
The string is executed unchanged, so callback authors own shell quoting. List
commands are rejected.

Every canonical rule invocation owns one shared `RuleCall`; co-outputs share
its 64-hex rule hash, 64-hex provenance hash, and once-per-output-root realized
command. Paths are `{rule}/{provenance_hash}/{filename}`. Equivalent calls made through
Pipelines sharing a DAG return the same RuleCall and Node objects immediately.

The rule hash covers local recipe structure and declared contracts. The
provenance hash covers rule hash, effective config, shell, exact parent lineage,
and any selected mixed plain values. Both
use framed canonical serialization. Callable command identity uses canonical
AST plus Python implementation/version. Fingerprinting is framework-owned.

## Variadic Node inputs

`tuple[NodeType, ...]` declares an ordered variadic Node group and accepts an
actual tuple as one positional rule argument. `Annotated[tuple[NodeType, ...],
Many()]` defaults to at least one element; `min` and `max` are inclusive and
`max=None` is unbounded. Several groups may coexist with fixed Node inputs.
RuleCall and fingerprint contexts retain named tuples, while `RuleCall.parents`
flattens them in declaration/element order for graph traversal. `CommandArgs`
contains tuples of resolved Paths. Static placeholders quote every group path
separately; callable commands control custom layouts. Grouping, order, names,
and `Many` bounds affect the default fingerprint.

## Abstract NodeType contracts

A `NodeType` with `filename = None` is an input-only format contract. Rules may
accept it and receive Nodes of concrete subclasses, but every declared output
NodeType must resolve to a non-`None` filename. `Rule` construction rejects
filename-less outputs immediately; output names are not filename fallbacks.

## RuleCall cache

RuleCall is atomic cache, state, scheduling, execution, report, and cleanup unit. Requesting one co-output activates all declared outputs; only CLI result copying remains Node-selective. Consumer `dependencies.toml` records `consumed_hash` (tagged `<hasher>:<hex>`, BLAKE3 by default) for every parent Node. Child classification waits for parent settlement: identical rebuilt bytes preserve cache, changed bytes replay. Output hashes use mtime only to invalidate the stored-hash fast path.

Persistent state that changes outside the DAG does not belong in a node workdir: a workdir is a function of its declared inputs, so any identity change gives a fresh empty one. Such state belongs outside the node store, passed in as a path. A `mutable=True` Rule flag existed for this until 2026-09-16 and was removed -- it stopped external byte changes from staling consumers, but could do nothing about the workdir itself being discarded whenever identity moved, which is the failure that actually costs work.

## NodeType invalidators

`NodeType.invalidator` is optional and defaults to `None`. When set, it is a callable receiving the concrete `Node` and returning a stable `str` token. Necroflow stores the token at `.rip/{filename}.invalidation` after a successful run. During classification, an existing output with a missing or changed token is marked `STALE`; callback exceptions fail fast. The token does not participate in the node fingerprint.

## Path limit checks

`DAG(nodes_dir)` owns the absolute node-store root and every `Pipeline(dag, ...)`
references that registry. Every rule call
computes and validates its full fingerprint and assigns each output's final
absolute path immediately. Path checks cover component byte lengths against
`PC_NAME_MAX` and the full path byte length against `PC_PATH_MAX`, using
`os.pathconf()` on the nearest existing parent. Violations raise during the rule
call, before assignment or execution.

Pipeline item labels may independently be canonical relative POSIX paths, for
example `P["dataset/config"]`. Assignment validates every component against the
portable Linux `NAME_MAX` of 255 encoded bytes, validates the label plus output
filename against `PATH_MAX` 4096, and rejects absolute paths, dot components,
dot-prefixed components, non-canonical separators, and file/directory result
conflicts. These labels only select visible result copies and do not affect
fingerprints. CLI run and outputs commands validate the complete absolute result
paths against the destination filesystem before execution; copy materialization repeats
the check defensively. Doctor reports failures as `NF_RESULT_PATH_INVALID`.

## Scheduling

FIFO RuleCall scheduling is built-in and default. Active calls preserve canonical `DAG.calls` insertion order. Custom three-argument schedulers receive ready calls, remaining calls, and available capped resources.

## Rule retries

`@command(..., repeat=N)` sets the maximum number of command attempts,
including the first; the default `repeat=1` does not retry. A failed subprocess
is retried until one attempt succeeds or all `N` attempts fail. Other failures
are not retried. Retries remain one scheduler submission and `repeat` is not a
scheduler resource. V3 identity excludes retry policy.

## CLI forced invalidation

The CLI accepts repeated `--invalidate LABEL` and `--reap NAME` options. Labels resolve to owning RuleCall paths passed through `run(..., forced_stale_call_keys=...)`. Forced invalidation applies only inside the already requested call closure and does not request extra outputs.

## Job config validation

The CLI accepts repeatable `--validation PATH.py:FUNCTION` flags. Each validator is a Python callable receiving the expanded, metadata-stripped job config dict, exactly like the workflow. Validators run after `__grid` expansion and before workflow evaluation; they should raise to reject malformed configs. This is callback-based because raw job TOML can contain grids, so pre-validating the unexpanded file is not equivalent to validating the concrete configs workflows receive.

`necroflow.config.iter_job_configs()` is intentionally validation-free: it yields expanded, metadata-stripped `JobConfig` objects. Python-only callers that want validation should call their validator explicitly inside the `for job in iter_job_configs(...)` loop. Cerberus is an optional extra via `necroflow[validation]`; core necroflow does not import it unless user validator code does.

## Execution reports

`run()` returns `dict[str, RuleCallExecution]` keyed by each RuleCall stable POSIX relative path. Each event nests every declared output Node key/path and owns call-level duration, result size, state, and cache status. `DAG.run()` stores the same normally returned dict as `dag.last_execution_report`. `.rip/run.toml` and CLI `execution.toml` use the same call unit, so co-output time is never double-counted. Cached calls have no new duration. Keep-going exceptions carry the call-keyed report.

## CLI output roots

The CLI separates hashed node storage from job-facing copies. `--nodes-dir DIR`
controls the node store and defaults to `nodes`; `--results-dir DIR` controls
per-job copied-output folders and defaults to `results`. `--outdir DIR` / `-o DIR`
remains a compatibility alias that uses one directory for both and cannot be
combined with either split-dir flag. Manifest keys are exact requested Pipeline
labels. Each manifest entry records the visible path, canonical origin node key,
and content hash. Linux uses `cp -a --reflink=auto`; macOS uses `cp -a -c`.

### CLI JSON node paths

`src/necroflow/cli.py` serializes `outputs.node_relative_path` and the
`relative_path` fields on Nodes in `graph` and `explain` from
`node.relative_path.as_posix()`. These values equal the corresponding node key
and stay relative to the DAG node store. The CLI root spelling may be relative
or a symlink while the DAG root is resolved, so recomputing these fields with
`node.path.relative_to()` against the CLI argument is incorrect.
`tests/test_cli.py::test_json_node_paths_are_relative_to_store` covers default,
relative, absolute, legacy `--outdir`, and symlink roots for all three commands.

## Built-in text file rules

`text_file_rule(name, output, input_name="text", encoding="utf-8")` returns a single-output rule that writes a string config value directly to the output file. It is intended for large tool configs that come from job TOML tables, e.g. serialize `config["sage"]` with `json.dumps(..., sort_keys=True, indent=2) + "\n"` and pass it as `text`.

Text-file rules do not run a shell command. The executor calls the built-in materializer, which avoids quoting problems and command-line length limits from `printf`-style config dumping. Their fingerprints hash the stable recipe identity (`necroflow.text_file/v1:...`) instead of command text; the string payload is still included through normal node config hashing.

## Config update helper tool

`necroflow-config-set` is a packaged console script implemented in `necroflow.tools.config_set`. It copies a `.toml` or `.json` config, reads a dotted source field from another TOML/JSON config, writes that value to a dotted target field, and saves using the same extension as the copied input. Missing target tables are created; missing source fields and input/output extension mismatches fail. Use this as a normal rule command when runtime file content should influence a downstream tool config without dynamic Python DAG expansion.

## Canonical template and CLI inspection

`necroflow init DIR` copies the packaged canonical workflow from `src/necroflow/templates/canonical`. Keep that template byte-for-byte aligned with `examples/canonical`, which is the browsable reference copy in the repo. The template demonstrates the CLI-first shape: `pipeline.py`, `job.toml`, optional `job_grid.toml`, `schema.py`, `reap.toml`, and small input fixtures.

The CLI has subcommands while preserving legacy direct runs: `necroflow JOB.toml` and `necroflow --nodes-dir nodes JOB.toml` are coerced to `necroflow run ...`. Introspection commands support agent-friendly JSON: `necroflow graph --json JOB.toml`, `necroflow outputs --json JOB.toml`, and `necroflow provenance --json PATH`. `necroflow doctor [--json] JOB.toml` performs preflight checks and emits stable `NF_*` issue codes. `necroflow explain [--json] [--node LABEL] JOB.toml` reports ordered RuleCalls with nested output Nodes. Descendants of calls that would run remain unknown (`state` and `will_run` null, reason `parent_will_run`) until real parent bytes exist.

Package version is exposed as `necroflow.__version__`; `pyproject.toml` reads it dynamically via setuptools. Packaged data must include `templates/canonical/*` so `necroflow init` works after installation.

## Constraint command placeholders

Rule constraints can be interpolated into command templates. `{threads}` always resolves: it uses the declared `threads` constraint or defaults to `1`. Other direct placeholders, such as `{ram}` or `{gpu}`, are allowed only when that constraint is declared. `{constraint:name}` forces a constraint lookup and is useful when a normal config input has the same name, e.g. `{threads}` can remain the config value while `{constraint:threads}` is the scheduler thread requirement. Command-facing values are raw declared constraint values (`"32Gi"` stays `"32Gi"`); executor resource accounting still uses parsed integer values via `Rule.resources`.

## Docker execution

`{env}:` at the start of a command runs it in Docker when `env` is annotated
`Docker`; callbacks return the literal prefix. `Docker` is read-only config, so
image, platform, and run args are lineage. See `docs/rules.md#docker-execution`.
Docker is the only runtime; Podman/Apptainer are not implemented.

## Shellpath

`Pipeline(..., shellpath=PATH)` and CLI `--shellpath PATH` choose the executable
shell via `subprocess.run(..., shell=True, executable=PATH)`. The default
remains Python's normal `shell=True` behavior and is not fingerprint-salted.
Explicit shellpaths are normalized, stored directly on each command
`RuleCall`, included in all command-rule fingerprints, and written to
provenance. Built-in materializers remain unaffected. The shell selection is
immutable for the lifetime of a compiled pipeline; there is no late key
rebuild.
