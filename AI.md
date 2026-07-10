# necroflow AI Notes

For a compact map of the current software surface, start with `features.txt`.

## Rule command placeholders

Rule commands are validated when a `Rule` is registered. Placeholders are limited to declared input names, declared output names, and built-in command placeholders.

Built-in placeholders:

- `{workdir}` resolves to the rule-call output directory inside the node store, `nodes/{rule}/{hash16}` by default. Use it for tools that need to write side directories or scratch files that should be retained with the cached result. The name `workdir` is reserved and cannot be used as an input or output name.

`{workdir}` is created before the command subprocess starts. Its contents are kept by default. With `autoclean=True`, intermediate rule-call directories are removed as whole directories once all active children are up to date, so `{workdir}` side files are cleaned together with declared outputs.

## NodeType invalidators

`NodeType.invalidator` is optional and defaults to `None`. When set, it is a callable receiving the concrete `Node` and returning a stable `str` token. Necroflow stores the token at `.rip/{filename}.invalidation` after a successful run. During classification, an existing output with a missing or changed token is marked `STALE`; callback exceptions fail fast. The token does not participate in the node fingerprint.

## Path limit checks

`resolve_paths()` validates each generated path before assigning `node.path`. It checks component byte lengths against `PC_NAME_MAX` and the full path byte length against `PC_PATH_MAX`, using `os.pathconf()` on the nearest existing parent. Violations raise `ValueError` before execution. Tests monkeypatch `_filesystem_limits()` for deterministic `NAME_MAX` and `PATH_MAX` cases.

## Rule repeat metadata

Rules accept `repeat=N` via `R.register(...)`, `@r.command(...)`, and `@r.rule(...)`. `repeat` is validated as a positive integer and stored as `rule.repeat`. It is compatibility metadata only: not a scheduler resource, not an execution multiplier, and not part of node fingerprints.

## CLI forced invalidation

The CLI accepts repeated `--invalidate LABEL` and `--reap NAME` options. `--reap` expands labels from a top-level `reap.toml` table shaped like `name = ["label", ...]`; `--reap-file PATH` overrides the default file. Labels resolve to pipeline labels for each expanded pipeline, then to node keys passed into `execute(..., forced_stale_keys=...)`. The executor only marks active requested nodes stale, and then propagates STALE to active descendants. Invalidation does not request extra outputs.

## Job config validation

The CLI accepts repeatable `--validation PATH.py:FUNCTION` flags. Each validator is a Python callable receiving the expanded, metadata-stripped job config dict, exactly like the pipeline factory. Validators run after `__grid` expansion and before factory construction; they should raise to reject malformed configs. This is callback-based because raw job TOML can contain grids, so pre-validating the unexpanded file is not equivalent to validating the concrete configs factories receive.

`necroflow.config.iter_job_configs()` is intentionally validation-free: it yields expanded, metadata-stripped `JobConfig` objects. Python-only callers that want validation should call their validator explicitly inside the `for job in iter_job_configs(...)` loop. Cerberus is an optional extra via `necroflow[validation]`; core necroflow does not import it unless user validator code does.

## CLI output roots

The CLI separates hashed node storage from job-facing links. `--nodes-dir DIR` controls the node store and defaults to `nodes`; `--results-dir DIR` controls per-job symlink folders and defaults to `results`. `--outdir DIR` / `-o DIR` remains a compatibility alias that uses one directory for both and cannot be combined with either split-dir flag. Manifests list requested output paths relative to the node store.

## Built-in text file rules

`Rules.text_file(name, output, input_name="text", encoding="utf-8")` registers a single-output rule that writes a string config value directly to the output file. It is intended for large tool configs that come from job TOML tables, e.g. serialize `config["sage"]` with `json.dumps(..., sort_keys=True, indent=2) + "\n"` and pass it as `text`.

Text-file rules do not run a shell command. The executor calls the built-in materializer, which avoids quoting problems and command-line length limits from `printf`-style config dumping. Their fingerprints hash the stable recipe identity (`necroflow.text_file/v1:...`) instead of command text; the string payload is still included through normal node config hashing.

## Canonical template and CLI inspection

`necroflow init DIR` copies the packaged canonical workflow from `src/necroflow/templates/canonical`. Keep that template byte-for-byte aligned with `examples/canonical`, which is the browsable reference copy in the repo. The template demonstrates the CLI-first shape: `pipeline.py`, `job.toml`, optional `job_grid.toml`, `schema.py`, `reap.toml`, and small input fixtures.

The CLI has subcommands while preserving legacy direct runs: `necroflow JOB.toml` and `necroflow --nodes-dir nodes JOB.toml` are coerced to `necroflow run ...`. Introspection commands are intentionally lightweight: `necroflow graph JOB.toml` renders the DAG, `necroflow outputs JOB.toml` lists requested node/result paths, and `necroflow provenance PATH` reads the existing `.rip/dependencies.toml` for a cached output. Rich dry-run/explain output is not implemented yet.

Package version is exposed as `necroflow.__version__`; `pyproject.toml` reads it dynamically via setuptools. Packaged data must include `templates/canonical/*` so `necroflow init` works after installation.

## Constraint command placeholders

Rule constraints can be interpolated into command templates. `{threads}` always resolves: it uses the declared `threads` constraint or defaults to `1`. Other direct placeholders, such as `{ram}` or `{gpu}`, are allowed only when that constraint is declared. `{constraint:name}` forces a constraint lookup and is useful when a normal config input has the same name, e.g. `{threads}` can remain the config value while `{constraint:threads}` is the scheduler thread requirement. Command-facing values are raw declared constraint values (`"32Gi"` stays `"32Gi"`); executor resource accounting still uses parsed integer values via `Rule.resources`.

## Shellpath execution context

`execute(..., shellpath=PATH)` and CLI `--shellpath PATH` choose the executable shell for string commands via `subprocess.run(..., shell=True, executable=PATH)`. The default remains Python's normal `shell=True` behavior and is not fingerprint-salted. Explicit shellpaths are normalized to absolute executable files, stored in `node.execution_context["shellpath"]`, included in fingerprints for string-command nodes, and written to dependencies provenance under `[execution]`. List commands and built-in materializers never receive shellpath context. DAGs must rebuild their deduplication index after shell context changes because node keys depend on fingerprints.
