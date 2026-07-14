# Command-Line Interface

[Previous: Where Outputs Live and Caching](caching.md) | [README](../README.md) | [Next: Job TOML and Parameter Grids](job-toml.md)

## Command-line interface

necroflow ships a `necroflow` command. Each positional argument is a **job TOML** — a self-contained file that specifies the pipeline factory, optional requested outputs, and user config params.

```bash
necroflow [--nodes-dir nodes] [--results-dir results] [-c N|all] \
          [--constraint KEY=VALUE ...] [--keep-going] [--autoclean] [--dry-run] \
          [--invalidate LABEL ...] [--reap NAME ...] [--reap-file PATH] \
          [--validation PATH.py:FUNCTION ...] [--scheduler NAME|PATH.py:FUNCTION] [--shellpath PATH] \
          JOB.toml [JOB2.toml ...]
```

| Flag | Meaning |
|---|---|
| `--nodes-dir DIR` | Hashed node output store (default: `nodes`). |
| `--results-dir DIR` | Per-job symlink and manifest directory (default: `results`). |
| `--outdir DIR` / `-o DIR` | Compatibility alias that uses one directory for both node outputs and job links. Cannot be combined with `--nodes-dir` or `--results-dir`. |
| `-c N` / `-call` | Thread cap — integer or `all` (default: all CPUs). |
| `--constraint KEY=VALUE` | Additional resource cap. Repeatable. Accepts SI/binary suffixes. |
| `--keep-going` / `-k` | Continue past failures; collect all errors at the end. |
| `--autoclean` | Delete orphan outputs and intermediate rule-call directories, including `{workdir}` side files. |
| `--dry-run` / `-n` | Show what would run without executing. |
| `--invalidate LABEL` | Force an already-requested pipeline label to rerun. Repeatable. |
| `--reap NAME` | Force labels listed under `NAME` in `reap.toml` to rerun. Repeatable. |
| `--reap-file PATH` | TOML file for named invalidation sets (default: `reap.toml`). |
| `--validation PATH.py:FUNCTION` | Validate each expanded job config with a Python callable. Repeatable. |
| `--scheduler NAME|PATH.py:FUNCTION` | Run-only scheduler: `connected-components` (default), `fifo`, or a local three-argument callable. |
| `--shellpath PATH` | Executable shell for string commands, e.g. `/bin/bash`. Defaults to Python's system shell behavior. |

```bash
necroflow --invalidate counts job.toml
necroflow --reap quick --reap-file reap.toml job.toml
```

`--invalidate` and `--reap` do not override `.requests` and do not request extra outputs. They only mark matching labels stale when those labels are already in the active requested subgraph. A `reap.toml` file contains top-level named label lists:

```toml
quick = ["counts", "qc"]
```

Use `--shellpath` when a command needs shell-specific syntax such as Bash brace expansion:

```bash
necroflow --shellpath /bin/bash job.toml
necroflow outputs --shellpath /bin/bash job.toml
```

Explicit shell paths affect node hashes for string commands, so `outputs --shellpath PATH` reports the same paths that `run --shellpath PATH` will produce.

## Project scaffolding

Create a starter workflow with:

```bash
necroflow init my-workflow
```

The command copies the canonical template into `my-workflow`. It refuses to write into a non-empty directory unless `--force` is passed.

## Introspection commands

Render a DAG without running jobs:

```bash
necroflow graph job.toml
necroflow graph --output graph.txt job.toml
necroflow graph --json job.toml
necroflow graph --png graph.png job.toml
```

`--png` requires the `dev` extra and Graphviz `dot`. When every displayed rule call has one unambiguous `P.section(...)` label, the PNG uses labelled section clusters; otherwise it groups nodes by dependency depth.

List requested output paths without running jobs:

```bash
necroflow outputs job.toml
necroflow outputs --json job.toml
```

Print stored provenance for an existing cached output:

```bash
necroflow provenance nodes/rule/hash/file
necroflow provenance --json nodes/rule/hash/file
```

Run preflight checks without executing rules:

```bash
necroflow doctor job.toml
necroflow doctor --json job.toml
```

`doctor` reports stable issue codes such as `NF_CONFIG_MISSING_PIPELINE`,
`NF_PIPELINE_IMPORT_FAILED`, `NF_VALIDATION_FAILED`,
`NF_REQUEST_LABEL_NOT_FOUND`, `NF_SHELLPATH_INVALID`, `NF_NODESTORE_LOCKED`,
`NF_OUTPUT_PATH_TOO_LONG`, and `NF_RESOURCE_INVALID`. It exits non-zero when
any error issue is present.

Explain what would run and why without executing rules:

```bash
necroflow explain job.toml
necroflow explain --node counts job.toml
necroflow explain --json job.toml
```

`explain` reports requested nodes and ancestors, predicted paths, state, command,
resource constraints, whether each node would run, and best-effort reasons such
as `output_missing`, `up_to_date`, `parent_not_up_to_date`,
`parent_content_changed`, `forced_invalidation`, `invalidator_changed`, and
`compromised_prior_state`.

[Previous: Where Outputs Live and Caching](caching.md) | [README](../README.md) | [Next: Job TOML and Parameter Grids](job-toml.md)
