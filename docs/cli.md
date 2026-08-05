# Command-Line Interface

[Previous: Where Outputs Live and Caching](caching.md) | [README](../README.md) | [Next: Job TOML and Parameter Grids](job-toml.md)

## Command-line interface

necroflow ships a `necroflow` command. Each positional argument is a **job
TOML** — a self-contained file that specifies the pipeline factory, optional
requested outputs, and user config params. For each expanded job, the CLI
constructs one shared `DAG(nodes_dir)`, creates each
`Pipeline(dag, shellpath=...)`, and calls
`factory(P, config)`. Rule calls intern immediately; after the factory returns,
the CLI calls `P.finish()`, then resolves requested labels or sinks with
`dag.require(...)`. Qualified labels created by `P.subpipeline(prefix)` are
requested with their full paths, for example `samples/A/counts`.

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
| `--results-dir DIR` | Per-job copied outputs and manifests (default: `results`). |
| `--outdir DIR` / `-o DIR` | Compatibility alias that uses one directory for both canonical node outputs and result copies. Cannot be combined with `--nodes-dir` or `--results-dir`. |
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

Fingerprinting is framework-owned. A job containing `.fingerprint` is rejected.

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

`--png` requires the `dev` extra and Graphviz `dot`; it groups rule calls by dependency depth. Mutable Nodes are marked in the ASCII view, mutable Graphviz edges are dashed and labelled, and JSON Nodes and edges include a `mutable` boolean.

List requested output paths without running jobs:

```bash
necroflow outputs job.toml
necroflow outputs --json job.toml
```

Print stored provenance for an existing cached output:

```bash
necroflow provenance nodes/rule/rule_hash/provenance_hash/file
necroflow provenance --json nodes/rule/rule_hash/provenance_hash/file
```

Delete cache entries that cannot have been produced by the current collection
of pipeline rules:

```bash
necroflow gc --nodes-dir nodes --gc-rules-script gc_rules.py
necroflow gc --nodes-dir nodes --gc-rules-script gc_rules.py -y
```

The script states the preserved scope explicitly as a non-empty list of `Rule`
objects:

```python
from pipeline import align, count_reads, prepare_reference

rules = [align, count_reads, prepare_reference]
```

Any expression producing that list works, so rules built by `text_file_rule`,
`symlink_file_rule`, or the `command(...)` factory and stored in a container are
declared with `rules = list(REGISTRY.values())`. A renamed or deleted rule makes
the script fail at import, which is deliberate: silent under-discovery is the
one failure mode that deletes live nodes.

GC prints provenance-incompatible rule-call directories and folders that fail
the current layout or metadata checks in separate batches, followed by their
total size. It then asks `Delete these directories? [y/N]`; `-y` skips
confirmation. The node-store lock is held while scanning and deleting. Rule
calls with mutable outputs are protected from provenance-based deletion;
malformed or non-current folders are not. A current local rule call is also
deleted when its recorded provenance descends from an obsolete parent rule. GC
has no job TOML dependency and never evaluates concrete job configurations.

A rule *name* present in the node store but absent from `rules` is reported
under `Rules absent from ...` and preserved, together with everything descending
from it — an undeclared name is a forgotten import far more often than a deleted
rule. Pass `--prune-unknown-rules` to collect those nodes once the deletion is
genuinely intended.

Run preflight checks without executing rules:

```bash
necroflow doctor job.toml
necroflow doctor --json job.toml
```

See [Doctor preflight checks](doctor.md) for its stable issue codes, execution
boundary, filesystem probes, informational findings, and exit semantics.

Explain what would run and why without executing rules:

```bash
necroflow explain job.toml
necroflow explain --node counts job.toml
necroflow explain --json job.toml
```

`explain` reports requested nodes and ancestors, predicted paths, state, command,
resource constraints, whether each node would run, and best-effort reasons such
as `output_missing`, `up_to_date`, `parent_not_up_to_date`,
`parent_content_changed`, `mutable_parent_content_ignored`,
`forced_invalidation`, `invalidator_changed`, and `compromised_prior_state`.

[Previous: Where Outputs Live and Caching](caching.md) | [README](../README.md) | [Next: Job TOML and Parameter Grids](job-toml.md)
