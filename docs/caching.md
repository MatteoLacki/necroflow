# Where Outputs Live and Caching

[Previous: README](../README.md) | [README](../README.md) | [Next: Command-Line Interface](cli.md)

## Where outputs live

`DAG("some-dir")` writes the real content-addressed node outputs directly under that directory. The CLI defaults to a split layout: hashed node outputs under `nodes/`, plus one user-facing subfolder per job/grid combo under `results/`:

```
nodes/
  {rule}/{hash16}/{file}           ← real node outputs (content-addressed)

results/
  experiment__ref+hg38__aligner+bwa/
    {node_name}/{file}             ← symlinks to requested node outputs only
    manifest.toml                  ← requested output paths for this combo
  experiment__ref+hg38__aligner+bowtie2/
    ...
```

Only the **requested** outputs (defaults to pipeline sinks) get a symlink — intermediate ancestors are excluded. `node_name` is the Pipeline attribute name assigned in the factory, and the file name is the declared `NodeType.filename` when present:

```text
results/experiment__ref+hg38__aligner+bwa/counts/counts.txt
```

`manifest.toml` lists the same visible result paths keyed by node name:

```toml
[outputs]
counts = "counts/counts.txt"
```

The key (`counts`) matches `P.counts = R.count(...)` in the factory function.

See `examples/necroalchemy_grid.toml` and `examples/necroalchemy_factory.py`
for a runnable example.

## Caching

Each hashed node output lives at `nodes/{rule}/{hash16}/{filename}` by default. The 16-character hash captures the entire upstream config chain, including rule name, command, config values, parent fingerprints, and declared `Inputs`/`Outputs` types (`Constraints` are excluded — execution resources don't affect output identity).

During path resolution, necroflow validates generated paths against the filesystem's `NAME_MAX` and `PATH_MAX` limits. If a rule name, filename, output directory, or complete generated path would exceed those limits, path resolution fails before execution starts.

### Rule work directories

Commands may use the built-in `{workdir}` placeholder to refer to the rule-call output directory: `nodes/{rule}/{hash16}` by default. Use it for tools that need to write a directory of side files or temporary computation products that should live next to the declared outputs:

```python
@r.command("dosomething --tmp {workdir}/scratch -o {result}")
def compute(input: Input):
    return Result[result]
```

The `{workdir}` directory is created before the command runs. Files written there are kept by default, just like declared outputs, because the directory is part of the cached rule-call result. The name `workdir` is reserved for this built-in placeholder and cannot be used as an input or output name.

Command template validation is intentionally strict about outputs: every declared output name must appear in the command template. Declared inputs and config values may be unused; if they are passed to the rule call, they still participate in the output fingerprint. Any placeholder that does appear must be a declared input, a declared output, or a built-in placeholder such as `{workdir}`.

### Custom invalidation

A `NodeType` may define an optional `invalidator` callback next to `filename`. The callback receives the concrete `Node` and must return a stable string token. Necroflow stores that token under the node's `.rip/` metadata after a successful run and marks the node `STALE` if the token is missing or changes later.

```python
import hashlib
from pathlib import Path

def sha256_of_path(node):
    return hashlib.sha256(Path(node.config["path"]).read_bytes()).hexdigest()

class ToolBinary(NodeType):
    filename = "tool.ready"
    invalidator = sha256_of_path
```

Use this for external dependencies that should invalidate a cached node without becoming normal necroflow outputs, such as a binary, script, or selected source tree hash. Types without `invalidator` use the normal cache behavior. If the callback raises, execution fails fast instead of guessing whether the cache is valid.

Invalidators are evaluated during the initial node classification at the start of `execute()`. After a job succeeds, necroflow recomputes and stores the token for that node's outputs, but it does not re-run all invalidators between tasks in the same execution. If an external dependency changes while a pipeline is already running, that change is detected on the next `execute()` invocation.

- Re-running with the same inputs is a no-op (cache hit).
- Changing any upstream parameter, command, or declared type produces a new path — old results are never overwritten.
- A parent whose mtime is newer than a child triggers a content-hash check: if the parent's bytes are unchanged, the child is **not** re-run. Only a genuine content change marks children STALE.
- Each output folder contains a `.rip/` subdirectory with:
  - `dependencies.toml` — full accumulated config for provenance.
  - `{filename}.hash` — SHA-256 content hash, used for STALE detection on the next run.
  - `job.log` — captured stdout/stderr.
  - `state` — last recorded run state (`running` / `up_to_date` / `failed` / `interrupted`). If a process is killed mid-run the `state` file is left as `running`; on the next invocation necroflow detects this and re-runs the node even if its output exists on disk.

## Concurrency

**Only one necroflow instance may run against a given node store at a time.** `execute()` acquires an exclusive lock on `nodes/.rip/necroflow.lock` (via `fcntl.flock`) at startup and releases it on exit. A second instance targeting the same node store will fail immediately with a clear error. Running two instances against *overlapping* node stores (e.g. `nodes` and `nodes/sub`) is unsupported — there is no OS primitive to detect this, so avoid it.

[Previous: README](../README.md) | [README](../README.md) | [Next: Command-Line Interface](cli.md)
