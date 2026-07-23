# Where Outputs Live and Caching

[Previous: README](../README.md) | [README](../README.md) | [Next: Command-Line Interface](cli.md)

## Where outputs live

`DAG("some-dir")` writes the real lineage-addressed node outputs directly under that directory. The CLI defaults to a split layout: hashed node outputs under `nodes/`, plus one user-facing subfolder per job/grid combo under `results/`:

```
nodes/
  {rule}/{fingerprint}/{file}      ← real node outputs (64-hex content address)

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

The key (`counts`) matches `P.counts = count(...)` in the factory function.

See `examples/necroalchemy_grid.toml` and `examples/necroalchemy_factory.py`
for a runnable example.

## Caching

Each hashed node output lives at `nodes/{rule}/{fingerprint}/{filename}` by default.
Fingerprint v2 calculates a full 64-character SHA-256 digest from a
type-tagged, length-framed representation of the rule name, command, config,
full parent digests, execution context, and declared `Inputs`/`Outputs` types.
`node.fingerprint` exposes that complete digest and paths use it without
truncation. `node.relative_path` is the canonical Path relative to the node
store. Constraints and `repeat` are excluded.

Full-fingerprint paths intentionally replace the earlier 16-character path
form. Old cache directories are not probed, migrated, reused, or deleted
automatically.

During path resolution, necroflow validates generated paths against the filesystem's `NAME_MAX` and `PATH_MAX` limits. If a rule name, filename, output directory, or complete generated path would exceed those limits, path resolution fails before execution starts.

### Rule work directories

Commands may use the built-in `{workdir}` placeholder to refer to the rule-call output directory: `nodes/{rule}/{fingerprint}` by default. Use it for tools that need to write a directory of side files or temporary computation products that should live next to the declared outputs:

```python
@command("dosomething --tmp {workdir}/scratch -o {result}")
def compute(input: Input):
    result = output(Result)
    return result
```

The `{workdir}` directory is created before the command runs. Files written there are kept by default, just like declared outputs, because the directory is part of the cached rule-call result. The name `workdir` is reserved for this built-in placeholder and cannot be used as an input or output name.

Declared inputs, config values, and outputs may be unused by a command; they
still participate in identity. Any static-template placeholder that does
appear must be a declared input/output, a command-visible constraint, or a
built-in placeholder such as `{workdir}`.

The built-in v2 fingerprint canonically supports ordinary scalar values,
paths, dates/times, sequences, string-keyed mappings, and sets. Unsupported
custom objects fail with a diagnostic identifying the config field.

Projects may replace the complete policy:

```python
from necroflow import FingerprintArgs, default_fingerprint

def project_fingerprint(args: FingerprintArgs) -> str:
    digest = default_fingerprint(args)
    # Return any complete 64-character lowercase hexadecimal digest.
    return digest
```

Select it when constructing a pipeline with
`Pipeline(dag, fingerprint_function=project_fingerprint,
fingerprint_provider="project.project_fingerprint/v1")`, or in job TOML with
`".fingerprint" = "hashing.py:project_fingerprint"`. The function
receives the original command and all logical rule-call fields, before output
paths are derived. It replaces the default but may call
`default_fingerprint(args)` to compose standard identity with project-specific
dependencies. Each rule call computes this digest and its absolute output paths
immediately.

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

### External dataset ingestion

A path passed as a bare string config value (e.g. `align(fastq="/data/sample.fastq")`)
is fingerprinted as *text* — necroflow hashes the path string, never the file's
bytes. Editing that file in place changes nothing necroflow can see: the
downstream node stays `UP_TO_DATE` forever. This applies with no ingestion
node at all; there is nothing to compare against.

The fix is to ingest the file through its own rule, symlinking it in. Use the
built-in `@symlink_file` decorator instead of hand-writing the same
`ln -s` command for every dataset type:

```python
@symlink_file
def raw_spectra(path: str):
    spectra = output(Mzml)
    return spectra
P.spectra = raw_spectra(P, path=config["spectra"])
```

`$(realpath ...)` resolves to an absolute path so the symlink survives if the
working directory changes; see `examples/sage_recal/pipeline.py` for a
runnable version. Once the file is behind a symlinked node, the normal
mtime-fast-path / content-hash mechanism described above applies automatically:
`Path.stat()` follows the symlink to the real file, so editing it bumps the
mtime necroflow sees, the fast path fails, the content hash is recomputed and
found to differ from the value stored in `.rip/{filename}.hash`, and every
downstream consumer is correctly marked `STALE` and reruns. No
`NodeType.invalidator` is needed for this case — the existing STALE machinery
already covers it once the file is a real node in the DAG.

This is still **in-place overwrite**, not content-addressed versioning: the
ingestion node's `node.relative_path` is fixed by the path string, not by file content,
so a rerun always lands in the same directory, overwriting the previous
result. There is no side-by-side history of dataset versions. If that is
needed, the caller has to bake a distinguishing token (a version tag, a date,
a checksum) into the rule's own config themselves — necroflow does not derive
one from file content automatically.

Do not ingest with a plain copy (`cp {path} {output}`) expecting the same
detection: copying freezes the content at that moment, and nothing ever
revisits the original path again, so a later edit to the source file goes
unnoticed. Use `ln -s`, not `cp`, when the goal is to detect upstream changes.
(A `cp` import is still the right choice for the *config-file* case in
[Generated Config Files](generated-config-files.md#external-config-files),
where the accompanying `NodeType.invalidator` explicitly re-reads the original
path on every classification — that pattern doesn't rely on the symlink/mtime
trick at all.)

## Concurrency

**Only one necroflow instance may run against a given node store at a time.** `execute()` acquires an exclusive lock on `nodes/.rip/necroflow.lock` (via `fcntl.flock`) at startup and releases it on exit. A second instance targeting the same node store will fail immediately with a clear error. Running two instances against *overlapping* node stores (e.g. `nodes` and `nodes/sub`) is unsupported — there is no OS primitive to detect this, so avoid it.

[Previous: README](../README.md) | [README](../README.md) | [Next: Command-Line Interface](cli.md)
