# Where Outputs Live and Caching

[Previous: README](../README.md) | [README](../README.md) | [Next: Command-Line Interface](cli.md)

## Where outputs live

`DAG("some-dir")` writes the real lineage-addressed node outputs directly under that directory. The CLI defaults to a split layout: hashed node outputs under `nodes/`, plus one user-facing subfolder per job/grid combo under `results/`:

```
nodes/
  {rule}/{rule_hash}/{provenance_hash}/{file}  ← canonical node outputs

results/
  experiment__hg38__bwa/
    {node_name}/{file}             ← copies of requested node outputs only
    manifest.toml                  ← paths, origin node keys, and content hashes
  experiment__hg38__bowtie2/
    ...
```

Grid combo labels default to a short, deduplicated form (`--long-names` restores
full nested parameter paths like `experiment__ref+hg38__aligner+bwa`); see
[Job TOML and Parameter Grids](job-toml.md).

Only the **requested** outputs (defaults to pipeline sinks) are copied —
intermediate ancestors are excluded. On Linux, Necroflow uses GNU
`cp -a --reflink=auto`; on macOS it uses `cp -a -c`. Both opportunistically
create copy-on-write clones and fall back to physical copying when cloning is
unavailable. Archive mode preserves an intentionally symlink-valued output as
a symlink instead of dereferencing it.

```text
results/experiment__hg38__bwa/counts/counts.txt
```

`manifest.toml` identifies every owned result and its canonical source:

```toml
[outputs.counts]
path = "counts/counts.txt"
origin_node_key = "count/<rule_hash>/<provenance_hash>/counts.txt"
content_sha256 = "<64 lowercase hexadecimal characters>"
```

The key (`counts`) matches `P.counts = count(...)` in the factory function.

See `examples/necroalchemy_grid.toml` and `examples/necroalchemy_factory.py`
for a runnable example.

## Caching

Fingerprint v3 splits identity into two full SHA-256 values:

- `rule_hash` describes the local recipe: rule name, command or built-in recipe
  identity, declared input contracts, output contracts, filenames, and Rule mutability.
- `provenance_hash` describes one invocation: the `rule_hash`, effective config,
  selected shell, and ordered parent identities, including their rule hashes,
  provenance hashes, and output names. Parent rule hashes already encode mutability.

Each output lives at
`nodes/{rule}/{rule_hash}/{provenance_hash}/{filename}`. The canonical
rule-call key omits the filename; the canonical node key includes it.
`node.rule_hash`, `node.provenance_hash`, and `node.relative_path` expose these
values. Constraints and `repeat` remain excluded.

V3 paths intentionally break compatibility with v2. Old cache directories are
not probed, migrated, reused, or deleted automatically.

During path resolution, necroflow validates generated paths against the filesystem's `NAME_MAX` and `PATH_MAX` limits. If a rule name, filename, output directory, or complete generated path would exceed those limits, path resolution fails before execution starts.

### Rule work directories

Commands may use the built-in `{workdir}` placeholder to refer to the rule-call
output directory: `nodes/{rule}/{rule_hash}/{provenance_hash}` by default.

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

The v3 provenance hash canonically supports ordinary scalar values,
paths, dates/times, sequences, string-keyed mappings, and sets. Unsupported
custom objects fail with a diagnostic identifying the config field.
Fingerprint policy is framework-owned; custom fingerprint providers and the
`.fingerprint` job key are not supported.

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

Invalidators are evaluated when the owning RuleCall becomes classifiable. After success, necroflow recomputes and stores tokens for every declared output.

### Atomic cache policy

A RuleCall is the cache unit. Requesting any co-output activates, validates, hashes, retains, reports, and cleans every declared output together. Only requested Nodes are copied into visible `results/`.

After success, each consumer records `consumed_sha256` for every immutable parent Node in `dependencies.toml`. Classification waits until parent calls settle, then compares recorded hashes with current parent bytes. Missing or malformed consumed hashes are stale. A rebuilt parent producing identical bytes preserves the consumer cache; changed bytes replay it.

Current hashes use `.rip/{filename}.hash` as an mtime-gated fast path. The stored digest is trusted when output mtime is no newer than hash-file mtime; otherwise current bytes are hashed. Hashes are memoized during one invocation. External edits preserving or backdating mtime are unsupported.

### Mutable Rules

Set `mutable=True` on a single-output Rule whose persistent state may change in place without external byte edits invalidating consumers:

```python
@command("update-db {db}", mutable=True)
def update_db(...):
    db = output(Database)
    return db
```

Multiple outputs on a mutable Rule are rejected. Mutable calls remain normal graph, provenance, scheduling, and failure units. If a mutable parent executes during the current run, consumers replay. Missing, forced, compromised, invalidator-changed, and failed mutable parents retain normal propagation. External content-only changes are ignored.

Autoclean never deletes mutable RuleCall workdirs. Necroflow provides no transaction or external-writer coordination.

- Re-running with identical identity and unchanged evidence is a cache hit.
- Changing upstream parameters, commands, contracts, or Rule mutability produces new paths.
- `.rip/dependencies.toml` stores identity, accumulated config, declared outputs, canonical parents, and immutable `consumed_sha256` values.
- `.rip/{filename}.hash` stores each declared output SHA-256.
- `.rip/state` stores call state (`running`, `up_to_date`, `failed`, or `interrupted`). A leftover or unknown non-success value compromises the whole call.

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
stored-hash fast path and consumed-hash comparison apply automatically:
`Path.stat()` follows the symlink to the real file, so editing it normally bumps the
mtime necroflow sees, invalidates the stored-hash fast path, and exposes changed
bytes to each downstream consumer. No
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

**Only one necroflow operation may mutate a given node store at a time.**
`run()` acquires an exclusive lock on `nodes/.rip/necroflow.lock` (via
`fcntl.flock`) at startup; CLI result materialization completes before that lock
is released. `necroflow gc` holds the same lock while scanning and deleting. A
second operation targeting the same node store fails immediately. Running two
instances against *overlapping* node stores (for example `nodes` and
`nodes/sub`) remains unsupported because no OS primitive detects the overlap.

[Previous: README](../README.md) | [README](../README.md) | [Next: Command-Line Interface](cli.md)
