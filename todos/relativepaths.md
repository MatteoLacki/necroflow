# Task: Make `resolve_paths` resolve `outdir` to an absolute path

## Problem

`Node.path` is not guaranteed absolute. `resolve_paths()` (`src/necroflow/dag.py`,
~line 249) does:

``` python
def resolve_paths(nodes: list[Node], outdir: Path | str) -> None:
    outdir = Path(outdir)
    for node in nodes:
        path = outdir / node.key
        _check_path_limits(path)
        node.path = path
```

`outdir` is taken as-is. The CLI's default (`--nodes-dir`, `cli.py`) is the
relative `Path("nodes")`, and nothing downstream ever calls `.resolve()` on it.
So in the common case `node.path` — and everything derived from it —
is relative to whatever directory the `necroflow` process happened to be
invoked from.

This leaks into `resolve_command()` (`src/necroflow/dag.py`, ~line 218):

``` python
subs[iname] = parent.path        # {input_name} substitution
subs[oname] = onode.path         # {output_name} substitution
subs["workdir"] = node.path.parent   # {workdir} substitution
```

All three of these — `{some_input}`, `{some_output}`, `{workdir}` — inherit
whatever relativity `outdir` had. Any rule whose shell command needs an
absolute path (e.g. because the tool it invokes changes its own working
directory, or writes the path into a file that's read back later from a
different cwd) currently has to work around this itself with a manual
`$(realpath {...})` in the command template.

## Where this bit us

`ionmaidentools/pipelines.py`'s `write_fragpipe_manifest` rule needs to write
FragPipe's `manifest.tsv` with an *absolute* path to the input mzML, because
FragPipe is invoked with `--workdir <some other dir>` and resolves manifest
entries relative to its own cwd, not necroflow's. The rule currently does:

``` python
@R.command('printf "%s\tA\t1\tDDA" "$(realpath {mzml})" > {manifest}')
def write_fragpipe_manifest(mzml: TofFilteredMzml):
    return FragpipeManifest[manifest]
```

`{mzml}` here is a `parent.path` substitution — i.e. a path necroflow itself
computed, not user input. It should already be absolute; `realpath` is only
there to paper over `resolve_paths` not enforcing that.

(Separately, `source_fasta`/`source_bruker_d`-style rules in that same file
do `$(realpath {path})` too, but there `{path}` is a raw string from job
config — `cfg.tdf_path`, `cfg.fasta_path` — not a `Node.path`. That's
resolving arbitrary user input from outside necroflow's own tree and is
unrelated to this task; it'll still be needed no matter what `resolve_paths`
does.)

## Proposed fix

In `resolve_paths()`, resolve `outdir` once, up front:

``` python
outdir = Path(outdir).resolve()
```

This is the only change needed. It has no effect on CLI ergonomics (users
still pass relative strings like `--nodes-dir nodes`); it just makes every
`Node.path` — and therefore every `{input}`/`{output}`/`{workdir}` command
substitution — absolute from that point on, for every rule, with no
per-rule opt-in.

## Scope check before landing

- Confirm no code relies on `Node.path` being relative to some directory
  other than cwd (e.g. relative-path comparisons in tests, or anything that
  writes `Node.path` into a persisted file expecting portability across
  machines/mounts). Skimmed `write_dependencies()` — it stores
  `node.path.parent.name` (just the hash) and `_accumulated_config(node)`
  (arbitrary config values, not paths), so nothing there embeds path
  relativity. Worth double-checking test fixtures under `tests/` too, since
  some may assert on exact (relative) path strings.
- Once landed, revisit `write_fragpipe_manifest` in the necromerge2
  `ionmaidentools` repo and drop the now-redundant `$(realpath {mzml})`.
