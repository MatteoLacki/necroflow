# Canonical text pipeline in Pydra

This is an executable Pydra **1.0a11** port of
[the Necroflow canonical example](../canonical/pipeline.py). See
[the feature comparison](../../COMPARISON_PYDRA.md) for the engine differences
and the distinction between the 0.25 API and the 1.0 prerelease API used here.

Four tasks form the same graph:

```mermaid
flowchart LR
    input[External text] --> raw[Import text]
    config[Write JSON config] --> process[Uppercase with tr]
    raw --> process
    process --> summary[Count bytes with wc]
```

Tasks use `@python.define`; the workflow uses `@workflow.define`. Its body reads
as a sequence of local assignments, with returned output fields passed directly
to downstream tasks:

```python
@workflow.define(outputs=["raw", "tool_config", "processed", "summary"])
def canonical_pipeline(source: File, tool_text: str) -> tuple[File, File, File, File]:
    raw = workflow.add(import_text(source=source)).out
    tool_config = workflow.add(write_tool_config(text=tool_text)).out
    processed = workflow.add(process_text(raw_text=raw, tool_config=tool_config)).out
    summary = workflow.add(summarize(processed_text=processed)).out
    return raw, tool_config, processed, summary
```

`workflow.add` registers each task in the active construction context and
returns its output fields. `.out` selects the single default output; the local
variables hold lazy references until execution. The workflow returns its four
named products without `lzin`, `lzout`, or `set_output` calls in user code.
Config is a real dependency even though `tr` does not read it, matching the
original graph. Inputs/returns use typed `fileformats.generic.File` values.
`File` tracks source contents; the original canonical example's string input
path does not. These are generic file types rather than replicas of Necroflow's
four domain-specific Node subclasses.

## Run through Make

From the Necroflow repository root, with `uv`, Python 3.12, `tr`, and `wc` available:

```sh
make pydra-example
make pydra-test
```

`pydra-example` creates `examples/pydra_canonical/.venv`, installs the pinned
requirements, runs Python byte compilation, then executes with two local
process workers. It leaves the main `.venv` untouched. No C/native compilation
is needed. Dependencies are version-pinned at the direct requirement level;
transitive dependencies are resolved by the installer.

`pydra-test` also installs this checkout's Necroflow package into the **example
venv**, solely to run cross-engine parity checks. The Pydra script itself has
no Necroflow import or runtime requirement. Its own pytest config avoids the
main test suite's coverage/xdist plugin requirements.

Additional targets/settings:

```sh
make pydra-venv
make pydra-compile
make pydra-example PYDRA_BUILD_DIR=/tmp/pydra-demo
```

`PYDRA_PYTHON_VERSION` defaults to `3.12`; that is the tested interpreter.
Pydra is pinned to `1.0a11`, a prerelease. Its function-defined workflow API is
incompatible with 0.25; do not downgrade the requirement without porting the code.

## Inspect results

Default artifacts:

```text
build/pydra-canonical/
  cache/                         # Pydra task/workflow checksum directories
  results/
    raw/input.txt
    tool_config/tool_config.json
    processed/processed.txt
    summary/summary.txt
    outputs.json                 # published/cache paths and numeric byte count
```

Expected processed text is `HELLO NECROFLOW` followed by a newline; expected
count is **16**. `summary.txt` retains `wc -c FILE` format, including the cached
processed file's path. That path naturally differs between engines and roots.
Result copies and `outputs.json` are produced by this example's CLI wrapper,
not by a built-in Pydra result publication command. Reusing the output directory
replaces these visible copies; cached runs remain in the cache directory.
The wrapper uses `shutil.copy2` without an explicit CoW policy. Necroflow's CLI
instead requests reflinks/clones where supported, with physical-copy fallback;
matching its results directory layout does not reproduce that storage policy.

Run `make pydra-example` again to reuse cached tasks. The checks additionally
verify reuse across fresh Python interpreters without rewriting cached results.

## Standalone venv and alternate input

Make is optional. From the repository root:

```sh
python3.12 -m venv examples/pydra_canonical/.venv
examples/pydra_canonical/.venv/bin/python -m pip install -r examples/pydra_canonical/requirements.txt
examples/pydra_canonical/.venv/bin/python -m compileall -q examples/pydra_canonical/pipeline.py
examples/pydra_canonical/.venv/bin/python examples/pydra_canonical/pipeline.py \
    --input examples/canonical/input_alt.txt \
    --cache-dir /tmp/pydra-example/cache \
    --output-dir /tmp/pydra-example/results \
    --tool-json '{"mode":"uppercase"}' \
    --worker cf --n-procs 2
```

Use `--worker debug` for serial execution. `--n-procs` limits local worker
processes; it is not a budget for threads used by external tools. Source and
cache paths containing spaces are supported. The port uses POSIX `tr` and `wc`
on the host and fixes `tr`'s locale to `C` for deterministic ASCII conversion.

## Checks

The canonical tests cover both bundled inputs on both local workers, byte-for-byte
raw/config/processed parity with Necroflow, numeric summary parity, paths with
spaces, persistent cache reuse, same-path source changes, and config dependency
changes, using the `debug` and `cf` workers. The original source-path caching
limitation is documented rather than incorrectly claiming identical invalidation
behavior.

Remote workers, containers, resource admission policies, and crash recovery are
outside this small example's validation scope.

## Paper splitter examples

[splitter_example.py](splitter_example.py) adapts the SciPy 2020 paper's addition
and sine examples to Pydra 1.0a11. Run in the same isolated venv:

```sh
make pydra-splitter-example
```

For `x=[1,2]`, `y=[10,100]`, the output shows:

| Mapping/grouping | Values |
|---|---|
| Product split `["x", "y"]` | `[11, 101, 12, 102]` |
| Paired split `("x", "y")` | `[11, 102]` |
| Product split, combine `y` | `[[11, 101], [12, 102]]` |

The sine workflow obtains `[0,1,2]` from an upstream task, maps one term task per
index, combines its three outputs, then sums them. For `x=pi/2`, `n_max=2`, the
approximation is `1.0045248555348174`. Combining only groups results; summing is
a separate task.

Two additional checks exercise these semantics with `debug` and `cf`, bringing
`make pydra-test` to nine tests. See the
[splitter versus DAG construction comparison](../../COMPARISON_PYDRA.md#splitters-versus-explicit-dag-construction)
for the mapping to Necroflow loops, `__grid`, eager interning, and gather rules.
