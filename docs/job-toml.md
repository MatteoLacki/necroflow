# Job TOML and Parameter Grids

[Previous: Command-Line Interface](cli.md) | [README](../README.md) | [Next: Config Validation](config-validation.md)

## Job TOML format

```toml
# required — path resolved from the directory where necroflow is invoked
".pipeline" = "path/to/factory.py:function_name"

# optional — Pipeline labels to request (defaults to all sink labels)
".requests" = ["counts", "dataset/qc"]

# user config — passed as the second factory argument after Pipeline
ref    = "hg38"
sample = "NA12878"
```

Keys starting with `.` are necroflow metadata and are stripped before the dict
reaches the factory. User config can freely use names such as `pipeline` or `request`.
The loaded callable must have the shape `factory(P: Pipeline, config: dict) ->
None`. Necroflow constructs `P` with `--nodes-dir` and `--shellpath` before
invoking the factory. Fingerprinting is framework-owned; `.fingerprint` is
rejected rather than forwarded.

`.requests` must be an array of strings. Each string is an exact Pipeline label;
labels may be canonical relative POSIX paths such as `dataset/qc`. The visible
result is then nested at `results/<job>/dataset/qc/<filename>`.

## Parameter grids

Any TOML key ending in `__grid` is expanded into a Cartesian product of all
combinations. Result-folder labels are derived deterministically from nested
parameter paths and their concrete values.

```toml
".pipeline"   = "factory.py:factory"
ref__grid     = ["hg38", "mm10"]
aligner__grid = ["bwa", "bowtie2"]
```

This produces four pipelines: `experiment__ref+hg38__aligner+bwa`,
`experiment__ref+hg38__aligner+bowtie2`, etc. Grid expansion also applies to
`pipeline` itself, so a single job TOML can fan out across different factory functions.

[Previous: Command-Line Interface](cli.md) | [README](../README.md) | [Next: Config Validation](config-validation.md)
