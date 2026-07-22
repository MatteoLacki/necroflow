# Job TOML and Parameter Grids

[Previous: Command-Line Interface](cli.md) | [README](../README.md) | [Next: Config Validation](config-validation.md)

## Job TOML format

```toml
# required — path resolved from the directory where necroflow is invoked
".pipeline" = "path/to/factory.py:function_name"

# optional — pipeline_label names to request (defaults to all sinks)
".requests" = ["counts", "qc"]

# optional — complete project fingerprint policy
".fingerprint" = "path/to/hashing.py:project_fingerprint"

# user config — passed as the second factory argument after Pipeline
ref    = "hg38"
sample = "NA12878"
```

Keys starting with `.` are necroflow metadata and are stripped before the dict
reaches the factory. `.fingerprint` is the exception that deliberately selects
the function used to compute output identity; the other metadata keys are not
node config. User config can freely use names such as `pipeline` or `request`.
The loaded callable must have the shape `factory(P: Pipeline, config: dict) ->
None`. Necroflow constructs `P` with `--nodes-dir`, the selected fingerprint
function, and `--shellpath` before invoking the factory.

## Parameter grids

Any TOML key ending in `__grid` is expanded into a Cartesian product of all
combinations. The resulting output subfolders use the same naming scheme as
[snakemakeconfigs](https://github.com/MatteoLacki/snakemakeconfigs).

```toml
".pipeline"   = "factory.py:factory"
ref__grid     = ["hg38", "mm10"]
aligner__grid = ["bwa", "bowtie2"]
```

This produces four pipelines: `experiment__ref+hg38__aligner+bwa`,
`experiment__ref+hg38__aligner+bowtie2`, etc. Grid expansion also applies to
`pipeline` itself, so a single job TOML can fan out across different factory functions.

[Previous: Command-Line Interface](cli.md) | [README](../README.md) | [Next: Config Validation](config-validation.md)
