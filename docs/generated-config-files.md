# Generated Config Files

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Execution, Scheduling, and Cleanup](execution.md)

Tool-specific config can live in necroflow in two useful ways.

First, keep large tool configs inside the main job TOML as ordinary tables. This
is useful when the job TOML is the run contract: one file contains the pipeline
factory, sample-specific parameters, tool settings, and optional grid axes. The
settings then participate naturally in necroflow fingerprints because the
pipeline factory serializes the table into a normal rule input.

Second, keep tool configs as separate files and put only their paths in the job
TOML. This is the Snakemake-style layout: each tool can own its native config
file, while necroflow records the path as part of the job config and passes it to
the relevant rule. Use this when the config is maintained outside necroflow, is
shared by many workflows, or is already the format expected by the tool.

## External config files

A separate config file can be passed as a normal string parameter when that is
all the downstream command needs:

```python
@command("sage --config {sage_config_path} --mzml {spectra} --out {sage_out}")
def run_sage(spectra: Mzml, sage_config_path: str):
    return SageOut[sage_out]


def pipeline(config):
    P = Pipeline()
    P.sage_out = run_sage(P.spectra, sage_config_path=config["sage_config"])
    return P
```

```toml
sage_config = "configs/sage.json"
```

This is closest to the Snakemake convention: each tool can keep its own native
config file, and the main job TOML records which file to use. In this form,
necroflow fingerprints the path string, not the file contents, so changing the
file in place does not automatically change the downstream node key.

When the external config should be a typed artifact in the DAG, add an import or
copy rule and pass the resulting node downstream:

```python
from necroflow import NodeType, Pipeline, command

class SageConfig(NodeType):
    filename = "sage.json"

class SageOut(NodeType):
    filename = "results.sage.tsv"
@command("cp {path} {sage_config}")
def import_sage_config(path: str):
    return SageConfig[sage_config]

@command("sage --config {sage_config} --mzml {spectra} --out {sage_out}")
def run_sage(spectra: Mzml, sage_config: SageConfig):
    return SageOut[sage_out]


def pipeline(config):
    P = Pipeline()
    P.sage_config = import_sage_config(path=config["sage_config"])
    P.sage_out = run_sage(P.spectra, P.sage_config)
    return P
```

This keeps the config as a normal upstream artifact. If in-place edits to the
source config should invalidate downstream work, add a `NodeType.invalidator` to
the imported config type that reads the copied config content, or make the config
producer itself a necroflow rule.

## Generated config files

For tools that normally consume a large config file, but whose settings belong in
the main job TOML, register a built-in text-file rule and pass serialized config
text from the pipeline factory. The text is written directly by Python, so it
avoids shell quoting problems and command-line length limits from patterns such
as `printf {config}`.

```python
import json
from necroflow import NodeType, command, text_file

class SageConfig(NodeType):
    filename = "sage.json"
@text_file
def write_sage_config(text: str):
    return SageConfig[sage_config]

@command("necromerge2-run-sage {spectra} {fasta} {outdir} {run_info} --config {sage_config}")
def run_sage(spectra: SageInputStaged, fasta: Fasta, sage_config: SageConfig):
    return SageRawOutdir[outdir], SageRunInfo[run_info]
```

A job TOML table can then be passed through as ordinary factory config:

```toml
[sage]
deisotope = true
min_peaks = 15
```

```python
P.sage_config = write_sage_config(
    text=json.dumps(config["sage"], sort_keys=True, indent=2) + "\n"
)
P.sage_out, P.run_info = run_sage(P.spectra, P.fasta, P.sage_config)
```

`@text_file` and `text_file_rule(name, output, input_name="text")` create normal cached nodes.
The text value participates in the node fingerprint, and the built-in writer
recipe (`necroflow.text_file/v1`) is hashed in place of shell command text.

## Updating one config from another

When a downstream tool needs a normal config file with one field filled from an
upstream result, keep that dependency as a file dependency and use the packaged
helper tool to perform the open-update-save step:

```bash
necroflow-config-set template.toml updated.toml \
  --target sage.precursor_tolerance \
  --source derived.toml \
  --source-field calibration.precursor_tolerance
```

The command copies `template.toml`, reads `calibration.precursor_tolerance` from
`derived.toml`, writes that value to `sage.precursor_tolerance`, and saves the
result as `updated.toml`. JSON uses the same interface:

```bash
necroflow-config-set template.json updated.json \
  --target sage.precursor_tolerance \
  --source derived.json \
  --source-field calibration.precursor_tolerance
```

The output extension must match the copied input extension, so TOML stays TOML
and JSON stays JSON. Source and target fields use dotted paths. Missing target
tables are created; missing source fields are errors. This keeps dynamic values
inside ordinary rule inputs and outputs instead of interleaving Python DAG
construction with execution.

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Execution, Scheduling, and Cleanup](execution.md)
