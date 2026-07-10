# Generated Config Files

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Execution, Scheduling, and Cleanup](execution.md)

## Generated config files

For tools that normally consume a large config file, register a built-in text-file rule and pass serialized config text from the pipeline factory. The text is written directly by Python, so it avoids shell quoting problems and command-line length limits from patterns such as `printf {config}`.

```python
import json
from necroflow import NodeType, Rules

class SageConfig(NodeType):
    filename = "sage.json"

R = Rules()
R.text_file("write_sage_config", SageConfig)

@R.command("necromerge2-run-sage {spectra} {fasta} {outdir} {run_info} --config {sage_config}")
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
P.sage_config = R.write_sage_config(
    text=json.dumps(config["sage"], sort_keys=True, indent=2) + "\n"
)
P.sage_out, P.run_info = R.run_sage(P.spectra, P.fasta, P.sage_config)
```

`Rules.text_file(name, output, input_name="text")` creates a normal cached node. The text value participates in the node fingerprint, and the built-in writer recipe (`necroflow.text_file/v1`) is hashed in place of shell command text.

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
