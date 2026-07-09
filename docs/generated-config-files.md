# Generated Config Files

[Back to README](../README.md)

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
