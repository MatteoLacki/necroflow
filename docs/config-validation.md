# Config Validation

[Back to README](../README.md)

## Config validation

Use `--validation path/to/schema.py:validate` to reject malformed job configs before pipeline construction. The callable receives the same plain config dict that the pipeline factory receives and should raise an exception on invalid input:

```python
def validate(config):
    if "sample" not in config:
        raise ValueError("missing required key: sample")
```

```bash
necroflow --validation schema.py:validate job.toml
```

`--validation` is repeatable and validators run in CLI order. Validation runs after `__grid` expansion and after stripping dot-prefixed necroflow metadata such as `.pipeline` and `.requests`. This callback mechanism is intentional: with `__grid`, the raw TOML file is not always the concrete config that a factory will receive, so validating the file ahead of time can miss or misreport errors in individual expanded combinations.

Python-only callers can use the same loader and validate in their own loop:

```python
from necroflow import iter_job_configs

def validate(config):
    if "sample" not in config:
        raise ValueError("missing required key: sample")

for job in iter_job_configs("job.toml"):
    validate(job.config)
    print(job.label, job.config)
```

Cerberus is available as an optional validation extra:

```bash
pip install "necroflow[validation]"
```

A validator can then load a Cerberus schema from TOML or JSON and apply it to the expanded config:

```python
import tomllib
from pathlib import Path
from cerberus import Validator

schema = tomllib.loads(Path("schema.toml").read_text())

def validate(config):
    validator = Validator(schema, allow_unknown=False)
    if not validator.validate(config):
        raise ValueError(validator.errors)
```

Cerberus handles structural checks well; branch-specific or cross-parameter domain rules can live in `check_with` hooks or in ordinary Python after the Cerberus check.
