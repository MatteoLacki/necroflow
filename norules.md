# Decorators return rules directly

Necroflow rules are ordinary module-level values. There is no `Rules` registry,
`register` API, or function-body `@rule` API.

## Shell rules

```python
from necroflow import command

@command("bwa mem {ref} {fastq} > {bam}", threads=4)
def align(fastq: Fastq, ref: str):
    return Bam[bam]

P.bam = align(P.fastq, ref=config.ref)
```

`@command` parses the typed function declaration and replaces its name with a
callable internal `Rule`. The declaration body is not executed.

## Built-in file rules

Built-ins have decorator sugar and explicit factories:

```python
from necroflow import (
    symlink_file,
    symlink_file_rule,
    text_file,
    text_file_rule,
)

@symlink_file
def raw_fastq(path: str):
    return Fastq[fastq]

raw_fasta = symlink_file_rule(
    "raw_fasta",
    Fasta,
    path_arg="path",
)

@text_file
def write_config(text: str):
    return ConfigFile[config_file]

@text_file(encoding="utf-16")
def write_utf16_config(text: str):
    return Utf16Config[config_file]

write_other_config = text_file_rule(
    "write_other_config",
    ConfigFile,
    input_name="serialized",
    encoding="utf-8",
)
```

Built-in decorators require exactly one `str` input and one concrete `NodeType`
output. Input and output names come from the declaration. Explicit factories
provide custom argument/output names.

Symlink rules retain the existing
`ln -s $(realpath {path}) {output}` command recipe, preserving cache identity.
Text rules retain the `necroflow.text_file/v1` materializer identity.

## Removed surface

`Rules`, `Inputs`, `Outputs`, and `Constraints` are not top-level exports.
The internal representations remain implementation details. Equivalent migrated
rules retain their names, commands, type contracts, constraints, repeat metadata,
fingerprints, and cached output paths.
