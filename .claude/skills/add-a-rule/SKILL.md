---
name: add-a-rule
description: How to declare a necroflow rule correctly — command decorators, built-in file rules, placeholders, typed outputs, and common mistakes.
---

# Adding a necroflow rule

Full reference: `docs/rules.md`, `docs/generated-config-files.md`, `src/necroflow/rules.py`.

## Shell command rules

```python
from necroflow import command, output

@command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4, ram="8Gi")
def align(fastq: Fastq, ref: str):
    """Align reads to a reference genome with BWA-MEM."""
    bam = output(Bam)
    log = output(Log)
    return bam, log
```

- First decorator argument = command template; remaining keywords = resource constraints.
- Function parameters: NodeType annotations are positional node inputs; plain types
  (`str`, `int`, unions) are config keywords.
- Outputs use real local assignments: `name = output(NodeType)`. Return every
  declared name exactly once, using a tuple for multiple outputs. The docstring becomes
  `node.info`.
- Call the decorated name directly: `bam, log = align(fastq_node, ref="hg38")`.
- There is no registry or explicit shell-rule constructor.

## Command placeholders — validated at declaration

Allowed: declared input names, declared output names, and built-ins:

- `{workdir}` — the rule-call output directory (`nodes/{rule}/{hash16}`), created before
  execution and reserved as an input/output name.
- `{threads}` (defaults to 1), `{ram}`, other declared constraints; `{constraint:name}`
  forces constraint lookup when a config parameter shadows the name.
- Double literal shell braces: `{{left,right}}`. Bash syntax needs an explicit
  `shellpath="/bin/bash"` or `--shellpath`.

An undeclared placeholder fails while the module is imported.

## Rules of the game

- Generated input/output paths are shell-quoted for string commands. List commands bypass
  the shell.
- The command must write every output; exit 0 with a missing output is a failure.
- Constraints are scheduling metadata and do not affect fingerprints. `repeat=N` is
  compatibility metadata only.
- Config values are fingerprinted by value. A file path string does not hash that file.
  Ingest external datasets with `@symlink_file` or
  `symlink_file_rule(name, OutputType)` so source edits enter stale detection.
- Subtypes satisfy parent NodeType inputs; declare the most general true contract.

## Built-in file rules

Use decorator sugar for the common case:

```python
from necroflow import symlink_file, text_file, output

@symlink_file
def raw_fastq(path: str):
    fastq = output(Fastq)
    return fastq
@text_file
def sage_config(text: str):
    sage_config = output(SageConfig)
    return sage_config
```

Use `symlink_file_rule(...)` or `text_file_rule(...)` for explicit names. The text
decorator also supports `@text_file(encoding="utf-16")`. Text content participates in
normal config hashing; neither built-in requires a user-authored shell command.
