---
name: add-a-rule
description: How to declare a necroflow rule correctly — command decorators, built-in file rules, placeholders, typed outputs, and common mistakes.
---

# Adding a necroflow rule

Full reference: `docs/rules.md`, `docs/generated-config-files.md`, `src/necroflow/rules.py`.

## shell command rules

```python
from necroflow import command, outputs, shell, Resource, memory_bytes

@command
def align(fastq: Fastq, ref: str, threads: Resource[int] = 4,
          ram: Resource[int] = memory_bytes("8Gi")):
    """Align reads to a reference genome with BWA-MEM."""
    bam = outputs(Bam)
    log = outputs(Log)
    shell("bwa mem -t {threads} {ref} {fastq} > {bam} 2> {log}")
    return bam, log
```

- Bare @command executes the body during construction; register exactly one shell.
- Function parameters follow ordinary Python binding; Node annotations describe
  dependencies, plain types describe config, and Resource[T] describes execution quantities.
- Outputs use real local assignments: `name = outputs(Node)`. Return every
  declared name exactly once, using a tuple for multiple outputs. The docstring becomes
  `node.info`.
- Call the decorated name directly: `bam, log = align(fastq_node, ref="hg38")`.
- There is no registry or explicit shell-rule constructor.

## Command placeholders — validated during construction

Allowed: declared input names, declared output names, and built-ins:

- `{workdir}` — the rule-call output directory (`nodes/{rule}/{hash16}`), created before
  execution and reserved as an input/output name.
- `{threads}` (defaults to 1), `{ram}`, other declared constraints; `{constraint:name}`
  is an explicit, self-documenting alternative to the plain form.
- Double literal shell braces: `{{left,right}}`. Bash syntax needs an explicit
  `shellpath="/bin/bash"` or `--shellpath`.

An undeclared shell placeholder fails when its command is called, before execution.

## Reserved names — checked at declaration

An input/output name may not collide with `workdir`, with a constraint the rule
declares, or with `threads` (every rule carries an implicit `threads=1` even without
`Constraints(...)`). Declaring `Inputs(threads=int)` on a rule constrained with
`Constraints(threads=8)` — or with no constraints at all — raises `ValueError` when
the `@command`/`Rule` runs, not at call time. There is no shadowing: rename the
input/output, or the constraint.

## Rules of the game

- Generated input/output paths are shell-quoted by the template renderer. Argv-list
  commands are unsupported.
- The command must write every output; exit 0 with a missing output is a failure.
- Constraints are scheduling metadata and do not affect fingerprints. `repeat=N` is
  compatibility metadata only.
- Config values are fingerprinted by value. A file path string does not hash that file.
  Ingest external datasets with `@symlink_file` or
  `symlink_file_rule(name, OutputType)` so source edits enter stale detection.
- Subtypes satisfy parent Node inputs; declare the most general true contract.

## Built-in file rules

Use decorator sugar for the common case:

```python
from necroflow import symlink_file, text_file, outputs

@symlink_file
def raw_fastq(path: str):
    fastq = outputs(Fastq)
    return fastq
@text_file
def sage_config(text: str):
    sage_config = outputs(SageConfig)
    return sage_config
```

Use `symlink_file_rule(...)` or `text_file_rule(...)` for explicit names or a non-default
encoding (`text_file_rule(name, output, encoding="utf-16")`) — both `@symlink_file` and
`@text_file` are bare-decorator-only. Text content participates in normal config hashing;
neither built-in requires a user-authored shell command.
