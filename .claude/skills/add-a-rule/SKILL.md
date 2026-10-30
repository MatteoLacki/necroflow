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

- `{workdir}` — the rule-call output directory (`nodes/{rule}/{provenance_hash}`), created before
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
- Constraints are scheduling metadata and do not affect fingerprints. `repeat=N` retries failed commands up to N attempts,
  stopping at the first success; retry policy stays outside fingerprints.
- Config values are fingerprinted by value. A file path string does not hash that file.
  Ingest external datasets with `symlink(Type, path)` or
  `symlink_file_rule(name, OutputType)` so source edits enter stale detection.
- Subtypes satisfy parent Node inputs; declare the most general true contract.

## Built-in file rules

Ingest files directly in workflows; declare text generation with `@text_file`:

```python
from necroflow import symlink, text_file, outputs, workflow

@text_file
def sage_config(text: str):
    config = outputs(SageConfig)
    return config

@workflow
def build(P, config):
    P.fastq = symlink(Fastq, config["reads"])
    P.config = sage_config(text=config["sage_config"])
```

Outside workflows, use `symlink(Fastq, path, pipeline=P)`. The old
`@symlink_file` decorator is removed. `symlink_file_rule(...)` supports explicit
rule/input/output names; `text_file_rule(name, Type, encoding="utf-16")` supports
non-default text encodings. `@text_file` is bare-decorator-only. Text content
participates in normal config hashing; neither built-in needs a user shell command.
