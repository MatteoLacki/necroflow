---
name: add-a-rule
description: How to register a necroflow rule correctly — decorator and explicit APIs, command placeholders, typed outputs, common mistakes. Load before adding or editing rules in a necroflow pipeline.
---

# Adding a necroflow rule

Full reference: `docs/rules.md`, `docs/generated-config-files.md`, `src/necroflow/rules.py`.

## Decorator style (preferred)

```python
r = Rules()

@r.command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4, ram="8Gi")
def align(fastq: Fastq, ref: str):
    """Align reads to a reference genome with BWA-MEM."""
    return Bam[bam], Log[log]
```

- First decorator arg = command template; remaining kwargs = `Constraints`.
- Function params: NodeType annotations = positional node inputs; plain types (`str`, `int`,
  unions) = config kwargs.
- Outputs declared in the body as `return Type[name]` (tuple for multiple). The docstring
  becomes `node.info`.
- Call: `bam, log = r.align(fastq_node, ref="hg38")` — single output returns the Node
  directly, multiple return a named tuple.

## Explicit style

```python
R.register("align", Inputs(fastq=Fastq, ref=str), Outputs(bam=Bam, log=Log),
           "bwa mem {ref} {fastq} > {bam} 2> {log}", Constraints(threads=4))
```

## Command placeholders — validated at registration

Allowed: declared input names, declared output names, config keys, and built-ins:

- `{workdir}` — the rule-call output dir (`nodes/{rule}/{hash16}`), created before the
  command runs; use for retained side/scratch files. Reserved name.
- `{threads}` (defaults to 1), `{ram}`, other declared constraints; `{constraint:name}` forces
  constraint lookup when a config key shadows the name.
- Literal shell braces must be doubled: `{{left,right}}`. Bash-isms need
  `execute(..., shellpath="/bin/bash")` / `--shellpath`.

An undeclared placeholder fails at registration — read the error, don't guess.

## Rules of the game

- Paths are quoted for you (`shlex.quote`) in string commands; config values are not.
  List-form commands bypass the shell entirely.
- The command MUST write every declared output; exit 0 with a missing output raises.
- Constraints (`threads`, `ram="8Gi"`, custom) are scheduling metadata only — not part of the
  fingerprint. `repeat=N` is accepted but is compatibility metadata only.
- **Config values are fingerprinted as strings.** Passing a file *path* as config does not
  hash the file's content, and if that path is never turned into a node at all, editing the
  file is invisible — nothing reruns. For an external dataset, ingest it with
  `R.symlink_file(name, OutputType)` instead: the existing mtime/hash STALE machinery then
  picks up content changes automatically (see `docs/caching.md#external-dataset-ingestion`).
  For a config value, prefer inlining content (see below) or a `NodeType.invalidator`.
- Subtypes are accepted where the parent type is expected (`issubclass` check), so declare
  the most general NodeType that is truly required.

## Generated config files

For tool configs coming from job TOML, don't echo strings through the shell — use the
built-in materializer:

```python
R.text_file("sage_config", output=SageConfig, input_name="text")
P.cfg = R.sage_config(text=json.dumps(config["sage"], sort_keys=True, indent=2) + "\n")
```

No shell involved; content is fingerprinted through normal config hashing.
