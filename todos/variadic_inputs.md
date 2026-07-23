# Variadic Rule Inputs

## Goal

Allow a rule to consume an ordered, runtime-sized collection of parent
`Node`s. This is needed by command-line tools such as merge utilities, whose
number of input paths is not known when the rule is declared.

Callable commands are the initial target because normal Python can construct
the command from any number of inputs without extending the shell-template
language.

## Proposed API

Declare one variadic node input with `Many`:

```python
def merge_command(args: CommandArgs) -> str:
    inputs = " ".join(shlex.quote(str(path)) for path in args.inputs["bams"])
    output = shlex.quote(str(args.outputs["merged"]))
    threads = args.constraints.threads
    return f"samtools merge -@ {threads} {output} {inputs}"


merge = command(
    merge_command,
    Inputs(bams=Many(Bam, min=1)),
    Outputs(merged=MergedBam),
    Constraints(threads=8),
    name="merge",
)

P.merged = merge(P, *bams)
```

`CommandArgs.inputs["bams"]` is an ordered `tuple[Path, ...]`. The output,
constraints, config, and workdir remain available through the existing
`CommandArgs` interface.

## Initial Rules

- A rule may declare at most one `Many` input.
- `Many` must be the final node-input declaration.
- Fixed node inputs precede its values at the rule-call boundary.
- Config arguments remain keyword-only.
- `min` defaults to `0`; `max=None` means no upper bound.
- Every supplied node must have the declared `NodeType`.
- Parent order is significant and must be preserved in command realization,
  fingerprints, provenance, and DAG inspection.
- Initial support is limited to callable commands. Shell-template support can
  be designed later if there is a compelling syntax.

## Implementation Questions

- Decide whether `Many` belongs in `nodes.py`, `rules.py`, or a dedicated
  input-schema module.
- Define the static typing for rules whose positional arity includes `Many`;
  runtime validation is straightforward, but precise type inference may not
  be.
- Decide how graph and provenance output distinguish the variadic group while
  retaining the order of its members.
- Add tests for empty, minimum, maximum, wrong-type, mixed fixed/variadic,
  ordering, fingerprint, and deduplication behavior.
