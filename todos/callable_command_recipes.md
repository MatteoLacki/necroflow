# Callable Command Recipes

The consolidated computation-identity and hashing design is in
[`fingerprint_design.md`](fingerprint_design.md). This file retains the original
proposal and inline review remarks.

## Goal

Generalize `command()` so that, in addition to the existing shell-template and
argv forms, a rule may use an importable Python function to construct its
command from resolved input paths, output paths, config, and constraints.

The design must preserve content-addressed output paths, deterministic DAG
deduplication, provenance, graph inspection, and all existing command APIs.

## Proposed public API

Existing commands remain unchanged:

```python
sort_bam = command(
    "samtools sort {bam} -o {sorted_bam}",
    Inputs(bam=Bam),
    Outputs(sorted_bam=SortedBam),
    name="sort_bam",
)
```

`command()` additionally accepts a named, importable Python function with one
`CommandArgs` argument:

```python
def merge_command(args: CommandArgs):
    argv = [
        "samtools",
        "merge",
        "-@",
        str(args.constraints["threads"]), #K here args.constraints.threads should work as well.
    ]

    if args.config["force"]:
        argv.append("-f")

    return [
        *argv,
        str(args.outputs["merged"]),
        *(str(path) for path in args.inputs["bams"]),
    ] #K of course this can return a str too


merge = command(
    merge_command,#K this here can also be an anonymous lambda.
    Inputs(bams=Many(Bam, min=1), force=bool),
    Outputs(merged=MergedBam),
    Constraints(threads=8),
    name="merge",
)

P.merged = merge(*bams, force=True)
```

`CommandArgs` is an immutable view of every value available to an ordinary
command template:

```python
@dataclass(frozen=True)
class CommandArgs:
    inputs: Mapping[str, Path | tuple[Path, ...]]
    config: Mapping[str, object]
    outputs: Mapping[str, Path]
    constraints: Mapping[str, object]
    workdir: Path
```

At recipe evaluation:

- `args.inputs` maps every named fixed node input to a `Path`.
- A `Many(Bam)` input maps to an ordered `tuple[Path, ...]` under its declared
  name.
- `args.config` maps every declared non-node input to its original Python value.
- `args.outputs` maps every declared output name to its resolved `Path`.
- `args.constraints` contains every command-visible constraint, including the
  default `threads=1` when threads were not explicitly declared.
- `args.workdir` is the shared resolved output directory for the rule call.

The mappings preserve declaration order and are read-only. The contained
config values are not recursively frozen; recipes are required not to mutate
them. A callback has the fixed protocol
`recipe(args: CommandArgs) -> str | list[str]`. Necroflow validates that the
callable accepts exactly one positional argument.

## Variadic inputs
#K This we will do later on: add to todos for later on.
Introduce:

```python
Many(Bam, min=1, max=None)
```

Rules:

- A rule may declare at most one `Many` input.
- It must be the final node-input declaration.
- Fixed node inputs precede its values at the rule-call boundary.
- Config remains keyword-only at the rule-call boundary.
- Parent order remains significant for commands and fingerprints.
- Initially, `Many` is supported only by callable command recipes.

## Built-in AST identity

When declaring a callable command:

1. Require a named, top-level Python function.
2. Unwrap decorated functions with `inspect.unwrap()`.
3. Reject closures and nested functions.
4. Require inspectable source and a source file.
5. Parse and canonicalize its AST.
#K Tell me how this is performed.
6. Hash the canonical AST with a versioned domain marker:

   ```text
   necroflow.python_recipe/v1
   ```

Whitespace, comments, and source locations do not affect this hash. Changes to
the function AST do.

The built-in hash does not automatically capture changes to referenced module
globals, imported helpers, environment variables, or external files. The
optional global hash provider covers project-specific dependencies of that
kind.

## Global additional hash

Add optional job metadata:

```toml
".pipeline" = "pipeline.py:build_pipeline"
".recipe_hash" = "hashing.py:recipe_hash"
```

The loaded callable has one fixed positional protocol:

```python
def recipe_hash(
    rule_name,
    recipe_callback,
    ast_hash,
    source_path,
) -> str:
    ...
```
#K perhaps let us follow the convention with possible hash arguments like for the command functions. Why ash_hash? Instead, provide raw inputs so that user can decide upon code hash too.

`functools.partial` is explicitly supported. Necroflow validates the
four-argument protocol when loading the provider and requires a string result.

The provider result is additive rather than a replacement:

```text
recipe identity = hash(AST identity, provider result)
```

Therefore a custom provider cannot accidentally disable built-in AST tracking.
The closure restriction applies to command recipes, not to the global hash
provider.

## Identity placement

Store the final recipe identity on the concrete rule call or node, not on the
shared module-level `Rule`.

This permits:

- Different job configurations or hash providers in one DAG.
- Correct deduplication when identities agree.
- Separation between reusable rule declarations and job-specific identity
  policy.

Static commands and existing built-in materializer identities remain unchanged.

## CLI and DAG lifecycle

Extend `JobConfig` with `recipe_hash_spec`.

For every expanded job:

1. Load the pipeline factory.
2. Construct the pipeline.
3. Load and validate `.recipe_hash`, if present.
#K Like per node or what?
4. Calculate callable-recipe identities for its nodes.
5. Resolve requests and explicit invalidations.
6. Add the pipeline to the DAG.

This ordering is required because `DAG.add()` deduplicates immediately using
`node.key`.

## Two-phase path resolution

Change `resolve_paths()` to:

1. Calculate and assign every node path from its recipe identity.
2. Group callable nodes by concrete rule-call fingerprint.
3. Build one immutable `CommandArgs` containing real input and output paths,
   config, constraints, and `workdir`.
#K Is this all needed at all? This now all looks like already doable on Node level? Or do we want to factorize to postpone? But then it could be cached instead?
4. Invoke each recipe once with that `CommandArgs`.
5. Normalize and validate the returned command.
6. Store it on all co-outputs and deduplicated aliases.

#K Basic question: why the Nodes do not simply make the command and why the maker of this simply does not use functools cache?

Resolution is idempotent for the same output root. Resolving against a different
root regenerates commands because absolute paths may differ. The executor never
reruns the callback.

## Shell execution context

Callable recipes may return either argv or a shell string. Because the result
type is unknown before fingerprinting, a configured `shellpath` conservatively
participates in every callable-recipe fingerprint.

Static argv commands retain their current behavior.

#K what is all this about? sounds like a non-problem?

## Provenance and inspection

Record callable-recipe identity metadata:

#K where is that recorded? in .rip?

```toml
[recipe]
kind = "python"
source = "pipeline.py"
ast_hash = "..."
extra_hash = "..."
```

Also retain the realized command for inspection. `graph`, `outputs`, `doctor`,
`explain`, dry-run, and execution must all use the same identity and resolution
path.

## Incremental implementation commits
#K adjust for what above
1. Add canonical Python-recipe AST hashing and closure rejection.
2. Add `CommandArgs` and callable command declarations for fixed node inputs.
3. Add `Many` declaration, binding, type validation, and fingerprint encoding.
4. Add two-phase path resolution and once-per-call command realization.
5. Add the four-argument global hash-provider protocol.
6. Add `.recipe_hash` loading before DAG indexing.
7. Add provenance, diagnostics, and documentation.
8. Run the complete tests and typing checks.

Each commit includes its corresponding tests and preserves all existing
string-template behavior.

## Review questions

- Should `Many` default to `min=1` or `min=0`?
#K not of importance now
- Should callable recipes initially support shell strings, or only argv lists?
#K Why not both?
- Should `shellpath` affect all callable recipes conservatively, or should the
  recipe declare its execution kind before fingerprinting?
#K We had a mechanism that selects the shell to use, perhaps this is already solved?
- Should the global hash provider run once per callable rule definition or once
  per concrete rule call?
#K likely can separate hash calculation with the usual digest mechanism?
- Should recipe source paths in provenance be absolute, job-relative, or
  normalized to the defining module name?
#K I think paths should be relative to the provided nodes folder?
