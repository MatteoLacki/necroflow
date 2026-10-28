# Inline commands and configurable execution resources

Status: implemented locally; verification results recorded below.

## Intended interface

```python
from necroflow import Resource, command, output

@command
def align(reads: Fastq, threads: Resource[int] = 4):
    bam = output(Bam)
    command = "align -t {threads} {reads} > {bam}"
    return bam
```

Also support a nested command builder:

```python
@command
def align(reads: Fastq, threads: Resource[int] = 4):
    bam = output(Bam)

    def command():
        return (
            f"align -t {threads} "
            f"{shlex.quote(str(reads))} > {shlex.quote(str(bam))}"
        )

    return bam
```

The outer function remains a declaration. Calling the decorated rule returns
Nodes, never the command text. Output declarations precede exactly one local
command definition; the final return selects declared outputs.

```bash
necroflow run --resources workstation.toml job.toml
```

```toml
[align]
threads = 12

[mokapot]
threads = 8
```

Resource files are separate from job configs and grids. The workflow does not
load or forward them. `Resource` marks scheduling parameters, not data identity.

## Compatibility requirements

- Preserve existing `@command("...")`, `@command(callback)`, explicit factories,
  `Constraints`, built-in rules, and their hashes.
- An equivalent inline string declaration must produce exactly the same recipe
  and invocation hashes as the existing decorator-template declaration.
- Exclude resource values, resource defaults, and resource annotation entries
  from both rule-hash input contracts and provenance-hash config/input values.
  Do not bump identity domains or modify `canonical_bytes`.
- Keep ordinary scalar settings in identity. Moving an existing ordinary setting
  to `Resource` intentionally changes its semantics and may change that rule's
  identity; this is distinct from preserving already resource-based rules.
- Resource overrides do not invalidate cached outputs. Record execution resources
  separately for inspection, without modifying historical cached-run metadata.
- Retain current path quoting, Docker prefix handling, return shapes, retries,
  global capacity checks, and resource-unit parsing.

## Implementation sequence

### Declaration parser and inline strings

Refactor `_parse_rule_fn` into a structured declaration result that separates
inputs, outputs, resource declarations, return order, and command source.
Keep built-in text/symlink declaration validation strict and unchanged.

Accept a literal string assignment to `command` after all output declarations.
Adjacent string literals work naturally through Python's AST. Reject f-strings,
arbitrary assignment expressions, duplicate command definitions, declarations
after command, command/output name collisions, and conditional output declarations.
Do not execute the outer function or evaluate arbitrary command expressions.

Pass the extracted template into the existing Rule/template machinery. Preserve
named placeholders and automatic shell quoting; no expression evaluation inside
braces. Validate placeholder names at declaration time.

Add bare `@command` and `@command(...)` with no external command, for options such
as `repeat`. Existing callable callback syntax makes dispatch sensitive: recognize
an inline declaration from its explicit top-level local command definition and
provide declaration errors for malformed inline bodies. Keep legacy callbacks,
including lambdas, on the existing path. Test both dispatch directions explicitly.

### Resource declarations and per-call policy

Export `Resource` and partition annotated parameters before constructing Inputs
and rule identity. Initial runtime support is `Resource[int]`, matching the
scheduler's integer quantities; reject unsupported types explicitly. Resource
parameters require defaults and accept keyword overrides on rule invocation.

Normalize external file values using existing resource parsing, including units
and the existing `threads = "all"` policy. The command sees resolved integer
quantities. Check positivity and reject bools and malformed values. Preserve
existing legacy constraints semantics rather than silently reinterpreting them.

Store effective resource requests on RuleCall, not by mutating module-level Rule
objects. Separate requested resources from allocation resolved against run caps.
Templates, nested builders, legacy CommandArgs.constraints, scheduler accounting,
and diagnostics must agree on effective values. Rebinding resource policy or
thread caps invalidates realized-command caching without changing node identity.

Update declaration/signature handling so `threads` is allowed as a Resource
parameter while retaining collision protection for ordinary inputs and outputs.
Audit static typing: annotation is a marker, but runtime builder values are ints.
Verify common declarations and call sites with existing typing fixtures; document
any remaining static-checker limitations rather than promising wrapper behavior.

### Resource files and compilation

Load `--resources PATH` once into invocation-scoped policy before workflows run.
Pass policy into the shared DAG; expose an equivalent optional Python API for DAG
construction. Existing calls without policy behave unchanged. No global state or
injection into workflow config.

Proposed precedence, highest first:

- Resource-file override.
- Explicit rule-call resource keyword.
- Rule declaration default / legacy constraint default.

The file wins so an operator can override developer-selected execution requests.
Global capacities remain scheduling limits, not another override layer. Retain
existing behavior for requests exceeding capacity and resolving `threads="all"`.

Use exact rule names as table keys. Overrides affect declared Resource parameters
and existing explicitly declared legacy constraints. Diagnose malformed tables,
unknown resource names, invalid values, and incompatible same-name declarations
before execution. Proposed default: reject unmatched rule tables after all jobs
are compiled, catching misspelled names; this means files must match the compiled
workflow rather than serve as a silent catalogue for unrelated pipelines.

Apply effective policy before canonical DAG interning. If otherwise identical
calls still request conflicting resources, raise a clear compilation error rather
than silently choosing the first. A matching file override can resolve such a
conflict. Calls remain identical by provenance regardless of resource requests.

All CLI commands that compile jobs must accept the same resource-file policy,
including explain, graph, doctor, outputs, and dry runs. Extend matching MCP tools
where they expose compilation, and keep stored-provenance inspection independent
of current resource files. Surface effective requests in existing JSON/report
fields and identify overrides in diagnostics where useful.

### Nested command builders

Accept a zero-argument synchronous nested `def command()` in the same declaration
slot. Output declarations stay static; arbitrary Python command-building logic is
allowed inside the nested function, not in the outer declaration body.

Compile a source-backed internal builder adapter that binds declared input values,
resolved input/output Paths, and effective resource values to the nested scope.
Variadic Node inputs become tuples of Paths; mixed inputs retain existing behavior.
Use the defining module's globals for imports/helpers, with the same documented
limitations as existing command callbacks. Reject unrelated outer closure captures,
nested-command decorators, async/generator builders, and non-string results.
Do not broadly remove legacy closure restrictions to make this work.

Realize the builder once per effective execution context, through the existing
command realization/provenance-preflight path. Compilation of a pipeline must not
execute the nested body. Syntax/contract errors should fail during declaration;
runtime builder errors must fail before the external command starts.

Derive the new builder recipe identity from canonical nested-function AST and
Python identity, plus existing non-resource contracts. Never hash captured Paths,
rendered command text, resource defaults, or resource values. Provide source-path
metadata for provenance without relying on inspect.getsource for generated
adapters. Existing external callback identity stays untouched; conversion from an
external callback to a nested builder need not preserve that individual recipe hash.

## Validation and regression coverage

- Old declarations and calls retain hashes against pre-change fixtures.
- Equivalent old/inline templates retain hashes, including resource-default changes.
- Resource-file contents, file location, keyword overrides, and global caps leave
  node paths unchanged; ordinary settings and command edits still affect identity.
- All combinations of bare decorator, option-only decorator, template decorator,
  callable decorator, and factory calls resolve correctly, with useful errors.
- Parser ordering, duplicate names, missing commands, invalid placeholders, and
  unsupported Resource types fail early without evaluating declaration bodies.
- Template quoting for spaces/metacharacters, multi-output rules, Docker selection,
  retries, nullable settings, and result publication retain behavior.
- Nested builders receive correct Paths, scalars, tuples, mixed inputs, defaults,
  and resources; no cross-call capture leakage or stale realized commands.
- Scheduler charges exactly the resources seen by the command. Test rebinding
  across runs, caps, explicit requests, and `threads="all"`.
- Deduplicated calls resolve deterministically or reject resource conflicts;
  file overrides settle conflicts without changing hashes.
- Resource files validate before scheduling, remain outside job/grid config,
  and produce consistent run/explain/doctor/MCP views.
- Cached nodes remain reusable after resource changes; no metadata rewrite that
  implies the cached output was generated with the new resource allocation.
- Run targeted parser/fingerprint/resource/CLI tests per stage, then full suite
  and relevant type checks before committing a complete stage.

## Documentation and delivery

Update exports, typing examples, rule docs, resource/CLI docs, lifecycle docs,
identity invariants, AI notes, and canonical examples alongside implementation.
Deliver in reviewable stages: inline templates; Resource/per-call policy;
resource files and tools; nested builders. Each stage includes tests and docs.
Avoid bulk rewriting existing examples until compatibility tests establish that
migration is safe. No release or push is implied by this plan.

Input-validation hooks are a separate follow-up. This work provides no new
compile-time file-validation callback and does not move command execution into
pipeline construction.

## Implementation notes

Resource is an Annotated integer alias, preserving ordinary integer typing.
Legacy-only constraints keep existing first-declaration interning behavior to
preserve compatibility; conflicting new Resource calls fail as planned.
Changing file policy requires a freshly compiled DAG; per-run thread caps remain
rebindable. No commit, publication, or release has been performed by this work.

## Verification

Full suite before the final graph-label correction: 668 passed, 2 skipped.
The graph-label correction has a dedicated override regression and was checked
with the graph test suite. Pyright passed for the rule DSL typing fixture and
canonical/sage_recal examples; `git diff --check` passed.
