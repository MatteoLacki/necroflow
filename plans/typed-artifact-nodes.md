# Typed artifact nodes

Status: implemented. Node subclasses now describe and instantiate artifacts.

## Target

```python
class RawText(Node):
    filename = "input.txt"

class ProcessedText(Node):
    filename = "processed.txt"

@command
def process_text(raw_text: RawText, threads: Resource[int] = 1) -> ProcessedText:
    processed_text = output(ProcessedText)

    def command():
        src = shlex.quote(str(raw_text.path))
        dst = shlex.quote(str(processed_text.path))
        return f"tr '[:lower:]' '[:upper:]' < {src} > {dst}"

    return processed_text
```

The class is the artifact contract. An instance is an artifact in a DAG.
The same objects reach pipeline callers and nested command builders. Literal
command templates continue to render and quote artifact paths automatically.
`output()` remains declaration-only; the outer function body is not executed.

## Keep the implementation small

Replace the current split instead of supporting both designs. Remove NodeType,
generic Node[T], and their exports/annotations. No aliases for the removed model,
compatibility flags, conversion wrappers, LSP plugin, or generated stub system.
Do not add __str__ path conversion, __fspath__, Path-method forwarding, or implicit
quoting to artifacts. Python builders explicitly use `.path`; templates remain terse.
Keep existing parser, command builder, DAG, and executor modules. No new subsystem.
Leave broader return-annotation-driven output declarations out of scope.

## Changes

### One artifact base class

Move `filename` and `invalidator` onto Node as class-level declarations. Retain
existing instance metadata, shared RuleCall ownership, equality, and hashing.
Make output construction instantiate the declared subclass (`ProcessedText(...)`)
rather than a generic Node carrying a separate type field. Plain subclasses inherit
the framework constructor; do not introduce custom constructors or initialization
hooks as an extension API.

Replace internal `node.node_type` reads with `type(node)` and remove the redundant
stored field. Use Node subclass checks for input/output contracts. Filename-less
bases remain valid abstract input contracts; concrete outputs require filenames.
Invalidators must still be retrieved from the class and explicitly called with
the artifact, avoiding accidental bound-method behavior.

Keep construction and path assignment framework-owned; do not add machinery merely
to forbid users from manually calling the inherited constructor. Cross-DAG input
checks, output collisions, and canonical interning remain enforced.

### Exact output and call typing

Use `T = TypeVar("T", bound=Node)` and `output(type[T]) -> T`.
The existing declaration parser still discovers output names/types. Return types
are inferred from those local values, or explicitly annotated by users.
Multiple outputs infer `tuple[Bam, Log]`; runtime named output access can remain,
but do not build machinery to manufacture static named-tuple classes.

Preserve decorated function parameters and result using ParamSpec and the existing
Command wrapper. Provide scoped calls plus the existing explicit Pipeline prefix
through two __call__ overloads; verify the overloads with Pyright before expanding
implementation. Apply the same typing approach to text/symlink decorators.

Use the original Python parameter kinds for decorated call binding. A declaration
that permits positional scalar/resource arguments should accept them at runtime;
users write `*` when they want keyword-only arguments. Do not preserve today's
hidden keyword-only transformation while claiming to preserve the signature.
Explicit factory-created Commands retain their dynamically assembled schema and
broad static typing; do not create a second signature-generation system for them.

### Typed objects in nested builders

Give NestedCommand its bindings from the RuleCall's actual input/output objects,
plus scalar settings and resolved resources. Variadic inputs remain tuples of
artifacts; mixed Node/value inputs retain the selected value. No conversion to Path
occurs at this boundary.

Keep one existing path-rendering boundary for templates and external CommandArgs
callbacks. Those APIs already explicitly model resolved paths; they need no new
mode or adapter. Do not route the new nested builder through path-valued CommandArgs
and then reconstruct artifacts. Share command-result validation and Docker-prefix
handling with the existing realization path.

### Identity

Retain artifact class module/name and filename identity, without hashing the new
base class or instance layout. Leave canonical_bytes and global hash domains alone;
verify equivalent template declarations against pre-change hashes.

Nested-builder binding semantics change from Paths to artifact instances. Stamp
that change only into nested-builder recipe identity (a single fixed format marker
alongside its AST/Python identity). Otherwise an unchanged builder body could reuse
an output produced under different runtime semantics. No old-binding fallback.
Resource values remain excluded from both hashes.

### Migrate the repository in the same change

Update code, examples, packaged starter template, tests, typing fixtures, and
current docs to subclass Node and use concrete artifact return types. Update
nested Python builders to access `.path`; leave template strings unchanged.
Remove obsolete generic typing fixtures and conflicting documentation instead of
retaining parallel old/new examples. No compatibility-only module or test suite.

## Verification

- LSP infers concrete single and tuple output types, plus `.path: Path`.
- Correct inputs and subtype inputs type-check; wrong artifact types, missing
  arguments, and wrong scalar/resource values produce static diagnostics.
- Scoped and explicit-Pipeline calls agree with their runtime signatures,
  including positional, keyword-only, default, mixed, and variadic arguments.
- Declared outputs are actual instances of the specified subclass; canonical
  interning, subclass input acceptance, invalidators, and cross-DAG rejection work.
- Nested builders receive those instances and can use artifact-specific methods;
  templates/external callbacks still receive their documented path representations.
- Template hashes remain stable; nested-builder binding marker changes only those
  recipes. Resource changes still leave identity untouched.
- Existing execution, retry, provenance, result-copy, and cache regressions pass.

Use the existing typing fixture and focused runtime tests; add only missing
behavioral coverage. Run relevant Pyright checks and the full suite once integrated.
Pre-implementation checkpoint: 6a5ba54. Implementation remains a separate working-tree change.
