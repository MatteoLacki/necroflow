# Fingerprint and Cache Identity Design

This is the single reference for Necroflow's hash and fingerprint design. It
records current behavior, the proposed callable-command extension, and the
policy choices that need approval.

## Approved v2 decisions

This section supersedes conflicting alternatives later in this historical
design record:

- Commands are either static shell strings or source-inspectable,
  closure-free Python functions/lambdas returning a complete shell string.
  Static and callback argv-list results are not supported.
- `CommandArgs` is an immutable resolved execution view. `FingerprintArgs` is
  a separate immutable logical rule-call view containing the original command;
  neither contains or mutates the other.
- `default_fingerprint(FingerprintArgs)` hashes callable AST when present and
  always includes the Python implementation and full major/minor/micro version.
- A job-wide `.fingerprint` function replaces the complete default policy for
  every concrete rule call and may call `default_fingerprint` to compose.
- Fingerprint v2 uses typed, length-framed canonical values and deliberately
  changes all old cache paths. Project functions return a full 64-character
  lowercase hexadecimal digest; paths retain the first 16 characters.
- Constraints and `repeat` remain available to project functions but excluded
  from the built-in default. Explicit shellpath participates in every command
  rule because all commands are shell strings.
- Callback authors are responsible for returning valid, correctly quoted shell
  text. Necroflow executes it unchanged.

Three mechanisms must not be conflated:

- A **computation fingerprint** asks whether two nodes are the same logical
  computation and determines their output path.
- An **output content hash** asks whether the bytes of an existing parent have
  changed and participates in stale detection.
- An **invalidator token** asks whether an external dependency has changed and
  also participates in stale detection.

Only the first determines a path. Content hashes and invalidators cause work to
be rerun at the same path.

## Recommended callable-recipe design

1. Preserve the identity of all existing string and argv commands.
2. Accept `recipe(args: CommandArgs) -> str | list[str]`.
3. Reject closures. Accept inspectable top-level functions and unambiguous,
   source-inspectable, closure-free lambdas.
4. Give Python recipes a default identity made from a canonical AST dump.
5. Optionally import one job-wide `.recipe_hash` function. It receives raw
   source and rule information in `HashArgs` and returns the complete recipe
   identity. This replaces the default policy; a public
   `default_recipe_hash(args)` lets it opt back into standard AST hashing.
6. Calculate identity once per distinct callable rule definition in an
   expanded job, before inserting its nodes into the deduplicating DAG.
7. Store it on the concrete job/rule call, not the reusable global `Rule`.
8. Let co-output nodes share a rule-call object that lazily realizes and caches
   the command once after paths have been assigned.
9. Record the recipe identity, source, provider, result kind, and realized
   command in `.rip/dependencies.toml`.
10. Defer variadic `Many(...)` inputs to a separate feature.

This avoids changing existing cache paths and avoids putting resolved absolute
paths into computation identity.

## Current computation identity

### Addressing

`Node.fingerprint` is SHA-256 truncated to 16 hexadecimal characters (64
bits). The node key and path are:

```text
{rule_name}/{fingerprint}/{filename}
{nodes_dir}/{rule_name}/{fingerprint}/{filename}
```

A `NodeType.filename` overrides the declared output name as the filename.

### Exact current hash inputs

The implementation feeds these values directly into SHA-256, in order:

1. `rule.__name__`.
2. The recipe representation:
   - `"recipe:" + rule.recipe_identity` when it exists;
   - otherwise the string command;
   - otherwise `repr(command)` for argv;
   - otherwise an empty string.
3. Config sorted by name, as `f"{name}={value!r}"`.
4. Execution context sorted by name, as `f"x:{name}={value!r}"`.
5. Parents in call order: each parent fingerprint, then the selected parent
   output name.
6. Declared inputs sorted by name, as `f"i:{name}={type_name}"`.
7. Declared outputs sorted by name, as `f"o:{name}={type_name}"`.

The current implementation does not length-frame these pieces.

The node's own output name is deliberately absent. Co-outputs therefore share
one computation fingerprint and directory; distinct filenames make their keys
different. A child hashes both the parent fingerprint and selected parent
output name, so consuming a different co-output is distinguishable.

Parent order is significant, and parent fingerprints recursively encode the
whole upstream lineage. Union `NodeType` names are normalized, so merely
reordering union members does not change identity.

### Inputs deliberately absent today

These do not currently affect computation fingerprints:

- `repeat`;
- constraints, including `threads`;
- the default system shell;
- `NodeType.invalidator` tokens;
- current output bytes and timestamps;
- the nodes directory and resolved paths;
- Python, Necroflow, OS, executable, and environment versions unless a pipeline
  represents them through config, execution context, recipe identity, or an
  invalidator.

An explicit `shellpath` is a special case. The executor normalizes it and puts
it into `execution_context` for static string commands, so it changes their
fingerprints. It is excluded for list commands and built-in materializers. The
DAG index is rebuilt after shell context changes.

Constraints deserve an explicit warning. They are visible to a command
template, including `{threads}`, but are treated as execution resources rather
than semantic inputs. Two otherwise equal nodes with different thread counts
therefore share an address even if their realized command lines differ. A value
that can change result semantics must currently be config. Callable recipes
should preserve this behavior initially rather than silently changing cache
semantics.

### Existing explicit recipe identity

`Rule.recipe_identity` already replaces command representation in the outer
fingerprint. The built-in text materializer uses a versioned identity similar
to:

```text
necroflow.text_file/v1:encoding=...:input=...:output=...
```

Its text payload is still covered by config hashing. Ordinary static rules use
their command template or argv as recipe representation.

## What a fingerprint controls

- `DAG.add()` immediately deduplicates by `node.key`.
- Requests and explicit invalidations resolve to keys.
- Co-outputs share one computation and directory.
- A parent identity change recursively changes downstream paths.
- Semantically different versions can coexist when they have different
  fingerprints.

Job-specific recipe identity must therefore exist before aggregate DAG
insertion. Adding it later can deduplicate or resolve requests against the
wrong key.

`Node.fingerprint` is currently recomputed on access. Mutating command, config,
parents, contracts, recipe identity, or execution context can change a key.
The shellpath flow relies on this and rebuilds the DAG index. Callable rule-call
identity should become immutable once indexed.

## Output content hashes and stale detection

After successful execution, a full SHA-256 content hash is written to:

```text
.rip/{filename}.hash
```

Files are hashed by bytes. Directories are hashed from relative paths and file
bytes, excluding `.rip`.

Classification works broadly as follows:

1. A missing required output is `MISSING`.
2. A node with a missing or stale parent is `STALE`.
3. If a parent is newer than its child, the current parent content is compared
   with the stored content hash. Equal bytes avoid a downstream rerun; changed
   bytes make it stale.
4. A missing or changed invalidator token makes the node stale.
5. Compromised prior state is handled during executor preparation.

This never creates a new path. It reruns the same addressed node. External
symlink ingestion is an example: the key may remain path-based while changes
behind the symlink invalidate work in place.

## Invalidator tokens

A `NodeType` may define `invalidator(node) -> str`. Its result is stored at:

```text
.rip/{filename}.invalidation
```

An invalidator may cover a binary, helper script, source tree, database, or
other dependency outside the ordinary DAG. A changed value reruns the existing
address; old and new results do not coexist. Exceptions fail fast. Tokens are
checked during initial classification and after successful work, not
continuously between tasks.

Use recipe identity when versions should coexist and deduplicate independently.
Use an invalidator for freshness at one stable address.

## Current provenance

`.rip/dependencies.toml` currently records the rule, node fingerprint,
accumulated ancestor config, and optional execution context such as explicit
shellpath. It does not record the static command, `recipe_identity`, Python
source, or a hash-provider policy. Output hashes and invalidator tokens live in
their separate `.rip` files.

Callable recipes need stronger provenance because their realized command is
not itself their identity.

## Existing format limitations

### The digest is truncated to 64 bits

Under a uniform-hash assumption, collision probability is approximately
`n^2 / (2 * 2^64)`: about `2.7e-8` at one million distinct nodes and about
`2.7%` at one billion. A longer fingerprint changes every output path and
should be a deliberate cache-format migration, not part of callable recipes.

### Components are not framed

Raw components are fed successively without explicit lengths or a canonical
container. Distinct boundaries can theoretically form the same pre-hash byte
stream. A future `v2` should use versioned structured serialization. For this
feature, a canonical full recipe digest can enter the existing
`recipe_identity` slot without changing static paths.

### Config and argv use Python `repr`

This is stable for ordinary TOML-derived primitives but not a universal
canonical encoding for arbitrary Python objects, mappings, sets, or custom
classes. Callable recipes should not implicitly broaden the config domain.

## Why not hash the realized command?

The callback needs real input/output paths and `workdir`. Those paths contain
the fingerprint:

```text
fingerprint -> output path -> CommandArgs -> realized command
```

Using the realized command to derive the fingerprint creates a cycle:

```text
realized command -> fingerprint -> output path -> realized command
```

Static templates already solve this by hashing the unresolved template, not
the final absolute command. Python recipes likewise need path-independent code
identity. The realized command should be provenance, not an addressing input.

## Callable command API

### `CommandArgs`

```python
@dataclass(frozen=True)
class CommandArgs:
    inputs: NamedValues[Path]
    config: NamedValues[object]
    outputs: NamedValues[Path]
    constraints: NamedValues[object]
    workdir: Path
```

`NamedValues` is a declaration-ordered, read-only mapping with both styles:

```python
args.inputs.bam
args.inputs["bam"]
args.constraints.threads
args.constraints["threads"]
```

Contained config objects are not recursively frozen; callbacks must not mutate
them. Recipes return either a nonempty shell string or a list of strings.

### Shared call and lazy command realization

All output nodes of one invocation should point to a shared internal
`RuleCall` holding config, ordered parents, outputs, recipe identity, and a
cached command.

Path resolution first assigns paths from the already-known fingerprint. On the
first `node.command` or `resolve_command(node)` access, the rule call:

1. builds `CommandArgs` from assigned paths;
2. invokes the callback;
3. normalizes and validates its result;
4. caches it for all co-outputs and aliases.

This produces ordinary node-level behavior without a separate public
\"two-phase\" API, while invoking a multi-output callback once. An instance
cache is better than global `functools.cache`: config can be unhashable, global
entries outlive jobs, and resolving the graph against another nodes directory
must rebuild commands containing different paths.

The cache must be keyed by the normalized nodes root or cleared when paths are
resolved against a different root. Inspection, dry-run, and execution must all
share the same cached result.

## Default Python code identity

### Accepted and rejected callbacks

Accept:

- inspectable top-level named functions;
- inspectable, closure-free lambdas whose source identifies one matching
  expression.

Reject:

- callbacks with `__closure__` values or `co_freevars`;
- nested functions;
- dynamically created functions without inspectable source;
- ambiguous lambda source;
- built-ins and callable objects until an explicit identity protocol exists.

Closures hide captured state. Lambdas do not inherently do so, hence
closure-free module-level lambdas are acceptable.

### AST canonicalization

The default hasher should:

1. run `inspect.unwrap(callback)`;
2. validate closure and source requirements;
3. obtain source, starting line, and defining file with `inspect`;
4. dedent with `textwrap.dedent()` and parse with `ast.parse()`;
5. select the matching `FunctionDef`/`AsyncFunctionDef` by name and source line,
   or the unique matching `Lambda`;
6. serialize it with:

   ```python
   ast.dump(node, annotate_fields=True, include_attributes=False)
   ```

7. hash a length-framed sequence containing:
   - `necroflow.python_recipe/v1`;
   - Python's AST schema version, at least major/minor;
   - the canonical dump;
8. return the full SHA-256 string. The outer node hash still produces its
   current 16-character address.

This ignores whitespace, comments, indentation, and source locations. It
retains control flow, names, literals, defaults, annotations, decorators, and
docstrings. Keeping the complete AST is conservative: a docstring edit may
invalidate, but meaningful code is less likely to be missed. Selective
stripping would require a new versioned policy.

The source path is provenance, not default identity; moving unchanged source
should not change results.

### What AST identity cannot see

It does not automatically track imported helper implementations, reassigned
module globals, environment variables, executables, external files,
monkeypatching, or import hooks. Closures are rejected because their hidden
state is directly identifiable. Recursively discovering all global
dependencies would be unreliable; project-specific dependencies belong in the
optional hash policy.

## Custom global recipe hasher

### Discovery and protocol

One provider may be imported for a job:

```toml
".pipeline" = "pipeline.py:build_pipeline"
".recipe_hash" = "hashing.py:recipe_hash"
```

It follows the same one-object convention as command recipes:

```python
def recipe_hash(args: HashArgs) -> str:
    ...
```

Proposed immutable inputs:

```python
@dataclass(frozen=True)
class HashArgs:
    rule_name: str
    callback: Callable[[CommandArgs], str | list[str]]
    source: str
    source_path: Path
    inputs: NamedValues[type[NodeType]]
    outputs: NamedValues[type[NodeType]]
    constraints: NamedValues[object]
```

`source` is the raw dedented callback source selected by Necroflow.
`source_path` is its resolved defining file. The callback is supplied so users
may inspect its module, qualified name, annotations, or attributes. Contracts
are supplied because they describe the declaration, though the outer node hash
also includes input/output contracts.

`HashArgs` deliberately excludes resolved `CommandArgs`. Paths need identity
first, while per-call config and parents already enter the outer node hash.
This provider identifies a recipe definition, not one invocation's lineage.

The result must be a stable, nonempty string. Necroflow should expose:

```python
default_recipe_hash(args: HashArgs) -> str
```

so a provider can compose standard code identity with project inputs:

```python
def recipe_hash(args: HashArgs) -> str:
    h = hashlib.sha256()
    h.update(default_recipe_hash(args).encode())
    h.update(hash_file(Path("tools/merge.py")).encode())
    h.update(tool_version("samtools").encode())
    return h.hexdigest()
```

`functools.partial` is a supported way to bind provider configuration. The
closure restriction applies to command recipes, not the job-wide provider.

### Replacement policy

Recommendation: `.recipe_hash` replaces default AST hashing. This gives the
provider raw inputs and lets the user decide whether callback code participates.
A provider wanting the standard behavior calls `default_recipe_hash(args)`.

This permits project versioning, helper hashes, and tool versions without a
separate unexplained \"extra hash.\" The cost is that an advanced provider can
omit important code and cause stale cache hits. Its import specification and
result must therefore be visible in provenance.

An additive provider is safer but contradicts the requested ability to decide
code hashing. It remains an alternative if Necroflow wants a mandatory safety
floor.

### Invocation scope and storage

Load the provider once per expanded job and call it once per unique callable
rule definition in that job. Reuse the result for all concrete calls and
co-outputs. Config and ordered parents still distinguish calls in the outer
fingerprint.

Two jobs may use one module-level `Rule` with different providers. Never mutate
the global `Rule.recipe_identity`; store the resolved result on the job's
concrete rule-call layer.

The provider import string need not independently salt identity. Equal returned
identities explicitly claim equal recipes. Record the import string anyway.

## Shell policy for callable recipes

Execution itself is already solved: callback strings use the selected shell;
callback argv lists execute directly.

The ordering issue is only identity. Static commands reveal their kind before
hashing, whereas callbacks reveal it after paths and identity exist. Choices:

1. Salt every callable identity with explicit shellpath: safe but changes argv
   recipe paths unnecessarily.
2. Never salt callable identity with shellpath: simple, but selected shells
   share an address for string results.
3. Require `shell=True/False` on the declaration: precise but duplicates
   information and complicates the API.

Recommendation for the initial feature: option 2. Preserve current command
selection and record shell/result kind in provenance. A shell-sensitive
pipeline can make shell choice config or include it in its custom hasher.

## Callable provenance

Add a `[recipe]` table to each output's existing
`.rip/dependencies.toml`:

```toml
[recipe]
kind = "python"
source = "../../project/pipeline.py"
identity = "<full recipe identity>"
hasher = "necroflow.default_recipe_hash/v1"
command_kind = "argv"
command = ["samtools", "merge", "..."]
```

For custom policy, `hasher` is its import specification. Store `source`
relative to the user-provided nodes directory with `os.path.relpath`; `..` is
allowed when source is outside it. Store a shell result as a TOML string with
`command_kind = "shell"`.

The full recipe identity belongs in provenance even though the outer address is
only 16 hex characters.

## Compatibility boundary

Preserve in this feature:

- current outer hash ordering and 16-character truncation;
- all existing static command and materializer paths;
- exclusion of constraints and `repeat`;
- path-independent recipe identity;
- content hash and invalidator behavior.

Add:

- full, versioned identity for Python callbacks;
- a job/concrete-rule-call identity layer before DAG insertion;
- lazy once-per-call command realization;
- recipe provenance;
- a replacement custom hash policy.

Defer to separate todos/migrations:

- `Many(...)` variadic inputs;
- length-framed canonical outer serialization;
- longer path fingerprints;
- canonical arbitrary config encoding;
- changing whether constraints participate;
- general toolchain/environment capture.

## Decision table

| Question | Recommendation | Cost/trade-off |
| --- | --- | --- |
| Static command identity | Keep current template/argv | Preserves paths |
| Python recipe identity | Versioned canonical AST by default | Helpers/globals not automatic |
| Closures | Reject | Avoids captured hidden state |
| Lambdas | Allow if inspectable and unambiguous | Some lambda forms fail |
| Custom `.recipe_hash` | Replaces default; can call public default | User can omit important inputs |
| Provider interface | One immutable `HashArgs` with raw source | No resolved per-node paths |
| Provider frequency | Once/recipe definition/expanded job | Invocation data stays in outer hash |
| Command interface | One `CommandArgs`; attribute and item access | Config is not deeply frozen |
| Command realization | Lazy cache on shared `RuleCall` | Reset for another nodes root |
| Callback result | String or argv | Kind known only after addressing |
| Callable shellpath identity | Do not salt initially; record it | Shell-sensitive users opt in |
| Generated command in identity | No; provenance only | Avoids circularity |
| Constraints in identity | Preserve current exclusion | Semantic values must be config |
| Source provenance | Relative to nodes directory | May contain `..` |
| Outer hash v2 | Separate migration | Existing weaknesses remain |
| Variadic inputs | Separate later feature | Keeps first implementation focused |

## Acceptance criteria

- Existing static fingerprints and paths remain byte-for-byte unchanged.
- Formatting/comments alone do not change default Python identity.
- Semantic AST changes do change identity and paths.
- Closures fail with captured names in the diagnostic.
- A closure-free inspectable lambda works; ambiguous source fails.
- A custom provider may call or omit the default hasher.
- Provider results control deduplication and it runs once per definition/job.
- Config and parents still distinguish calls sharing one recipe identity.
- Attribute and item access on `CommandArgs` namespaces are equivalent.
- String and argv results use existing execution paths.
- Multi-output callbacks run once.
- Resolving under a new nodes directory regenerates absolute commands without
  changing computation identity.
- Identity exists before requests, invalidations, and DAG deduplication.
- `dependencies.toml` records full identity, relative source, provider, result
  kind, realized command, and explicit shell context.
- Content hashes and invalidators retain stale-in-place semantics.

## Decisions requiring approval

1. A custom `.recipe_hash` fully replaces default AST hashing, with public
   `default_recipe_hash(args)` for opt-in composition.
2. Callable recipes initially preserve constraint exclusion and do not add
   explicit shellpath to their identity.
3. Outer fingerprint modernization (framing and more than 64 bits) is a
   separate migration, preserving all current static cache paths here.
