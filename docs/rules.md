# Rules and Typed Outputs

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Command-Call Lifecycle](rule-call-lifecycle.md)

Use executable `@command` declarations with `outputs(...)` and `shell(...)` in
the body. `@command(template)`, `@command(callback)`, the generic command
factory, direct `Command(...)` construction, and the `Rule` alias have been
removed. `CommandArgs` is removed too. Move old template text or callback logic
into the declaration body and register its template with `shell(...)`.

```python
from necroflow import Resource, command, outputs, shell

@command
def run_sage(spectra: SpectraFile, fasta: Fasta, config: SageConfig,
             threads: Resource[int] = 4):
    results_json, results_pin = outputs(SageResultsJson, SageResultsPin)
    shell("sage {config} -f {fasta} -o {workdir} {spectra}")
    return results_json, results_pin
```

Existing executable declarations keep their hashes. Migrating old syntax changes
recipe and provenance hashes: executable declarations hash their Python body,
not the old template or callback representation. Built-in text-file and symlink
helpers retain their internal recipes and remain supported.

The decorator form executes ordinary Python during graph construction.

Examples can use `import necroflow as nf` with `@nf.command`, `nf.outputs(Type)`,
and `nf.shell(...)`. Module-qualified artifact types are accepted in outputs.

For a decorated rule, the function signature is the complete input schema.
Every parameter must have a type annotation: Node dependencies use a
`Node` contract, scalar/config inputs use ordinary Python types, and a
fixed mixed union may select either kind at call time.
Necroflow rejects an unannotated parameter when the decorator constructs the
Command, even if the command does not reference that parameter. Calls likewise
reject keyword inputs absent from the declared schema before fingerprinting or
DAG interning. The function's return annotation is optional and ignored;
outputs are declared only with `outputs(ConcreteNode)`.

## Inline commands and resources

A rule can keep its command inside its declaration:

```python
from necroflow import shell
from necroflow import Resource, command, outputs

@command
def align(reads: Fastq, threads: Resource[int] = 4):
    bam = outputs(Bam)
    shell("align -t {threads} {reads} > {bam}")
    return bam
```

Declare fixed outputs first, then execute ordinary Python and register exactly
one `shell(template)`. Return only the declared artifacts. Use
`@command(repeat=2)` when execution options are needed.

```python
@command
def align(reads: Fastq, threads: Resource[int] = 4):
    bam = outputs(Bam)
    options = "-t {threads}" if threads > 1 else ""
    shell(f"align {options} {{reads}} > {{bam}}")
    return bam
```

The decorator analyses the signature and unconditional output-assignment prefix
once. On each call it applies defaults/resource overrides, creates typed artifacts
with assigned paths, and executes the original function in an isolated construction
context. `outputs(...)` retrieves those artifacts; `shell(...)` registers a template.
The wrapper validates the template and returned artifacts before interning the call.
Context is restored on success or failure. Paths exist as values, not necessarily
as files. Keep command composition in workflows; invoking another command from a
command body is rejected. Ordinary helpers may register shell in the active context.

The body runs on every call, including repeated/cache-hit constructions and CLI
inspection. Prints and side effects occur during construction, not job execution.
Avoid side effects; a failed body does not register a DAG call or create its files,
but user side effects cannot be rolled back. Closure state other than declared
artifact classes/output helpers is rejected; globals and helper implementations
are not automatically dependency-tracked. Represent result-affecting external
state explicitly as inputs/config.

Recipe identity includes normalized body source and Python identity, not resource
defaults or the constructed shell string. Migrating old declarations changes hashes. Resource branches must preserve
dependencies, output schema, and semantic results.

Python evaluates f-strings first. Write `f"tool {options} {{reads}}"` to
interpolate the local `options` value while preserving `{reads}` for necroflow.
Ordinary strings need only single braces: `"tool {reads}"`.
Literal shell braces must be doubled in the returned template; for example,
`"awk '{{print $1}}' {reads} > {bam}"`. Inside an f-string those literal braces
need four braces in source. shell variables such as `$HOME` need no escaping;
write `${{HOME}}` when using shell's braced form in an ordinary template.

Keep dynamic arguments as placeholders so necroflow can quote them. Values
already interpolated by Python cannot be automatically quoted afterward, and
any literal braces they contain must be escaped before template rendering.
Expansion happens once: braces inside substituted paths or config values stay
literal. shell templates use the existing renderer without a second interpolation pass.


`Resource[T]` marks a scheduler quantity, not a scalar setting. It requires a
positive default and can be overridden by keyword, e.g. `align(P, reads, threads=8)`.
Resources are excluded from both rule and provenance hashes. Ordinary settings
remain hashed. The annotation is an `Annotated` alias, so static analyzers see an
declared type rather than a wrapper. Supported types are `int`, `float`, and
`str` (numeric/unit strings). Quantities must be finite and positive; threads
require integers. Float resources support fractional scheduler caps.
Use `from necroflow import memory_bytes` and
`ram: Resource[int] = memory_bytes("16G")` for integer byte counts
(`G` is decimal, `Gi` is binary). Runtime resource parsing also accepts unit strings
for integer quantities. Executable declarations require concrete integer threads;
`threads="all"` is unsupported. Scheduling caps do not change arguments after
the body has executed.

Resource requests are attached to each call. New Resource declarations with
conflicting requests for the same canonical call raise a compilation error;
a common resource-file override can resolve the conflict. Decorator keyword constraints
retain their previous first-declaration deduplication behavior.

Use `--resources workstation.toml` to override declared resources automatically;
see [resource files](cli.md#resource-files). Python callers can supply the same
mapping as `DAG(nodes_dir, resources={"align": {"threads": 8}})`. This policy is
scoped to that DAG and never mutates module-level rule defaults. It applies to
existing explicitly declared constraints as well as Resource parameters.

## Reserved names

`threads: Resource[int]` is a resource declaration and is allowed.
An ordinary input or output name may not collide with the built-in placeholders `workdir` and `constraint`, with a constraint the rule
declares, or with the implicit `threads` default every rule carries even without
an explicit resource declaration. `@command` raises `ValueError` at declaration time, not
at call time, naming the colliding names. `{constraint:name}` is an explicit,
self-documenting alternative to the plain placeholder form — not a way to
disambiguate a shadowed name, since shadowing is rejected outright.

## Command input defaults

Scalar/config inputs on decorated command rules may use ordinary Python
defaults. Pure Node inputs remain explicit dependencies and must not have
defaults; mixed Node/value inputs may default to one of their value arms:

```python
@command
def run_sage(
    spectra: SpectraFile,
    train_fdr: float = 0.05,
    test_fdr: float = 0.01,
    plugin: str | None = None,
):
    results = outputs(SageResults)
    shell('sage --train-fdr {train_fdr} --test-fdr {test_fdr} {spectra} -o {results}')
    return results

P.results = run_sage(P, P.spectra)
```

Defaults are validated when the Command is declared. A config default must satisfy
its runtime-checkable annotation. A mixed-input default must satisfy one of its
non-Node arms; a managed Node cannot be captured as a default because it belongs
to one concrete DAG. Defaults on pure fixed or variadic Node inputs raise
``TypeError``. Positional mixed defaults must be trailing among positional
inputs so omission cannot shift a later required dependency. Specialized
``text_file`` declarations continue to require their text input explicitly;
``symlink(Type, path)`` requires an external path.

Defaults are expanded before a rule call is validated and fingerprinted. An
omitted value and the same value passed explicitly therefore intern to the same
Node. Overriding or changing an effective default changes the fingerprint just
like changing any other config input.

## Mixed Node/value inputs

A fixed union may combine one or more `Node` contracts with ordinary Python
types. It remains one positional logical input:

```python
@command
def consume(source: Bam | str | None = None):
    result = outputs(Result)
    shell("touch {result}" if source is None else "tool {source} > {result}")
    return result

P.managed = consume(P, P.bam)   # P.bam is a parent dependency
P.external = consume(P, "/data/external.bam")  # no parent dependency
P.absent = consume(P)           # same call identity as consume(P, None)
```

At call time, a managed `Node` is checked against the Node arms and must
belong to the compiling Pipeline's DAG. Any other value is checked against the
non-Node arms. A matching Node creates a DAG edge and contributes parent lineage
to provenance. A matching plain value creates no edge and contributes its named,
canonical value and positional input order directly to provenance. Consequently,
an external filename passed as `str` is fingerprinted by that string; necroflow
does not track or hash the external file's contents.

Mixed inputs may be passed positionally or by keyword, whichever value arm is
selected: `consume(P, value)` and `consume(P, source=value)` intern to the same
call. The body receives typed Nodes or unchanged plain values. Templates render
Nodes as paths and shell-quote substitutions. `None` renders as literal `None`;
branch in the body when absence should omit an argument.

Mixed element unions inside variadic tuples, such as
`tuple[Bam | str, ...]`, remain invalid. Variadic inputs model only ordered Node
dependency groups.

## Variadic Node inputs

Use `list[Node]` or `tuple[Node, ...]` for an ordered number of artifact inputs.
A list annotation requires a list; a tuple annotation requires a tuple. Order and
duplicates are preserved. Sets and generators are not supported as named dependency collections.
Expanded positional artifact inputs are available through the star-argument form below. The collection kind and element contract participate in recipe identity.

```python
@command
def merge(bams: list[Bam]) -> MergedBam:
    merged = outputs(MergedBam)
    shell("samtools merge {merged} {bams}")
    return merged

P.merged = merge(P, sample_bams)
```

Alternatively, annotate each expanded argument with its artifact type:

```python
@command
def merge(*bams: Bam, threads: Resource[int] = 1) -> MergedBam:
    merged = outputs(MergedBam)
    shell("samtools merge -@ {threads} {merged} {bams}")
    return merged

P.merged = merge(P, bam0, bam1, threads=4)
P.other = merge(P, *sample_bams, threads=4)
```

Inside the body, `bams` is `tuple[Bam, ...]`, following ordinary Python rules.
Fixed inputs may precede it; later config/resource parameters are keyword-only.
An empty star group is allowed unless bounded with
`*bams: Annotated[Bam, Many(min=1)]` or rejected explicitly in the body.
Node unions are supported. Scalar/mixed varargs and `**kwargs` declarations are
rejected. Group ordering, duplicates, DAG validation, and all template formats
behave identically to a named tuple input. The original Python signature is
preserved for static checking and runtime binding.

Both plain forms accept zero or more artifacts. Use
`Annotated[list[Bam], Many(min=1, max=10)]` (or the tuple equivalent) for inclusive
bounds. `Many()` defaults to a minimum of one; `max=None` is unbounded. Element
unions such as `list[Bam | Cram]` work; mixed artifact/scalar element unions do not.
Every artifact must belong to the compiling Pipeline's DAG.

Lists are copied before construction. The body receives a fresh list matching its
annotation; dependencies are recorded as an immutable tuple before body execution.
Mutating the caller's list later cannot change a call's identity, dependencies, or
rendered arguments. The template's collection binding refers to that original
snapshot, even if the body modifies its private list. Build the intended dependency
list in the workflow before calling the command.

Several groups can coexist with fixed artifact inputs.
The normal template renderer expands `{bams}` to independently quoted paths;
`flag=` and `sep=` formats below apply equally to lists and tuples.
See `examples/simple_dag.py` for a cohort workflow that collects logs in a list.

## Variadic shell argument formats

List and tuple artifact inputs support three forms. Order and duplicate inputs are preserved.

| Placeholder | Expansion | Quoting |
|---|---|---|
| `{files}` | `file0 file1` | Each path separately |
| `{files:flag=-f}` | `-f file0 -f file1` | Each flag/path separately |
| `{files:sep=,}` | `file0,file1` | Join raw paths, then quote the whole argument |

Use `shell("analyse {files:flag=-f} > {report}")` for repeated flags, or
`shell("analyse --files {files:sep=,} > {report}")` for a delimited value.
The program must accept the chosen command-line convention. `flag=` and `sep=`
are mutually exclusive, nonempty options; unknown collection formats are errors.
Empty collections emit no arguments for the default/flag forms and one empty argument
for `sep=`. Reject empty inputs in the declaration if the tool requires files.
A path containing the separator is ambiguous to the tool's list parser; shell
quoting preserves argument boundaries but cannot escape its internal format.
In f-strings, double the outer braces: `f"analyse {options} {{files:flag=-f}}"`.

## Building commands in Python

Use ordinary Python inside the declaration. Inputs and outputs remain typed
artifacts; use `.path` or `.quoted_path()` when constructing explicit paths.
Nested helpers are allowed. Register the resulting shell template exactly once
with `shell(...)`. Prefer placeholders for values needing shell quoting.

## Docker execution

A rule runs its command in Docker when the command starts with `{name}:` and
`name` is an input annotated exactly `Docker`. The image is ordinary config,
usually read from the job TOML:

```toml
[images.salmon]
image    = "quay.io/biocontainers/salmon@sha256:..."
platform = "linux/amd64"
# run_args = [...]   # optional; replaces Docker.DEFAULT_RUN_ARGS entirely
```

```python
from necroflow import Docker, command, outputs

@command(threads=1)
def index(fasta: Reference, env: Docker):
    index = outputs(Index)
    shell('{env}:salmon index --threads {threads} -t {fasta} -i {index}')
    return index

def pipeline(p, cfg):
    salmon = Docker(**cfg["images"]["salmon"])
    p.index = index(p, p.fasta, env=salmon)
```

`Docker(image, platform, run_args=Docker.DEFAULT_RUN_ARGS)` is a read-only
mapping. It requires a `@sha256:` digest and a platform, and never contacts
Docker. Because it is config, image, platform, and run args enter the provenance
of the consumer and its descendants; changing any of them yields new paths, and
switching back reuses the old ones. The defaults are
`--rm --init --pull=missing --user={uid}:{gid} --env=HOME=/tmp`. `{uid}` and
`{gid}` are expanded only at launch, so identities do not depend on the user.

Prefix rules:

- A leading `{x}:` whose `x` is not a `Docker` input is left unchanged.
- A `Docker` input without a prefix is plain config; the command runs on the host.
- Constructed commands select Docker with `shell("{env}:" + body)`.
  The template is rendered once after removing the prefix.
- The prefix covers the complete shell command, including pipes and redirections.
  The container shell is `/bin/sh -c`; `shellpath` applies to host commands only.
  Multi-line commands that should stop on the first error need `set -e`.

Framework-owned `docker run` arguments follow the user run args and cannot be
overridden: `--platform`, one read-only bind per Node input at its node-store path
(sourced from the resolved path, so a symlinked input exposes only its target),
a writable bind of the call workdir used as `--workdir`, and
`--entrypoint=/bin/sh`. Plain config values never create mounts. Directory inputs
containing symlinks that point outside the directory are unsupported. Paths
containing `,`, `"`, or newlines are rejected.

## Declaring rule outputs

Import `outputs` and `shell` with the decorator and bind every output to a real local name:

```python
from necroflow import Node, command, outputs, shell

class Bam(Node):
    filename = "aligned.bam"

class Log(Node):
    filename = "align.log"

@command
def align(reads: str):
    bam, log = outputs(Bam, Log)
    shell('aligner {reads} > {bam} 2> {log}')
    return bam, log
```

`@command` executes the body and requires a registered shell. After an optional
docstring it contains one or more top-level `name = outputs(ConcreteNode)`
assignments (or `a, b = outputs(A, B)` for multiple artifacts) and a final return of each declared name exactly once. The return order
defines the rule call's single-node or tuple shape. This is ordinary valid Python, so
linters understand that the names are bound and type checkers can preserve the return
shape through pipeline assignments.

## Conditional pipelines

Workflow functions are plain Python, so `if/else` branching on config values works naturally:

```python
from necroflow import workflow

@workflow
def my_pipeline(P: Pipeline, config) -> None:
    P.a = align(path=config.path, ref=config.ref)
    if config.call_variants:
        P.result = call_snps(P.a)
    else:
        P.result = count_reads(P.a)
```

The branching config value (`config.call_variants`) does not need to be passed to any node. The rule name already encodes which branch was taken in the fingerprint, so `call_snps` and `count_reads` always produce distinct output paths regardless.

Two pipelines sharing the same upstream config (e.g. same `path` and `ref`) will reuse the `align` output — recognised as a cache hit — even if they take different branches downstream.

## Local variables and Pipeline labels

Pipeline labels are write-once. Assigning a second Node to the same attribute
or item label raises `ValueError`:

```python
P.current = write_text(P, text=config["text"])
P.current = uppercase(P, P.current)  # ValueError: current is already assigned
```

This keeps every Pipeline label bound to one unambiguous Node for the lifetime
of the Pipeline. A label used by `.requests`, result-copy creation, graph
inspection, or `P.current` therefore always identifies the same Node. Allowing
reassignment would make earlier Nodes inaccessible under that label and make a
request for `current` dependent on when it was resolved.

Command results do not need labels immediately. Ordinary Python variables can hold
and rebind intermediate Nodes:

```python
from necroflow import workflow

@workflow
def text_pipeline(P, config):
    current = write_text(text=config["text"])
    current = uppercase(current)
    current = add_prefix(current)

    P.result = current
```

Rebinding `current` replaces the local reference; it does not mutate a Node.
Each rule call creates and interns a new Node in the shared DAG. Only the final
Node above receives the public Pipeline label `result`.
What needs to be noted, is that the not-last version of Node cannot be requested and to bypass that a user must actually come up with a unique name.
The same is the case for loops, see below.

Requiring `P.sinks()` still executes the final Node's unlabelled ancestors. An
unlabelled Node disconnected from every required output is not executed.

This pattern is useful for sequential transformations. When every loop
iteration should remain visible as a result, use a prefixed subpipeline view:

```python
from necroflow import workflow

@workflow
def step_pipeline(P: Pipeline, source, step) -> None:
    P.result = process(source, mode=step)

for i, step in enumerate(steps):
    step_pipeline(P.subpipeline(f"steps/{i}"), step_node, step)
```

This creates requestable labels such as `steps/0/result`. A subpipeline is a
view over its root: it shares the complete label/node collections, DAG, and
shell policy, while attribute and item access through the view adds its prefix.
Nested calls compose prefixes. Prefixes remain outside both fingerprints, so
equivalent calls under different prefixes return the same canonical Node.

See the [complete runnable example](../examples/local_variables.py).

The idiomatic pattern for multi-sample or multi-condition work is one shared
`DAG` and a separate `Pipeline(dag)` per config. Equivalent rule calls are
interned immediately; after each workflow, call `P.finish()`, then
`dag.require(P.sinks())` or require explicitly selected labels. The CLI calls
`finish()` automatically after a successful workflow return.
Attribute and item labels (`P.result` and `P["result"]`) share one namespace.
Item labels may be canonical relative POSIX paths, so generated targets can use
`P[f"{dataset}/{config}"]`; request them with the identical string. Components
must be non-empty, non-dot-prefixed, and neither `.` nor `..`; absolute paths and
non-canonical separators are rejected. Components and complete visible result
paths are checked by encoded byte length against Linux `NAME_MAX` and `PATH_MAX`.

## Construction boundary

`P.finish()` freezes the root and every view. Later rule calls, label
assignments, and subpipeline creation raise `RuntimeError`; the rule guard runs
before fingerprinting and interning, so a late call cannot leave an orphan in
the DAG. Only the root may finish construction. `P.sinks()` is available after
this boundary because only then is the complete public label graph known.

## Inspecting a pipeline

From the command line, render the requested job DAG without executing it:

```bash
necroflow graph job.toml
necroflow graph --output graph.tgf job.toml
```

The same rendering is available from Python:

```python
from necroflow import DAG, Pipeline

dag = DAG("results")
P = Pipeline(dag)
rna_pipeline(P, config)
P.finish()
print(P)                    # TGF DAG to stdout
P.save("pipeline.txt")      # same render to a file

dag.save("dag.txt")         # works on DAG too

for node in P.nodes:
    print(node.rule_call.resolve())   # fully-resolved shell command
```

## Types and subtypes

Node subclasses form an inheritance hierarchy — a rule accepting `Bam` also accepts `SortedBam`:

```python
class SortedBam(Bam):
    """Coordinate-sorted BAM."""
    filename = "sorted.bam"

@command
def sort(bam: Bam):
    """Sort BAM by coordinate with samtools."""
    sorted_bam = outputs(SortedBam)
    shell('samtools sort {bam} -o {sorted_bam}')
    return sorted_bam
@command
def quantify(bam: SortedBam, gene_model: str):  # only accepts sorted bam
    """Count reads per gene using featureCounts."""
    counts = outputs(Counts)
    shell('featureCounts -a {gene_model} {bam} -o {counts}')
    return counts
```

The same pattern is useful for format families. Define a base `Node` for the
format contract, then make every concrete output subclass it. Downstream rules
can accept the base class when they only care that the input is a valid member
of that family. A `Node` whose `filename` is `None` is input-only:
using it in `outputs(...)` or `Outputs(...)` raises `TypeError` when the Command is
declared. Every concrete output type must resolve to a non-`None` filename;
necroflow does not infer one from the output variable or mapping key.

This makes a filename-less base class an abstract format contract without
requiring a separate marker:

```python
class MmappetDataset(Node):
    """Base type for mmappet directory outputs."""


class PrecursorTable(MmappetDataset):
    filename = "precursors.mmappet"


class FilteredPrecursors(MmappetDataset):
    filename = "filtered.mmappet"


class IndexedDataset(Node):
    """Base type for outputs with a ready-to-query index."""


class IndexedFilteredPrecursors(FilteredPrecursors, IndexedDataset):
    filename = "indexed-filtered.mmappet"


@command
def filter_precursors(precursors: PrecursorTable):
    filtered_precursors = outputs(FilteredPrecursors)
    shell('filter-mmappet {precursors} > {filtered_precursors}')
    return filtered_precursors
@command
def index(dataset: FilteredPrecursors):
    indexed_filtered_precursors = outputs(IndexedFilteredPrecursors)
    shell('index-mmappet {dataset} > {indexed_filtered_precursors}')
    return indexed_filtered_precursors
@command
def score(dataset: MmappetDataset):
    scores = outputs(Scores)
    shell('score-mmappet {dataset} > {scores}')
    return scores
@command
def query(dataset: IndexedDataset):
    report = outputs(Report)
    shell('query-index {dataset} > {report}')
    return report
@command
def import_any_mmappet(dataset: PrecursorTable | FilteredPrecursors):
    precursor_table = outputs(PrecursorTable)
    shell('import-mmappet {dataset} > {precursor_table}')
    return precursor_table
```

Here `score()` accepts `PrecursorTable`, `FilteredPrecursors`, or
`IndexedFilteredPrecursors`, because all are `MmappetDataset` subclasses.
`query()` accepts `IndexedFilteredPrecursors`, because it also inherits from
`IndexedDataset`. Use multiple inheritance for combined requirements: "must be
both filtered precursors and indexed". Use a union for alternatives:
`PrecursorTable | FilteredPrecursors` means "either concrete contract is fine".
Mixed unions such as `PrecursorTable | str | None` additionally allow a plain
positional value or absence under the mixed-input rules above.

Unions are for inputs only. A rule output should be a concrete `Node`, not a
union, because necroflow needs one exact artifact type to choose the filename,
node identity, downstream type, and provenance shape. If two rules can produce
alternative formats, give each producer a concrete output type and let downstream
consumers accept the alternatives with a union input.

Keep structural or semantic checks close to pipeline construction with a
validator when a config path is imported from outside the DAG:

```python
from pathlib import Path


def validate(config):
    path = Path(config["precursors"])
    if path.suffix != ".mmappet" or not path.is_dir():
        raise ValueError("precursors must be a .mmappet directory")
    if not (path / "precursors.parquet").exists():
        raise ValueError("invalid mmappet dataset: missing precursors.parquet")
```

## External-file ingestion

```python
from necroflow import symlink, workflow

@workflow
def build(P, config):
    P.fastq = symlink(Fastq, config["reads"])
```

`symlink(Type, path)` returns an instance of the concrete Node subclass using
its declared filename. It accepts string or text `PathLike` paths. Workflow
ownership is implicit; outside workflows use `symlink(Type, path, pipeline=P)`.
An explicit owner overrides the active context for that call. Labels remain
explicit, construction creates no files, and finished Pipelines reject new calls.

The generated rule is named `symlink_<TypeName>` with output name `file`.
Fingerprints include the qualified artifact type, filename, path text, and usual
shell policy, so the same type/path deduplicates within a shared DAG regardless
of labels or call locations. Distinct qualified types retain distinct identities.
External bytes participate in normal content-change detection after ingestion;
see [external dataset ingestion](caching.md#external-dataset-ingestion).

For custom rule names or input/output names use `symlink_file_rule(...)`.
`@symlink_file` has been removed; replace its declarations/calls with the helper.
Generated rule identities can change existing cache addresses.

## Multi-output rules

A rule with multiple declared outputs is one atomic RuleCall. Requesting either output activates, executes, validates, hashes, reports, retains, and cleans all co-outputs together; CLI results still copy only requested Nodes:

```python
@command(threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM, capturing the log."""
    bam, log = outputs(Bam, Log)
    shell('bwa mem {ref} {fastq} > {bam} 2> {log}')
    return bam, log
dag = DAG("nodes")
P = Pipeline(dag)
P.fastq = symlink(Fastq, config.path, pipeline=P)
P.bam, P.log = align(P, P.fastq, ref="hg38")
P.finish()
dag.require(P.sinks())
```

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Command-Call Lifecycle](rule-call-lifecycle.md)

## Command declaration class

`Command` is the declaration class returned by `@command`,
`@text_file`, and `symlink_file_rule(...)`. It is available as `from necroflow import Command`.
Calling a Command compiles output Nodes; `RuleCall` remains the invocation/execution
object. Use `Command` for type checks, not direct construction. The `Rule` alias
and generic factory are removed.

## Generic output typing

`outputs(ProcessedText)` is inferred as `ProcessedText`, retaining the output
contract for LSP completion and static checks. Existing declarations need no changes.
An explicit return annotation is optional:

```python
@command
def process_text(raw_text: RawText) -> ProcessedText:
    processed_text = outputs(ProcessedText)
    shell("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
    return processed_text
```

Multiple outputs infer `tuple[Bam, Log]`. Input annotations describe actual
artifact instances passed by callers and received by executable bodies. Decorated
commands preserve parameter types and Python argument binding, including
positional-only and keyword-only parameters. Bare `Node` accepts any artifact.
Dynamic Pipeline labels have no static subtype information; keep the direct
command result when precise typing matters.

Outputs are instances of their declared subclasses. Executable declarations use
a source-based construction recipe. Return
annotations do not replace explicit `outputs(...)` declarations.
