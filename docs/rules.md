# Rules and Typed Outputs

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Command-Call Lifecycle](rule-call-lifecycle.md)

The primary API is executable `@command` declarations with `shell`. The explicit factory form is also available. Factory rules require a stable `name=` and may include `doc=`; `Inputs` and `Outputs` preserve declaration order.

```python
run_sage = command(
    "sage {config} -f {fasta} -o {workdir} {spectra}",
    Inputs(spectra=SpectraFile, fasta=Fasta, config=SageConfig),
    Outputs(results_json=SageResultsJson, results_pin=SageResultsPin),
    Constraints(threads=4),
    name="run_sage",
    doc="Run Sage.",
)
```

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
defaults or the constructed shell string. This new recipe kind changes hashes for
migrated declarations. Existing factory recipes retain their hashes. Resource
branches must preserve dependencies, output schema, and semantic results.
External CommandArgs callbacks retain their existing separate callback lifecycle.

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
`threads="all"` remains supported only by factory/callback commands. Scheduling
caps do not change arguments after the body has executed.

Resource requests are attached to each call. New Resource declarations with
conflicting requests for the same canonical call raise a compilation error;
a common resource-file override can resolve the conflict. Legacy-only constraints
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
`Constraints(...)`. `Command`/`@command` raises `ValueError` at declaration time, not
at call time, naming the colliding names. `{constraint:name}` is an explicit,
self-documenting alternative to the plain placeholder form — not a way to
disambiguate a shadowed name, since shadowing is rejected outright.

## All available threads

Use `threads="all"` in a decorated command or `Constraints(threads="all")`
in a factory rule. The executor reserves its full thread cap for that call,
and `{threads}`, `{constraint:threads}`, and Python command callbacks receive
the effective number. For example, with `-c 8` the following rule runs with
eight threads; with `-c all` it uses the machine's detected core count.

```python
@command("samtools sort -@ {threads} {bam} -o {sorted_bam}", threads="all")
def sort_bam(bam: Bam):
    sorted_bam = outputs(SortedBam)
    return sorted_bam
```

## Command input defaults

Scalar/config inputs on decorated command rules may use ordinary Python
defaults. Pure Node inputs remain explicit dependencies and must not have
defaults; mixed Node/value inputs may default to one of their value arms:

```python
@command("sage --train-fdr {train_fdr} --test-fdr {test_fdr} {spectra} -o {results}")
def run_sage(
    spectra: SpectraFile,
    train_fdr: float = 0.05,
    test_fdr: float = 0.01,
    plugin: str | None = None,
):
    results = outputs(SageResults)
    return results

P.results = run_sage(P, P.spectra)
```

Explicit factory rules declare the same policy with ``input_defaults``:

```python
run_sage = command(
    "sage --train-fdr {train_fdr} --test-fdr {test_fdr} {spectra} -o {results}",
    Inputs(
        spectra=SpectraFile,
        train_fdr=float,
        test_fdr=float,
        plugin=str | None,
    ),
    Outputs(results=SageResults),
    name="run_sage",
    input_defaults={
        "train_fdr": 0.05,
        "test_fdr": 0.01,
        "plugin": None,
    },
)
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
@command(select_command)
def consume(source: Bam | str | None = None):
    result = outputs(Result)
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
call. In `CommandArgs`, the value remains under `args.inputs.source`: managed
Nodes resolve to `Path`; plain values remain unchanged. Static command templates
stringify and shell-quote the selected value with the normal substitution
policy. In particular, `None` renders as the literal `None`; use a Python
command callback when absence should omit an argument.

Mixed element unions inside variadic tuples, such as
`tuple[Bam | str, ...]`, remain invalid. Variadic inputs model only ordered Node
dependency groups.

## Variadic Node inputs

Use `tuple[Node, ...]` when a rule consumes an ordered number of Nodes
that is only known while the workflow runs. The call receives one actual
tuple for that named input:

```python
from typing import Annotated
from necroflow import Many

merge = command(
    "samtools merge {merged} {bams}",
    Inputs(bams=Annotated[tuple[Bam, ...], Many(min=1, max=10)]),
    Outputs(merged=MergedBam),
    name="merge",
)

P.merged = merge(P, tuple(sample_bams))
```

A plain `tuple[Bam, ...]` accepts zero or more Nodes. `Many()` changes the
default minimum to one; `min` and `max` are inclusive, and `max=None` is
unbounded. Values must be tuples rather than lists, generators, or expanded
positional arguments. Tuple elements retain their order and must belong to the
same DAG as the compiling Pipeline. Node unions also work, for example
`tuple[Bam | Cram, ...]`.

Several variadic groups may appear alongside fixed Node inputs. Each group is
still one positional rule argument:

```python
P.comparison = compare(
    P,
    tuple(tumor_bams),
    P.reference,
    tuple(normal_bams),
)
```

In a static shell template, `{bams}` expands to one shell-quoted path per tuple
element, separated by spaces. An empty plain tuple expands to the empty string.
For repeated flags or another layout, use a Python command callback; its
`args.inputs.bams` is an ordered `tuple[Path, ...]`. Group membership, element
order, input name, and any `Many` bounds participate in the default fingerprint.

## Variadic shell argument formats

Tuple artifact inputs support three forms. Order and duplicate inputs are preserved.

| Placeholder | Expansion | Quoting |
|---|---|---|
| `{files}` | `file0 file1` | Each path separately |
| `{files:flag=-f}` | `-f file0 -f file1` | Each flag/path separately |
| `{files:sep=,}` | `file0,file1` | Join raw paths, then quote the whole argument |

Use `shell("analyse {files:flag=-f} > {report}")` for repeated flags, or
`shell("analyse --files {files:sep=,} > {report}")` for a delimited value.
The program must accept the chosen command-line convention. `flag=` and `sep=`
are mutually exclusive, nonempty options; unknown tuple formats are errors.
Empty tuples emit no arguments for the default/flag forms and one empty argument
for `sep=`. Reject empty inputs in the declaration if the tool requires files.
A path containing the separator is ambiguous to the tool's list parser; shell
quoting preserves argument boundaries but cannot escape its internal format.
In f-strings, double the outer braces: `f"analyse {options} {{files:flag=-f}}"`.

## Python command callbacks

When a command must be assembled from resolved values, pass a module-level
Python function instead of a static template:

```python
from necroflow import CommandArgs

def merge_command(args: CommandArgs) -> str:
    return "samtools merge --threads {threads} {merged} {first} {second}"

merge = command(
    merge_command,
    Inputs(first=Bam, second=Bam),
    Outputs(merged=MergedBam),
    Constraints(threads=8),
    name="merge",
)
```

`CommandArgs` contains read-only named `inputs`, `config`, `outputs`, and
`constraints` collections plus `workdir`. Names support both attribute and
mapping access, such as `args.outputs.merged` and `args.outputs["merged"]`.
`inputs` contains resolved Node paths, Node-path tuples, and selected plain
values from mixed inputs; `config` contains keyword scalar/config inputs.

Callbacks return templates. Necroflow substitutes and shell-quotes placeholders,
just as for literal commands. Keep dynamic arguments as placeholders
when interpolated values require shell escaping. Argv-list commands are not
supported in fingerprint v4.

External command callbacks must be module-level, closure-free functions or unambiguous
source-file lambdas accepting exactly one argument. Their canonical AST and
the running Python implementation/version participate in the default
fingerprint.

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

@command("{env}:salmon index --threads {threads} -t {fasta} -i {index}", threads=1)
def index(fasta: Reference, env: Docker):
    index = outputs(Index)
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
- Python callbacks select Docker by returning the literal prefix, e.g.
  `return "{env}:" + body`. The remainder is not formatted again.
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

Import `output` with the decorator and bind every output to a real local name:

```python
from necroflow import Node, command, outputs

class Bam(Node):
    filename = "aligned.bam"

class Log(Node):
    filename = "align.log"

@command("aligner {reads} > {bam} 2> {log}")
def align(reads: str):
    bam, log = outputs(Bam, Log)
    return bam, log
```

For the explicit `@command(template)` form shown above, the body stays declaration-only.
Bare `@command` instead executes the body and requires a registered shell. After an optional
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

@command("samtools sort {bam} -o {sorted_bam}")
def sort(bam: Bam):
    """Sort BAM by coordinate with samtools."""
    sorted_bam = outputs(SortedBam)
    return sorted_bam
@command("featureCounts -a {gene_model} {bam} -o {counts}")
def quantify(bam: SortedBam, gene_model: str):  # only accepts sorted bam
    """Count reads per gene using featureCounts."""
    counts = outputs(Counts)
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


@command("filter-mmappet {precursors} > {filtered_precursors}")
def filter_precursors(precursors: PrecursorTable):
    filtered_precursors = outputs(FilteredPrecursors)
    return filtered_precursors
@command("index-mmappet {dataset} > {indexed_filtered_precursors}")
def index(dataset: FilteredPrecursors):
    indexed_filtered_precursors = outputs(IndexedFilteredPrecursors)
    return indexed_filtered_precursors
@command("score-mmappet {dataset} > {scores}")
def score(dataset: MmappetDataset):
    scores = outputs(Scores)
    return scores
@command("query-index {dataset} > {report}")
def query(dataset: IndexedDataset):
    report = outputs(Report)
    return report
@command("import-mmappet {dataset} > {precursor_table}")
def import_any_mmappet(dataset: PrecursorTable | FilteredPrecursors):
    precursor_table = outputs(PrecursorTable)
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
@command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM, capturing the log."""
    bam, log = outputs(Bam, Log)
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

`Command` is the declaration class returned by `@command`, `command(...)`,
`@text_file`, and `symlink_file_rule(...)`. It is available as `from necroflow import Command`.
Calling a Command compiles output Nodes; `RuleCall` remains the invocation/execution
object. Existing `from necroflow.rules import Rule` imports remain supported:
`Rule` is an alias of `Command`, not a separate type. The rename does not alter
recipe or provenance identity.

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
a source-based construction recipe; existing factory fingerprints remain stable. Return
annotations do not replace explicit `outputs(...)` declarations.
