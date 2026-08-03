# Rules and Typed Outputs

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Rule-Call Lifecycle](rule-call-lifecycle.md)

The primary API is the explicit factory form. Factory rules require a stable `name=` and may include `doc=`; `Inputs` and `Outputs` preserve declaration order.

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

The decorator form below remains supported as syntactic sugar.

For a decorated rule, the function signature is the complete input schema.
Every parameter must have a type annotation: Node dependencies use a
`NodeType` contract, while scalar/config inputs use ordinary Python types.
Necroflow rejects an unannotated parameter when the decorator constructs the
Rule, even if the command does not reference that parameter. Calls likewise
reject keyword inputs absent from the declared schema before fingerprinting or
DAG interning. The function's return annotation is optional and ignored;
outputs are declared only with `output(ConcreteNodeType)`.

## Command input defaults

Scalar/config inputs on decorated command rules may use ordinary Python
defaults. Node inputs remain explicit dependencies and must not have defaults:

```python
@command("sage --train-fdr {train_fdr} --test-fdr {test_fdr} {spectra} -o {results}")
def run_sage(
    spectra: SpectraFile,
    train_fdr: float = 0.05,
    test_fdr: float = 0.01,
    plugin: str | None = None,
):
    results = output(SageResults)
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

Defaults are validated when the Rule is declared. A default may name only a
declared scalar/config input and must satisfy its runtime-checkable annotation;
a default on a fixed or variadic Node input raises ``TypeError``. Specialized
``text_file`` and ``symlink_file`` declarations continue to require their one
input explicitly.

Defaults are expanded before a rule call is validated and fingerprinted. An
omitted value and the same value passed explicitly therefore intern to the same
Node. Overriding or changing an effective default changes the fingerprint just
like changing any other config input.

## Variadic Node inputs

Use `tuple[NodeType, ...]` when a rule consumes an ordered number of Nodes
that is only known while the pipeline factory runs. The call receives one actual
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
same DAG as the compiling Pipeline. NodeType unions also work, for example
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

## Python command callbacks

When a command must be assembled from resolved values, pass a module-level
Python function instead of a static template:

```python
import shlex
from necroflow import CommandArgs

def merge_command(args: CommandArgs) -> str:
    parts = [
        "samtools",
        "merge",
        "--threads",
        str(args.constraints.threads),
        str(args.outputs.merged),
        *(str(path) for path in args.inputs.values()),
    ]
    return shlex.join(parts)

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

Callbacks return a complete shell string. Necroflow executes it unchanged and
does not attempt to infer or repair quoting; use `shlex.quote` or `shlex.join`
when interpolated values require shell escaping. Argv-list commands are not
supported in fingerprint v3.

Command callbacks must be module-level, closure-free functions or unambiguous
source-file lambdas accepting exactly one argument. Their canonical AST and
the running Python implementation/version participate in the default
fingerprint.

## Declaring rule outputs

Import `output` with the decorator and bind every output to a real local name:

```python
from necroflow import NodeType, command, output

class Bam(NodeType):
    filename = "aligned.bam"

class Log(NodeType):
    filename = "align.log"

@command("aligner {reads} > {bam} 2> {log}")
def align(reads: str):
    bam = output(Bam)
    log = output(Log)
    return bam, log
```

The decorated body is a declaration, not executable rule code. After an optional
docstring it contains one or more top-level `name = output(ConcreteNodeType)`
assignments and a final return of each declared name exactly once. The return order
defines the rule call's single-node or tuple shape. This is ordinary valid Python, so
linters understand that the names are bound and type checkers can preserve the return
shape through pipeline assignments.

## Conditional pipelines

Pipeline factory functions are plain Python, so `if/else` branching on config values works naturally:

```python
def my_pipeline(P: Pipeline, config) -> None:
    P.a = align(P, path=config.path, ref=config.ref)
    if config.call_variants:
        P.result = call_snps(P, P.a)
    else:
        P.result = count_reads(P, P.a)
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

Rule results do not need labels immediately. Ordinary Python variables can hold
and rebind intermediate Nodes:

```python
def text_pipeline(P, config):
    current = write_text(P, text=config["text"])
    current = uppercase(P, current)
    current = add_prefix(P, current)

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
def step_pipeline(P: Pipeline, source, step) -> None:
    P.result = process(P, source, mode=step)

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
interned immediately; after each factory, call `P.finish()`, then
`dag.require(P.sinks())` or require explicitly selected labels. The CLI calls
`finish()` automatically after a successful factory return.
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
necroflow graph --output graph.txt job.toml
```

The same rendering is available from Python:

```python
from necroflow import DAG, Pipeline, resolve_command

dag = DAG("results")
P = Pipeline(dag)
rna_pipeline(P, config)
P.finish()
print(P)                    # layered ASCII DAG to stdout
P.save("pipeline.txt")      # same render to a file

dag.save("dag.txt")         # works on DAG too

for node in P.nodes:
    print(resolve_command(node))   # fully-resolved shell command
```

## Types and subtypes

NodeTypes form an inheritance hierarchy — a rule accepting `Bam` also accepts `SortedBam`:

```python
class SortedBam(Bam):
    """Coordinate-sorted BAM."""
    filename = "sorted.bam"

@command("samtools sort {bam} -o {sorted_bam}")
def sort(bam: Bam):
    """Sort BAM by coordinate with samtools."""
    sorted_bam = output(SortedBam)
    return sorted_bam
@command("featureCounts -a {gene_model} {bam} -o {counts}")
def quantify(bam: SortedBam, gene_model: str):  # only accepts sorted bam
    """Count reads per gene using featureCounts."""
    counts = output(Counts)
    return counts
```

The same pattern is useful for format families. Define a base `NodeType` for the
format contract, then make every concrete output subclass it. Downstream rules
can accept the base class when they only care that the input is a valid member
of that family. A `NodeType` whose `filename` is `None` is input-only:
using it in `output(...)` or `Outputs(...)` raises `TypeError` when the Rule is
declared. Every concrete output type must resolve to a non-`None` filename;
necroflow does not infer one from the output variable or mapping key.

This makes a filename-less base class an abstract format contract without
requiring a separate marker:

```python
class MmappetDataset(NodeType):
    """Base type for mmappet directory outputs."""


class PrecursorTable(MmappetDataset):
    filename = "precursors.mmappet"


class FilteredPrecursors(MmappetDataset):
    filename = "filtered.mmappet"


class IndexedDataset(NodeType):
    """Base type for outputs with a ready-to-query index."""


class IndexedFilteredPrecursors(FilteredPrecursors, IndexedDataset):
    filename = "indexed-filtered.mmappet"


@command("filter-mmappet {precursors} > {filtered_precursors}")
def filter_precursors(precursors: PrecursorTable):
    filtered_precursors = output(FilteredPrecursors)
    return filtered_precursors
@command("index-mmappet {dataset} > {indexed_filtered_precursors}")
def index(dataset: FilteredPrecursors):
    indexed_filtered_precursors = output(IndexedFilteredPrecursors)
    return indexed_filtered_precursors
@command("score-mmappet {dataset} > {scores}")
def score(dataset: MmappetDataset):
    scores = output(Scores)
    return scores
@command("query-index {dataset} > {report}")
def query(dataset: IndexedDataset):
    report = output(Report)
    return report
@command("import-mmappet {dataset} > {precursor_table}")
def import_any_mmappet(dataset: PrecursorTable | FilteredPrecursors):
    precursor_table = output(PrecursorTable)
    return precursor_table
```

Here `score()` accepts `PrecursorTable`, `FilteredPrecursors`, or
`IndexedFilteredPrecursors`, because all are `MmappetDataset` subclasses.
`query()` accepts `IndexedFilteredPrecursors`, because it also inherits from
`IndexedDataset`. Use multiple inheritance for combined requirements: "must be
both filtered precursors and indexed". Use a union for alternatives:
`PrecursorTable | FilteredPrecursors` means "either concrete contract is fine".
Mixed unions such as `NodeType | str` are rejected because node inputs and config
inputs are different parts of the rule API.

## Mutable NodeTypes

Set `mutable = True` on a concrete `NodeType` for persistent state, such as a
SQLite database, whose bytes may legitimately change in place without
invalidating every consumer:

```python
class Database(NodeType):
    filename = "state.sqlite3"
    mutable = True
```

The inherited default is `False`, and Rule declaration rejects non-boolean
values. Necroflow copies the resolved flag onto each concrete Node. Mutable
Nodes remain ordinary parents for commands, graph traversal, scheduling,
provenance, and fingerprints. Only the newer-parent content comparison is
skipped. A missing, stale, compromised, forcibly invalidated, or
`invalidator`-changed mutable parent still stales consumers. Changing the
producer identity also changes downstream fingerprints.

Mutable inputs may be modified by commands, but necroflow does not serialize
writers, prevent external changes, or provide transactions. Rules sharing a
mutable input may run concurrently unless the pipeline author creates explicit
ordering or resource constraints. Use this escape hatch only when those races
and the cache consequences are understood. Mutable outputs are never removed
by autoclean.

Unions are for inputs only. A rule output should be a concrete `NodeType`, not a
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

## Multi-output rules

A rule with multiple declared outputs runs its command **once**; all co-outputs are marked complete when the command finishes:

```python
@symlink_file
def raw_fastq(path: str):
    fastq = output(Fastq)
    return fastq
@command("bwa mem {ref} {fastq} > {bam} 2> {log}", threads=4)
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM, capturing the log."""
    bam = output(Bam)
    log = output(Log)
    return bam, log
dag = DAG("nodes")
P = Pipeline(dag)
P.fastq = raw_fastq(P, path=config.path)
P.bam, P.log = align(P, P.fastq, ref="hg38")
P.finish()
dag.require(P.sinks())
```

[Previous: Config Validation](config-validation.md) | [README](../README.md) | [Next: Rule-Call Lifecycle](rule-call-lifecycle.md)
