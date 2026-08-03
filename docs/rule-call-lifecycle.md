# What Happens When a Rule Is Called in a Pipeline Factory

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)

A factory compiles one configured view of a shared DAG. Rule calls calculate
identity, paths, and canonicalize equivalent computations immediately. Command
realization and filesystem materialization remain deferred until execution.

## Flow at a glance

1. A root `Pipeline` and all of its prefixed subpipeline views compile into one
   shared `DAG`.
2. Each rule call validates its inputs, computes both hashes and its final paths,
   and is immediately interned with any equivalent call already in that DAG.
3. Assigning the returned Nodes records request/result labels. A subpipeline adds
   its prefix to those labels only; it does not change computation identity.
4. For each root Pipeline, `P.finish()` closes construction and the caller adds
   that Pipeline's selected outputs to the shared DAG, commonly with
   `dag.require(P.sinks())`. Requirements from all configured Pipelines
   accumulate.
5. After every Pipeline has contributed its requirements, execution classifies
   the combined required subgraphs. Only missing or stale calls have their
   commands realized and materialized; up-to-date calls remain cached.

The running example compiles two root Pipelines into one DAG. The first is a
cohort with shared reference data, one subpipeline per sample, and a nested
quality-control subpipeline. The second is the simple sorting Pipeline:

```python
from necroflow import DAG, Pipeline

def qc_pipeline(Q: Pipeline, bam) -> None:
    Q.metrics = collect_metrics(Q, bam)
    Q.report = render_qc(Q, Q.metrics)


def sample_pipeline(S: Pipeline, reference, annotation, sample: dict) -> None:
    S.fastq = raw_fastq(S, path=sample["reads"])
    S.bam, S.align_log = align(S, S.fastq, reference)
    qc_pipeline(S.subpipeline("qc"), S.bam)
    S.counts = count_reads(S, S.bam, annotation)


def cohort_pipeline(P: Pipeline, config: dict) -> None:
    P.reference = prepare_reference(P, path=config["reference"])
    P.annotation = prepare_annotation(P, path=config["annotation"])

    for sample in config["samples"]:
        sample_pipeline(
            P.subpipeline(f"samples/{sample['name']}"),
            P.reference,
            P.annotation,
            sample,
        )


def sorting_pipeline(P: Pipeline, config: dict) -> None:
    P.source = source_text(P, path=config["input"])
    P.sorted = sort_text(P, P.source, reverse=config.get("reverse", False))


dag = DAG("nodes")

# One root Pipeline compiles the cohort.
P = Pipeline(dag)
cohort_pipeline(P, cohort_config)
P.finish()
dag.require(P.sinks())

# Another root Pipeline compiles an independent request namespace into the
# same canonical DAG.
T = Pipeline(dag)
sorting_pipeline(T, sorting_config)
T.finish()
dag.require(T.sinks())

# Requirements from both Pipelines execute together.
dag.execute()
```

For sample `A`, this creates labels such as `samples/A/bam`,
`samples/A/counts`, and `samples/A/qc/report`. The walkthrough below focuses on
that sample's multi-output `align(...)` call. `P` denotes the root Pipeline,
`S` its `samples/A` view, and `Q` the nested `samples/A/qc` view. `T` is
the independent sorting Pipeline. Both roots share `dag`, so their requirements
accumulate and equivalent rule calls would still be interned together.

## 1. The Pipeline identifies the shared DAG

The CLI creates one DAG for the node store, then one Pipeline per expanded job
configuration:

```python
dag = DAG(nodes_dir)
P = Pipeline(
    dag,
    shellpath=selected_shell,
)
factory(P, config)
P.finish()
```

The DAG owns canonical rule calls, output Nodes, required outputs, and
execution. One root Pipeline owns the finished state and qualified labels for
one factory evaluation. `P.subpipeline(prefix)` creates a lightweight view over
that same state; the view shares the DAG, shell policy, nodes, and labels while
qualifying its attribute/item assignments. Every root Pipeline participating
in the same run references the same DAG.

## 2. The rule receives its compiling Pipeline

For:

```python
S.bam, S.align_log = align(S, S.fastq, reference)
```

`Rule.__call__` receives:

```python
pipeline = S
args = (S.fastq, reference)
kwargs = {}
```

The Pipeline is positional-only and must be first. `Rule.__call__` first checks
that Pipeline construction remains open, so calls after `finish()` fail before
fingerprinting or interning. A view uses the root's construction state, DAG, and
shell policy. Every Node input must belong to `S.dag`. A
canonical Node can be used from another Pipeline sharing that DAG, but a Node
from a different DAG is rejected. This is why reusable subpipeline factories
receive external Nodes such as `reference` explicitly.

## 3. Parent Nodes already have canonical addresses

`S.fastq` and `reference` are instantiated canonical Nodes. For example,
`S.fastq` already has:

```python
S.fastq.rule_hash       # 64 lowercase hexadecimal characters
S.fastq.provenance_hash # 64 lowercase hexadecimal characters
S.fastq.relative_path   # Path("raw_fastq/<rule_hash>/<provenance_hash>/reads.fastq.gz")
S.fastq.path            # S.dag.nodes_dir / S.fastq.relative_path
```

Their output files may not exist yet. A known address and a materialized
artifact are separate facts. External values such as `sample["reads"]` are
ordinary configuration values on the `raw_fastq` call; by the time `align` is
compiled, the resulting FASTQ is a parent Node.

## 4. Inputs and configuration are validated

The rule validates positional Node inputs against declared NodeTypes and
config values against their declared Python types. Decorated command rules
derive scalar/config defaults from their Python signature; explicit Rule and
factory construction use ``input_defaults``. Rule construction rejects unknown
defaults, wrongly typed defaults, and defaults on fixed or variadic Node inputs.
Every decorated-rule parameter must have a type annotation; an unannotated
parameter fails while the decorator constructs the Rule, even when no command
placeholder references it. Return annotations remain optional and do not
declare outputs.

At call time, explicit keyword values overlay a fresh copy of the defaults.
This effective config is then used for presence/type validation and output
compilation. Keywords outside the declared scalar/config inputs fail before
fingerprinting or DAG interning. The caller's ``kwargs`` mapping is not mutated.

`Rule.__call__` coordinates the phases through focused methods:

```python
self._validate_pipeline(pipeline)
config = self._effective_config(kwargs)
self._validate_input_presence(args, config)
self._validate_parent_nodes(pipeline, args)
self._validate_config_values(config)
nodes = self._compile_outputs(pipeline, args, config)
return self._shape_outputs(nodes)
```

The validated values retain their named logical shape. Config contains every
effective default as a concrete value. A fixed input stores one Node, while a
variadic input stores one ordered tuple of Nodes:

```python
node_inputs = {
    name: value
    for (name, _contract), value in zip(self._pos_inputs, args)
}
```

`RuleCall.parents` flattens those values only for graph traversal, preserving
declaration order and each tuple’s element order. Each parent has already copied
its concrete output type's inherited `mutable` boolean when it was compiled;
mutability is not supplied per Rule call or per input annotation.

## 5. A candidate RuleCall receives split v3 identity

One candidate `RuleCall` represents the invocation and all of its co-outputs:

```python
call = RuleCall(
    dag=S.dag,
    rule=align,
    inputs=NamedValues({"fastq": S.fastq, "reference": reference}),
    config={},
    command=align.command,
    shellpath=S.shellpath,
)
```

Necroflow first hashes the configuration-independent local recipe into
`rule_hash`. That payload contains the rule name, command or built-in recipe
identity, declared input types, and each output's type, filename, and mutability.
`declared_rule_hash(rule)` can therefore compute it without a job config or DAG.

It then hashes one configured invocation into `provenance_hash` from the local
`rule_hash`, effective config, selected shell, and ordered parent identities.
The framework-owned hasher reads those values directly from the freshly built
`RuleCall`, before output paths are resolved. Constraints and `repeat` are not
identity inputs and are never passed through an intermediate fingerprint view.

Only effective config is hashed; default declaration metadata is not a second
identity input. Consequently, omitting a default and passing that same value
explicitly produce the same provenance hash. Changing a default changes the
provenance hash for calls that omit it, while calls with an explicit override
retain the hash associated with that explicit value.

Parent Nodes contribute rule hash, provenance hash, output name, and mutable
marker. A variadic input remains one named tuple, so group boundaries and order
remain part of provenance. Static commands contribute their strings to the
local recipe. Supported Python callbacks contribute canonical AST plus Python
implementation/version identity. Both hashes are exactly 64 lowercase
hexadecimal characters. The policy is framework-owned; there is no custom
fingerprint provider.

## 6. Relative and absolute paths are derived

The rule-call identity and work directory are:

```python
call.relative_path = Path(rule.__name__) / rule_hash / provenance_hash
call.workdir = S.dag.nodes_dir / call.relative_path
```

Each output receives its declared `NodeType.filename`. Rule construction has
already rejected output NodeTypes whose `filename` is `None`; filename-less
NodeTypes remain valid as input contracts, but there is no output-name fallback:

```python
node.relative_path = call.relative_path / output_filename
node.path = S.dag.nodes_dir / node.relative_path
node.mutable = output_type.mutable
```

For a multi-output call:

```text
align/<64-hex-rule-hash>/<64-hex-provenance-hash>/aligned.bam
align/<64-hex-rule-hash>/<64-hex-provenance-hash>/align.log
```

Rule names and output filenames must each be one safe relative path component.
The actual filesystem's component and total path limits are checked before the
rule returns.

## 7. The DAG interns the RuleCall immediately

The DAG is a dictionary-backed canonical registry:

```python
dag.calls: dict[Path, RuleCall]
```

The lookup key is `call.relative_path`, which contains the rule name and both
full hashes.

If no call exists, the DAG registers the candidate and all outputs atomically.
If the key already exists, the DAG returns the existing RuleCall and its
existing Node objects. Conflicting output declarations for one call path are a
split-hash collision and raise an error.

Consequently, equivalent calls through differently prefixed views—or through
different root Pipelines sharing a DAG—return identical objects during factory
evaluation:

```python
first = P.subpipeline("aliases/first")
second = P.subpipeline("aliases/second")
first.fastq = raw_fastq(first, path="shared.fastq.gz")
second.fastq = raw_fastq(second, path="shared.fastq.gz")

assert first.fastq is second.fastq
```

Both framework hashes are computed for each candidate because they are needed
for lookup. The two labels differ, but the prefixes never enter either hash.
The command callback does not run during lookup.

## 8. The rule returns canonical Node values

A single-output rule returns one Node. A multi-output rule returns its declared
named-tuple shape. Co-outputs share the canonical RuleCall, both hashes,
workdir, realized command, and execution.

At return time:

```python
node.rule_call
node.rule_hash
node.provenance_hash
node.relative_path
node.path
```

are final.

## 9. Assignment creates Pipeline-local labels

Attribute and item assignment share one namespace:

```python
S.bam = node
assert S.bam is S["bam"]
assert S.bam is P["samples/A/bam"]
```

Assignment records a qualified label in the root Pipeline. Labels are not
stored on the canonical Node because one Node can have different labels in
different Pipelines.

Several labels in one Pipeline may alias the same canonical Node:

```python
P["featured_qc"] = P["samples/A/qc/report"]
P["exports/qc"] = P["samples/A/qc/report"]

assert P["featured_qc"] is P["exports/qc"]
assert P.labels_for(P["featured_qc"]) == (
    "samples/A/qc/report",
    "featured_qc",
    "exports/qc",
)
```

The Pipeline's `nodes` list contains that Node once. Labels cannot be
overwritten, cannot match a name declared in `necroflow.keywords.RESERVED`, and
must refer to Nodes in the same DAG. Item labels may be canonical relative
POSIX paths:

```python
cohort_summary = combine_counts(P, sample_counts)
P["exports/cohort"] = cohort_summary
assert P["exports/cohort"] is cohort_summary
```

Each component must be non-empty, non-dot-prefixed, and neither `.` nor `..`;
absolute paths and repeated or trailing separators are rejected. Encoded
components are limited to Linux `NAME_MAX` (255 bytes), and the relative label
plus output filename to Linux `PATH_MAX` (4096 bytes). Assignment also rejects
result paths where one output would have to be both a file and a directory.
These checks happen at assignment; labels remain outside both Node hashes.

Subpipeline views apply the same operation after qualifying the local name:

```python
S = P.subpipeline("samples/A")
Q = S.subpipeline("qc")
Q.report = render_qc(Q, Q.metrics)

assert Q.report is S["qc/report"]
assert Q.report is P["samples/A/qc/report"]
assert Q.labels == P.labels
assert Q.nodes == P.nodes
```

Nested subpipelines compose their canonical relative POSIX prefixes. Prefixes
are request/result presentation only and never affect rule or provenance
hashes, so equivalent calls through different views still intern to one Node.
External input Nodes are passed explicitly to reusable subpipeline factories.

## 10. Finishing freezes construction and enables request selection

The factory mutates its Pipeline and returns `None`. The CLI then calls
`P.finish()`, which freezes the root and every subpipeline view. Later rule
calls, label assignments, and subpipeline creation raise `RuntimeError`.
`finish()` is idempotent on the root; calling it through a view is rejected so
a nested factory cannot freeze its caller unexpectedly. The DAG remains open,
allowing other root Pipelines to compile into the same canonical registry.

After finishing, the caller resolves explicit labels or uses the Pipeline's
sinks and marks those canonical outputs required:

```python
factory(P, config)
P.finish()
dag.require(P.sinks())
```

Requirements from multiple Pipelines accumulate. Registration and requirement
selection are separate: interning happens during rule calls; `require()`
controls which canonical subgraphs execute.

## 11. Classification decides whether work is needed

Only a DAG can be executed. The executor walks every required Node and its
canonical ancestors, classifying outputs as missing, stale, up to date, or
orphan. An up-to-date call is skipped without realizing its callable command.

V2 paths are not probed or migrated. Split v3 paths form a new cache namespace.

## 12. Commands are realized only for submitted work

Immediately before running a missing or stale canonical call,
`resolve_command(node)` creates immutable `CommandArgs`:

```python
CommandArgs(
    inputs={"fastq": S.fastq.path, "reference": reference.path},
    config={},
    outputs={"bam": S.bam.path, "log": S.align_log.path},
    constraints={"threads": 4},
    workdir=node.rule_call.workdir,
)
```

A callable returns one complete shell string. The result is cached on the
canonical RuleCall, so co-outputs and duplicate factory calls realize it once.
Static command templates use the same resolved values. For a variadic input,
`CommandArgs.inputs` contains an ordered tuple of resolved Paths; a static
`{name}` placeholder shell-quotes each path independently and joins them with
one space.

## 13. Execution materializes the canonical call

The executor creates the workdir, runs a built-in materializer or resolved
shell command, and verifies every declared co-output exists. The immutable
`RuleCall.shellpath` supplies the selected shell executable. Success writes
state, dependency hashes, invalidator tokens, provenance, and run statistics
under the rule-call's `.rip/` directory.

Classification treats every parent as an ordering and failure dependency. For a
mutable parent only the newer-mtime/content-hash comparison is skipped. Missing,
stale, compromised, forced, and invalidator-changed state still propagates to
consumers. Autoclean refuses to remove mutable paths or any shared rule-call
directory containing one.

Execution returns a plain dict mapping each cached or attempted Node's
`relative_path.as_posix()` key to its `ExecutionEvent`. `DAG.execute()` stores
and returns that same dict; a keep-going `ExceptionGroup` carries it as
`execution_report`. Nodes blocked by failed dependencies have no event because
they were neither cache hits nor attempted.

Identity, paths, and deduplication are eager. Command realization and
materialization are lazy.

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)
