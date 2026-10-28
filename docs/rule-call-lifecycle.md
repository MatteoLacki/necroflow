# What Happens When a Command Is Called in a Workflow

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)

A workflow compiles one configured view of a shared DAG. Command calls calculate
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
from necroflow import DAG, Pipeline, workflow

@workflow
def qc_pipeline(Q: Pipeline, bam) -> None:
    Q.metrics = collect_metrics(bam)
    Q.report = render_qc(Q.metrics)


@workflow
def sample_pipeline(S: Pipeline, reference, annotation, sample: dict) -> None:
    S.fastq = raw_fastq(path=sample["reads"])
    S.bam, S.align_log = align(S.fastq, reference)
    qc_pipeline(S.subpipeline("qc"), S.bam)
    S.counts = count_reads(S.bam, annotation)


@workflow
def cohort_pipeline(P: Pipeline, config: dict) -> None:
    P.reference = prepare_reference(path=config["reference"])
    P.annotation = prepare_annotation(path=config["annotation"])

    for sample in config["samples"]:
        sample_pipeline(
            P.subpipeline(f"samples/{sample['name']}"),
            P.reference,
            P.annotation,
            sample,
        )


@workflow
def sorting_pipeline(P: Pipeline, config: dict) -> None:
    P.source = source_text(path=config["input"])
    P.sorted = sort_text(P.source, reverse=config.get("reverse", False))


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
dag.run()
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
build_workflow(P, config)
P.finish()
```

Before compiling each configuration, the CLI checks that its job name has not
already appeared in this invocation. Duplicate names from different job files or
grid combinations fail before execution, with both source paths and configuration
numbers in the error.

The DAG owns canonical rule calls, output Nodes, required outputs, and
execution. One root Pipeline owns the finished state and qualified labels for
one workflow evaluation. `P.subpipeline(prefix)` creates a lightweight view over
that same state; the view shares the DAG, shell policy, nodes, and labels while
qualifying its attribute/item assignments. Every root Pipeline participating
in the same run references the same DAG.

## 2. The workflow supplies the compiling Pipeline

`@workflow` validates its first positional argument as an open `Pipeline`, sets a
private `ContextVar`, and runs the original function. It restores the previous
context in `finally`, including after exceptions. A decorated subworkflow receiving
`P.subpipeline(prefix)` temporarily activates that view. Ordinary helpers inherit
the caller's context. No source analysis or local-variable capture is involved.

Inside `sample_pipeline`:

```python
S.bam, S.align_log = align(S.fastq, reference)
```

`Command.__call__` resolves the active owner and logical arguments as:

```python
pipeline = S  # retrieved from the active workflow context
args = (S.fastq, reference)
kwargs = {}
```

Explicit `align(S, S.fastq, reference)` remains valid inside or outside a workflow.
A leading Pipeline takes precedence for that call without changing the context.
With neither an explicit owner nor an active context, the rule raises a
`RuntimeError` explaining that it requires `@workflow` or an explicit Pipeline.

A missing or incorrectly typed first workflow argument raises `TypeError` naming
the workflow and requiring an open Pipeline. A finished owner raises `RuntimeError`
with the same requirement. Coroutine, generator, and async-generator functions are
rejected at decoration with `TypeError` explaining their deferred execution.

Every rule call checks that its selected Pipeline remains open before fingerprinting
or interning. A view uses its root's construction state, DAG, and shell policy.
Every Node input must belong to `S.dag`. Nodes from another Pipeline sharing that
DAG are accepted; Nodes from a different DAG are rejected. Reusable subworkflows
therefore receive external Nodes such as `reference` explicitly.

## 3. Parent Nodes already have canonical addresses

`S.fastq` and `reference` are instantiated canonical Nodes. For example,
`S.fastq` already has:

```python
S.fastq.rule_hash       # 64 lowercase hexadecimal characters
S.fastq.provenance_hash # 64 lowercase hexadecimal characters
S.fastq.relative_path   # Path("raw_fastq/<provenance_hash>/reads.fastq.gz")
S.fastq.path            # S.dag.nodes_dir / S.fastq.relative_path
```

Their output files may not exist yet. A known address and a materialized
artifact are separate facts. External values such as `sample["reads"]` are
ordinary configuration values on the `raw_fastq` call; by the time `align` is
compiled, the resulting FASTQ is a parent Node.

## 4. Inputs and configuration are validated

The rule validates Node inputs (positional or by name) against declared
NodeTypes and config values against their declared Python types. A fixed mixed
union works the same way: a Node selects its NodeType arm, while any other
value selects a matching non-Node arm. Decorated command rules derive scalar/config and mixed
value defaults from their Python signature; explicit Command and factory
construction use ``input_defaults``. Command construction rejects unknown or
wrongly typed defaults, defaults on pure fixed or variadic Node inputs, managed
Node defaults, and non-trailing positional defaults.
Every decorated-rule parameter must have a type annotation; an unannotated
parameter fails while the decorator constructs the Command, even when no command
placeholder references it. Return annotations remain optional and do not
declare outputs.

At call time, `Command.__call__` binds `args`/`kwargs` against an `inspect.Signature`
built once per Command from its declared schema: Node, variadic, and mixed inputs
are `POSITIONAL_OR_KEYWORD`, and plain config inputs stay `KEYWORD_ONLY`. Every
input may therefore be supplied positionally or by name — `consume(value)`
and `consume(source=value)` inside a workflow bind identically — while `bind()`/`apply_defaults()`
supply presence, duplicate, and defaulting checks with ordinary Python calling
semantics. The bound values are then read back out in the Command's fixed
declaration order (not the order the caller wrote them), so call syntax never
affects fingerprinting: a keyword call and the equivalent positional call
produce byte-identical `NamedValues`/provenance input. Keywords outside the
declared schema fail before fingerprinting or DAG interning. The caller's
``args`` and ``kwargs`` are not mutated.

`Command.__call__` coordinates the phases through focused methods:

```python
# After resolving the explicit or scoped Pipeline:
pipeline._assert_open()
bound = self._bind_call(args, kwargs)
args = tuple(bound[name] for name, _contract in self._pos_inputs)
config = {name: bound[name] for name in self._kw_inputs}
self._validate_positional_inputs(pipeline, args)
self._validate_config_values(config)
nodes = self._compile_outputs(pipeline, args, config)
return self._shape_outputs(nodes)
```

The validated values retain their named logical shape. Config contains every
effective config default as a concrete value. A fixed pure Node input stores one
Node, while a variadic input stores one ordered tuple of Nodes. A mixed input is
partitioned by its runtime branch: Nodes enter `node_inputs`; plain values enter
`input_values` under the same declared name:

```python
node_inputs = {
    name: value
    for (name, contract), value in zip(self._pos_inputs, args)
    if contract.variadic or isinstance(value, Node)
}
input_values = {
    name: value
    for (name, contract), value in zip(self._pos_inputs, args)
    if not contract.variadic and not isinstance(value, Node)
}
```

`RuleCall.parents` flattens those values only for graph traversal, preserving
declaration order and each tuple’s element order. Mixed plain values never enter
`parents` and therefore create no DAG edge. Command resolution reunites both
mappings in declaration order: Nodes become paths, while plain values remain
unchanged under `CommandArgs.inputs`.

## 5. A candidate RuleCall receives v4 recipe and provenance identity

One candidate `RuleCall` represents the invocation and all of its co-outputs:

```python
call = RuleCall(
    dag=S.dag,
    rule=align,
    inputs=NamedValues({"fastq": S.fastq, "reference": reference}),
    input_values=NamedValues(),
    config={},
    command=align.command,
    shellpath=S.shellpath,
)
```

`RuleCall.__post_init__` computes both hashes and the relative path immediately,
before the constructor call above returns; there is no separate compilation step
and no window where they are unset. It first hashes the configuration-independent
local recipe into `rule_hash`. That payload contains the rule name, command or
built-in recipe identity, declared input types, and each output's type, filename,
and mutability. `declared_rule_hash(rule)` can therefore compute it without a job
config or DAG.

It then hashes one configured invocation into `provenance_hash` from the local
`rule_hash`, effective config, selected shell, ordered parent identities, and
any named mixed plain values. Container-capable rules (`Command.container_capable`:
a `{name}:` template prefix naming a `Docker` input, or a callback with a `Docker`
input) also add `container_policy` to the execution context; `Docker` values
themselves are ordinary config.
The framework-owned hasher reads those values directly from the freshly built
`RuleCall`, before output paths are resolved. Constraints and `repeat` are not
identity inputs and are never passed through an intermediate fingerprint view.

Only effective config is hashed; default declaration metadata is not a second
identity input. Consequently, omitting a default and passing that same value
explicitly produce the same provenance hash. Changing a default changes the
provenance hash for calls that omit it, while calls with an explicit override
retain the hash associated with that explicit value.

The same effective-value rule applies to mixed positional defaults. A Node arm
contributes its parent provenance and output name. A non-Node arm contributes a
conditionally present `input_values` mapping encoded by the canonical v4 value
encoder plus the callback-visible positional input order. Existing calls without
mixed plain values retain their previous v4 identity payload. Persisted dependency
metadata records mixed plain values as named diagnostic type/value representations;
those representations are not the identity encoding.

Parent Nodes contribute provenance hash and output name. Parent recipe identity is
already contained by that provenance hash. A variadic input remains one named
tuple, so group boundaries and order remain part of provenance. Static commands contribute their strings to the
local recipe. Supported Python callbacks contribute canonical AST plus Python
implementation/version identity. Both hashes are exactly 64 lowercase
hexadecimal characters. The policy is framework-owned; there is no custom
fingerprint provider.

## 6. Relative and absolute paths are derived

`__post_init__` derives the rule-call identity and work directory right after
the hashes above:

```python
call.relative_path = Path(rule.__name__) / provenance_hash
call.workdir = S.dag.nodes_dir / call.relative_path
```

Each output receives its declared `NodeType.filename`. Command construction has
already rejected output NodeTypes whose `filename` is `None`; filename-less
NodeTypes remain valid as input contracts, but there is no output-name fallback:

```python
node.relative_path = call.relative_path / output_filename
node.path = S.dag.nodes_dir / node.relative_path
```

`Node` stores only `output_name`, `node_type`, `relative_path`, `path`, `rule_call`, `state`,
and `info`. `config`, `rule`, `command`, `parents`, and `output_nodes` are not
copied onto the Node — they are properties that read through `node.rule_call`.
One `RuleCall` per invocation stays the single place those values live.

For a multi-output call:

```text
align/<64-hex-provenance-hash>/aligned.bam
align/<64-hex-provenance-hash>/align.log
```

Command names and output filenames must each be one safe relative path component.
The actual filesystem's component and total path limits are checked before the
rule returns.

Co-outputs must resolve to distinct paths within their shared hashed workdir.
Duplicate paths raise `ValueError` during the rule call, before DAG interning or
filesystem mutation. The error identifies the rule, both output names and
NodeTypes, and the conflicting filename. Change one output's `NodeType.filename`
to fix it; if both outputs use the same type, declare a separate subclass with a
different filename. Renaming an output variable alone does not change its path.
Inputs retain their parent calls' paths, so matching basenames across distinct
workdirs are valid; inputs are not automatically copied into the consumer's workdir.

## 7. The DAG interns the RuleCall immediately

The DAG is a dictionary-backed canonical registry:

```python
dag.calls: dict[Path, RuleCall]
```

The lookup key is `call.relative_path`, which contains the rule name and full
provenance hash.

If no call exists, the DAG registers the candidate and all outputs atomically.
If the key already exists, the DAG returns the existing RuleCall and its
existing Node objects. Conflicting output declarations for one call path are a
provenance-hash collision and raise an error.

Consequently, equivalent calls through differently prefixed views—or through
different root Pipelines sharing a DAG—return identical objects during workflow
evaluation:

```python
first = P.subpipeline("aliases/first")
second = P.subpipeline("aliases/second")
first.fastq = raw_fastq(first, path="shared.fastq.gz")
second.fastq = raw_fastq(second, path="shared.fastq.gz")

assert first.fastq is second.fastq
```

Both framework hashes are computed for each candidate: rule hash feeds the
provenance hash, which supplies the lookup key. The two labels differ, but the
prefixes never enter either hash.
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

Nodes support `set(nodes)` and dictionary keys. Equality and Python hashing use
`node.relative_path`: rule name, provenance hash, and output filename. Equivalent
outputs compare equal across DAGs and node-store roots; co-outputs remain distinct
despite sharing a provenance hash. Labels, descriptions, and runtime state do not
affect equality or hashing. Address fields must not be reassigned after creation.

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
overwritten and must refer to Nodes in the same DAG. Item labels may be
canonical relative POSIX paths:

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
External input Nodes are passed explicitly to reusable subworkflows.

## 10. Finishing freezes construction and enables request selection

The workflow mutates its Pipeline and returns `None`. The CLI then calls
`P.finish()`, which freezes the root and every subpipeline view. Later rule
calls, label assignments, and subpipeline creation raise `RuntimeError`.
`finish()` is idempotent on the root; calling it through a view is rejected so
a nested workflow cannot freeze its caller unexpectedly. The DAG remains open,
allowing other root Pipelines to compile into the same canonical registry.

After finishing, the caller resolves explicit labels or uses the Pipeline's
sinks and marks those canonical outputs required:

```python
build_workflow(P, config)
P.finish()
dag.require(P.sinks())
```

Requirements from multiple Pipelines accumulate. Registration and requirement
selection are separate: interning happens during rule calls; `require()`
controls which canonical subgraphs execute.

## 11. Execution handoff

`P.finish()` freezes construction. Callers select labels or sinks, then record endpoint Nodes with `dag.require()`. Requirements from all root Pipelines sharing the DAG accumulate.

Planning begins inside `dag.run()` under the node-store lock. It converts requested Nodes to owning RuleCalls, follows `parent_calls`, and preserves insertion order from `DAG.calls`. Requesting one co-output activates the complete RuleCall; only later CLI result copying remains Node-selective.

`planning.plan_execution()` partitions active and orphan RuleCalls, then walks the active calls once in registration order, which is topological. By default, rebuilding a call also marks every active descendant for execution, with reason `parent_will_run`. With `reuse_unchanged=True`, those descendants instead stay open until parents finish and their bytes can be checked. Verdicts and reasons live in the returned `ExecutionPlan`, never on the RuleCall. The reuse policy is passed by the executor and by CLI/MCP explain; it does not enter fingerprints.

## 12. Commands remain lazy

The executor's `Execution` object owns each active call's run `Phase`. A call whose parents are all done is settled: an up-to-date verdict makes it done at once, a missing or stale one makes it ready. Default FIFO ordering follows canonical RuleCall registration. Custom schedulers receive ready calls, remaining (waiting or ready) calls, and available resource capacity. Executor retains dependency gates, resource admission, submission, retries, and state transitions.

One submission runs one complete RuleCall. Before each attempt, `RuleCall.validate_dependencies()` checks provenance serialization, including nullable config encoding, before the executor clears the call's declared outputs (`RuleCall.clear_outputs()`), so a replay into an existing identity-addressed directory never finds a prior output already there; other files under `{workdir}` are left alone. The default runner is `runners.run_call(call, log_path)`; callable command realization happens during provenance validation and is cached on the canonical call. Realization also strips a leading `{name}:` Docker prefix; `RuleCall.container` then returns the selected `Docker` value and `run_call` launches it with `docker run`. `runners.attempt` wraps the runner with retries and timestamps and returns an `Outcome`. The runner must produce every declared output.

## 13. Materialization commits cache state

After provisional success, executor validates all co-outputs and writes dependency metadata, output hashes, invalidator tokens, ancestor graph, report, run stats, and finally call-level state. Each parent metadata entry stores the tagged content hash consumed by this call.

Children become eligible only after all parents settle successfully. Rebuilt parents force every active descendant to rerun by default, even if the bytes match. With `reuse_unchanged=True` (`--reuse-unchanged` on the CLI), deferred child classification instead preserves valid caches for identical rebuilt bytes. Autoclean considers parents for deletion after either execution or a cache hit, once all their consumers are done. A failed call blocks its descendants, which are never submitted.

CLI result copying selects requested outputs whose owning calls are marked
`up_to_date` in this invocation's report, including cache hits. During a
keep-going run, partial outputs from failed or interrupted calls and old outputs
from blocked calls are excluded from results and their manifest. Previously
managed copies of those excluded outputs are removed. Execution summaries still
include the failure events.

Identity, paths, interning, and RuleCall order are eager. Cache classification, command realization, materialization, and result copying happen after construction.

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)

## Inline command and resource compilation

`declarations.parse_declaration` separates ordinary inputs, Resource parameters,
outputs, and command definition before constructing a Command. Inline bodies contain
output declarations, one literal command or nested builder, and the output return.
The body never executes to discover outputs. Nested builders are source-backed
adapters that bind runtime paths/settings/resources at command realization.

Resource parameters are omitted from rule-hash input contracts and invocation
config/input values. Defaults are expanded into a separate call resource request;
DAG resource-file policy overrides it before `DAG.intern`. Neither policy contents
nor effective resource values enter identity. New Resource calls with conflicting
requests fail at interning; legacy-only constraint interning stays compatible.
The complete resource-file rule set is validated after all jobs compile and before
execution. Thread caps bind before inspection/planning; changing an all-threads
cap clears the realized command so the scheduler and command see the same value.

`Node` is generic in its declared NodeType. `output(T)` has static return type
`Node[T]`; decorator return inference preserves that type (and tuples of typed
Nodes). These typing parameters do not enter runtime recipe or provenance identity,
and return annotations still do not replace output declarations.
