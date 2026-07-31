# What Happens When a Rule Is Called in a Pipeline Factory

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)

A factory compiles one configured view of a shared DAG. Rule calls calculate
identity, paths, and canonicalize equivalent computations immediately. Command
realization and filesystem materialization remain deferred until execution.

The running example is:

```python
from necroflow import DAG, Pipeline

dag = DAG("nodes")
P = Pipeline(dag)

def sorting_pipeline(P: Pipeline, config: dict) -> None:
    P.source = source_text(P, path=config["input"])
    P.sorted = sort_text(P, P.source, reverse=config.get("reverse", False))

sorting_pipeline(P, config)
P.finish()
dag.require(P.sinks())
dag.execute()
```

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
P.sorted = sort_text(P, P.source, reverse=False)
```

`Rule.__call__` receives:

```python
pipeline = P
args = (P.source,)
kwargs = {"reverse": False}
```

The Pipeline is positional-only and must be first. `Rule.__call__` first checks
that Pipeline construction remains open, so calls after `finish()` fail before
fingerprinting or interning. Every Node input must belong to `P.dag`. A
canonical Node can be used from another Pipeline sharing that DAG, but a Node
from a different DAG is rejected.

## 3. Parent Nodes already have canonical addresses

`P.source` is an instantiated canonical Node. It already has:

```python
P.source.rule_hash       # 64 lowercase hexadecimal characters
P.source.provenance_hash # 64 lowercase hexadecimal characters
P.source.relative_path   # Path("source_text/<rule_hash>/<provenance_hash>/input.txt")
P.source.path          # P.dag.nodes_dir / P.source.relative_path
```

Its output file may not exist yet. A known address and a materialized artifact
are separate facts. External inputs such as `config["input"]` remain ordinary
configuration values.

## 4. Inputs and configuration are validated

The rule validates positional Node inputs against declared NodeTypes and
config values against their declared Python types. Decorated command rules
derive scalar/config defaults from their Python signature; explicit Rule and
factory construction use ``input_defaults``. Rule construction rejects unknown
defaults, wrongly typed defaults, and defaults on fixed or variadic Node inputs.

At call time, explicit keyword values overlay a fresh copy of the defaults.
This effective config is then used for presence/type validation and output
compilation. The caller's ``kwargs`` mapping is not mutated.

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
    dag=P.dag,
    rule=sort_text,
    inputs=NamedValues({"source": P.source}),
    config={"reverse": False},
    command=sort_text.command,
    shellpath=P.shellpath,
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
call.workdir = P.dag.nodes_dir / call.relative_path
```

Each output receives its declared `NodeType.filename`. Rule construction has
already rejected output NodeTypes whose `filename` is `None`; filename-less
NodeTypes remain valid as input contracts, but there is no output-name fallback:

```python
node.relative_path = call.relative_path / output_filename
node.path = P.dag.nodes_dir / node.relative_path
node.mutable = output_type.mutable
```

For a multi-output call:

```text
run_sage/<64-hex-rule-hash>/<64-hex-provenance-hash>/results.json
run_sage/<64-hex-rule-hash>/<64-hex-provenance-hash>/results.tsv
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

Consequently, equivalent calls in Pipelines sharing a DAG return identical
objects during factory evaluation:

```python
P1.source = source_text(P1, path="input.txt")
P2.source = source_text(P2, path="input.txt")

assert P1.source is P2.source
```

Both framework hashes are computed for each candidate because they are needed
for lookup. The command callback does not run during lookup.

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
P.sorted = node
assert P.sorted is P["sorted"]
```

Assignment records a qualified label in the root Pipeline. Labels are not
stored on the canonical Node because one Node can have different labels in
different Pipelines.

Several labels in one Pipeline may alias the same canonical Node:

```python
P.primary = make_result(P, value="same")
P.alias = make_result(P, value="same")

assert P.primary is P.alias
assert P.labels_for(P.primary) == ("primary", "alias")
```

The Pipeline's `nodes` list contains that Node once. Labels cannot be
overwritten, cannot match a name declared in `necroflow.keywords.RESERVED`, and
must refer to Nodes in the same DAG. Item labels may be canonical relative
POSIX paths:

```python
P["dataset/config"] = make_result(P, value="same")
assert P["dataset/config"] is node
```

Each component must be non-empty, non-dot-prefixed, and neither `.` nor `..`;
absolute paths and repeated or trailing separators are rejected. Encoded
components are limited to Linux `NAME_MAX` (255 bytes), and the relative label
plus output filename to Linux `PATH_MAX` (4096 bytes). Assignment also rejects
result paths where one output would have to be both a file and a directory.
These checks happen at assignment; labels remain outside both Node hashes.

Subpipeline views apply the same operation after qualifying the local name:

```python
sample = P.subpipeline("samples/A")
sample.result = make_result(sample, value="same")

assert sample.result is P["samples/A/result"]
assert sample.labels == P.labels
assert sample.nodes == P.nodes
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
    inputs={"source": P.source.path},
    config={"reverse": False},
    outputs={"sorted": node.path},
    constraints={"threads": 1},
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

## Compact sequence

```text
create shared DAG
    ↓
create Pipeline(dag, shell policy)
    ↓
factory(P, config)
    ├─ optional P.subpipeline(prefix) views share root state
    └─ rule calls reject a finished root before interning
    ↓
rule(P, fixed Nodes and/or Node tuples, config...)
    ↓
overlay explicit config on declared scalar defaults
    ↓
validate types and shared DAG ownership
    ↓
candidate RuleCall → 64-hex rule hash + 64-hex provenance hash
    ↓
derive rule/rule-hash/provenance-hash/output relative paths
    ↓
DAG dictionary lookup
    ├─ existing → return canonical RuleCall and Nodes
    └─ absent   → register call and all outputs atomically
    ↓
P.name = node or P["name"] = node records qualified root labels
    ↓
P.finish() freezes the root and every prefixed view
    ↓
dag.require(P.sinks() or explicitly selected labels)
    ↓
classify required canonical subgraphs
    ├─ ordinary parent content changed → stale consumer
    └─ mutable parent content changed → cached consumer
    ↓
missing/stale only: CommandArgs → realize command
    ↓
execute each canonical RuleCall once and verify every output
```

Identity, paths, and deduplication are eager. Command realization and
materialization are lazy.

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)
