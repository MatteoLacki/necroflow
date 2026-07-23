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
    fingerprint_function=selected_fingerprint,
    fingerprint_provider=provider_name,
    shellpath=selected_shell,
)
factory(P, config)
```

The DAG owns canonical rule calls, output Nodes, required outputs, and
execution. The Pipeline owns the labels and presentation sections for one
factory evaluation. Every Pipeline passed to factories participating in the
same run references the same DAG.

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

The Pipeline is positional-only and must be first. Every Node input must belong
to `P.dag`. A canonical Node can be used from another Pipeline sharing that
DAG, but a Node from a different DAG is rejected.

## 3. Parent Nodes already have canonical addresses

`P.source` is an instantiated canonical Node. It already has:

```python
P.source.fingerprint    # 64 lowercase hexadecimal characters
P.source.relative_path # Path("source_text/<fingerprint>/input.txt")
P.source.path          # P.dag.nodes_dir / P.source.relative_path
```

Its output file may not exist yet. A known address and a materialized artifact
are separate facts. External inputs such as `config["input"]` remain ordinary
configuration values.

## 4. Inputs and configuration are validated

The rule validates positional Node inputs against declared NodeTypes and
keyword values against their declared Python types. It rejects missing, extra,
misordered, or cross-DAG inputs before creating outputs.

`Rule.__call__` coordinates the phases through focused methods:

```python
self._validate_pipeline(pipeline)
self._validate_input_presence(args, kwargs)
self._validate_parent_nodes(pipeline, args)
self._validate_config_values(kwargs)
nodes = self._compile_outputs(pipeline, args, kwargs)
return self._shape_outputs(nodes)
```

The logical parents are the validated Node arguments:

```python
parents = [arg for arg in args if isinstance(arg, Node)]
```

## 5. A candidate RuleCall is fingerprinted

One candidate `RuleCall` represents the invocation and all of its co-outputs:

```python
call = RuleCall(
    dag=P.dag,
    rule=sort_text,
    parents=[P.source],
    config={"reverse": False},
    command=sort_text.command,
    shellpath=P.shellpath,
    fingerprint_provider=P.fingerprint_provider,
)
```

The Pipeline fingerprint function receives logical `FingerprintArgs`:

```python
FingerprintArgs(
    rule_name="sort_text",
    command=sort_text.command,
    inputs={"source": P.source},
    config={"reverse": False},
    input_types={"source": SourceText},
    output_types={"sorted": SortedText},
    constraints={"threads": 1},
    shellpath=P.shellpath,
    repeat=sort_text.repeat,
    recipe_identity=sort_text.recipe_identity,
)
```

Parent Nodes contribute their full fingerprints. Static commands contribute
their strings. Supported Python callbacks contribute canonical AST plus Python
implementation/version identity. A project fingerprint can replace or extend
the default policy.

The result must be exactly 64 lowercase hexadecimal characters.

## 6. Relative and absolute paths are derived

The rule-call identity and work directory are:

```python
call.relative_path = Path(rule.__name__) / fingerprint
call.workdir = P.dag.nodes_dir / call.relative_path
```

Each output receives a unique declared filename:

```python
node.relative_path = call.relative_path / output_filename
node.path = P.dag.nodes_dir / node.relative_path
```

For a multi-output call:

```text
run_sage/<64-hex-fingerprint>/results.json
run_sage/<64-hex-fingerprint>/results.tsv
```

Rule names and output filenames must each be one safe relative path component.
The actual filesystem's component and total path limits are checked before the
rule returns.

## 7. The DAG interns the RuleCall immediately

The DAG is a dictionary-backed canonical registry:

```python
dag.calls: dict[Path, RuleCall]
```

The lookup key is `call.relative_path`, which contains the rule name and full
fingerprint.

If no call exists, the DAG registers the candidate and all outputs atomically.
If the key already exists, the DAG returns the existing RuleCall and its
existing Node objects. Conflicting output declarations for one call path are a
fingerprint collision and raise an error.

Consequently, equivalent calls in Pipelines sharing a DAG return identical
objects during factory evaluation:

```python
P1.source = source_text(P1, path="input.txt")
P2.source = source_text(P2, path="input.txt")

assert P1.source is P2.source
```

The fingerprint callback runs for each candidate because its result is needed
for lookup. The command callback does not run during lookup.

## 8. The rule returns canonical Node values

A single-output rule returns one Node. A multi-output rule returns its declared
named-tuple shape. Co-outputs share the canonical RuleCall, fingerprint,
workdir, realized command, and execution.

At return time:

```python
node.rule_call
node.fingerprint
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

Assignment records a label and the active presentation section in the
Pipeline. Labels are not stored on the canonical Node because one Node can have
different labels in different Pipelines.

Several labels in one Pipeline may alias the same canonical Node:

```python
P.primary = make_result(P, value="same")
P.alias = make_result(P, value="same")

assert P.primary is P.alias
assert P.labels_for(P.primary) == ("primary", "alias")
```

The Pipeline's `nodes` list contains that Node once. Labels cannot be
overwritten, cannot start with `.`, must be one relative path component, and
must refer to Nodes in the same DAG.

## 10. The factory selects required outputs

The factory mutates its Pipeline and returns `None`. After it finishes, the
caller resolves explicit labels or uses the Pipeline's sinks and marks those
canonical outputs required:

```python
factory(P, config)
dag.require(P.sinks())
```

Requirements from multiple Pipelines accumulate. Registration and requirement
selection are separate: interning happens during rule calls; `require()`
controls which canonical subgraphs execute.

## 11. Classification decides whether work is needed

Only a DAG can be executed. The executor walks every required Node and its
canonical ancestors, classifying outputs as missing, stale, up to date, or
orphan. An up-to-date call is skipped without realizing its callable command.

The previous truncated-fingerprint layout is not probed or migrated. Full
fingerprint paths form a new cache namespace.

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
Static command templates use the same resolved values.

## 13. Execution materializes the canonical call

The executor creates the workdir, runs a built-in materializer or resolved
shell command, and verifies every declared co-output exists. The immutable
`RuleCall.shellpath` supplies the selected shell executable. Success writes
state, dependency hashes, invalidator tokens, provenance, and run statistics
under the rule-call's `.rip/` directory.

## Compact sequence

```text
create shared DAG
    ↓
create Pipeline(dag, fingerprint/shell policy)
    ↓
factory(P, config)
    ↓
rule(P, canonical parents..., config...)
    ↓
validate types and shared DAG ownership
    ↓
candidate RuleCall → FingerprintArgs → 64-hex fingerprint
    ↓
derive rule/fingerprint/output relative paths
    ↓
DAG dictionary lookup
    ├─ existing → return canonical RuleCall and Nodes
    └─ absent   → register call and all outputs atomically
    ↓
P.name = node or P["name"] = node records local labels
    ↓
dag.require(P.sinks() or explicitly selected labels)
    ↓
classify required canonical subgraphs
    ↓
missing/stale only: CommandArgs → realize command
    ↓
execute each canonical RuleCall once and verify every output
```

Identity, paths, and deduplication are eager. Command realization and
materialization are lazy.

[Previous: Rules and Typed Outputs](rules.md) | [README](../README.md) | [Next: Generated Config Files](generated-config-files.md)
