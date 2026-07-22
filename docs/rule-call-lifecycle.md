# What Happens When a Rule Is Called in a Pipeline Factory

A factory builds a graph; it does not execute commands. Unlike the older
two-phase design, however, graph construction now computes every Node's logical
fingerprint and final absolute path immediately. Only callable-command
realization and subprocess execution remain deferred.

The running example is:

```python
from necroflow import Pipeline

def sorting_pipeline(P: Pipeline, config: dict) -> None:
    P.source = source_text(P, path=config["input"])
    P.sorted = sort_text(P, P.source, reverse=config.get("reverse", False))
```

The CLI creates `P`; the factory does not:

```python
P = Pipeline(
    nodes_dir,
    fingerprint_function=selected_fingerprint,
    fingerprint_provider=provider_name,
    shellpath=selected_shell,
)
factory(P, config)
```

This order is essential. The node-store root, fingerprint policy, provider
identity, and shell context are known before the first rule call, so no later
mutation or graph reindexing is necessary.

## 1. The rule receives its owner

For this call:

```python
P.sorted = sort_text(P, P.source, reverse=False)
```

`Rule.__call__` receives:

```python
pipeline = P
args = (P.source,)
kwargs = {"reverse": False}
```

The Pipeline is positional-only and must be first. Necroflow verifies that
every Node input was compiled by this exact Pipeline. Passing a Node from a
different Pipeline is rejected, even when both pipelines use the same node
store and the Nodes have equal keys.

## 2. Parent Nodes are already complete logical addresses

`P.source` is an instantiated Node, not a deferred reference. It already has:

```python
P.source.full_fingerprint  # 64 lowercase hexadecimal characters
P.source.fingerprint       # first 16 characters
P.source.path              # absolute Path under P.nodes_dir
P.source.key               # rule/hash16/filename
```

The parent output file may not exist yet. “Known path” and “materialized file”
are separate facts. External inputs such as `config["input"]` are ordinary
config values; they are not placeholder Nodes.

The logical parent list is therefore simply:

```python
parents = [arg for arg in args if isinstance(arg, Node)]
```

It contains real Node objects whose identities and paths are final, while the
filesystem artifacts at those paths may still be missing.

## 3. Inputs and config are validated

The rule checks positional Node inputs against declared NodeTypes and keyword
values against declared Python types. It also rejects missing, extra, or
misordered values. NodeType unions and subclass relationships are handled by
the declared input contract.

## 4. One RuleCall represents the invocation

Necroflow creates one `RuleCall` shared by every output of the invocation:

```python
call = RuleCall(
    pipeline=P,
    rule=sort_text,
    parents=[P.source],
    config={"reverse": False},
    command=sort_text.command,
    execution_context=P.execution_context,
    fingerprint_provider=P.fingerprint_provider,
)
```

`RuleCall` remains useful because co-outputs share one fingerprint, one
workdir, one realized callable command, and one execution.

## 5. FingerprintArgs are built from logical values

The Pipeline's fingerprint function receives one `FingerprintArgs`:

```python
FingerprintArgs(
    rule_name="sort_text",
    command=sort_text.command,
    inputs={"source": P.source},
    config={"reverse": False},
    input_types={"source": SourceText},
    output_types={"sorted": SortedText},
    constraints={"threads": 1},
    execution_context=P.execution_context,
    repeat=sort_text.repeat,
    recipe_identity=sort_text.recipe_identity,
)
```

This object contains logical Nodes, not a mutable `CommandArgs`. A project
fingerprint can inspect the original static command or callback, rule fields,
parent full fingerprints, config, types, constraints, and execution context.
It cannot rewrite command paths.

The default fingerprint uses framed canonical serialization. Static commands
contribute their string. Supported Python callbacks contribute canonical AST
plus Python implementation/version identity. A project fingerprint replaces
the default policy, although it may call `default_fingerprint(args)` and extend
that result.

The selected function must return exactly 64 lowercase hexadecimal characters.
Validation happens during the rule call.

## 6. Absolute output paths are derived immediately

After the full digest is known, the shared workdir is:

```python
workdir = P.nodes_dir / rule.__name__ / full_fingerprint[:16]
```

Each output Node receives:

```python
path = workdir / (NodeType.filename or output_name)
```

Path component and total path limits are checked at this point. A rule call
therefore either returns fully addressed Nodes or raises; it never returns a
Node whose path will be filled in later.

For a multi-output rule, all outputs share `call` and `workdir` but have
distinct filenames and keys:

```text
run_sage/0123456789abcdef/results.json
run_sage/0123456789abcdef/results.tsv
```

## 7. The rule returns Node values

A single-output rule returns one Node. A multi-output rule returns its declared
named-tuple shape. Direct `Node()` and `NodeType()` construction are not the
output API; managed Nodes come from calling Rules with a Pipeline.

At this moment these are all available:

```python
node.rule_call
node.full_fingerprint
node.fingerprint
node.key
node.path
```

No command callback has run merely because the Node was created.

## 8. Assignment labels the Node

Attribute assignment:

```python
P.sorted = node
```

and item assignment:

```python
P["sorted"] = node
```

use the same label namespace. Both set `node.pipeline_label`, append the Node
to the Pipeline, and record the active presentation section. Ordinary reads
work across forms: `P.sorted is P["sorted"]`. Item syntax also permits generated
labels that are not Python identifiers, and item-only labels that collide with
Pipeline API attributes.

Labels cannot be overwritten, cannot start with `.`, and must receive Nodes
owned by the same Pipeline.

## 9. The factory returns None

The factory mutates the supplied Pipeline and returns `None`. The CLI resolves
requested labels or defaults to pipeline sinks, then adds the Pipeline to a
`DAG` whose node-store root must match `P.nodes_dir`.

`DAG.add` canonicalizes equal node keys. If two pipelines contain equivalent
Nodes, the first added Node is retained as the canonical representative. No
callable command is evaluated during this deduplication.

## 10. Classification decides whether work is needed

Execution classifies the requested subgraph as missing, stale, up to date, or
orphan. An up-to-date canonical Node is skipped without realizing its command
callback. The same is true for a newly compiled Pipeline that points to an
existing valid cache entry.

## 11. Callable commands are realized only for submitted work

Immediately before running a missing or stale canonical invocation,
`resolve_command(node)` creates immutable `CommandArgs`:

```python
CommandArgs(
    inputs={"source": P.source.path},
    config={"reverse": False},
    outputs={"sorted": node.path},
    constraints={"threads": 1},
    workdir=node.path.parent,
)
```

Unlike `FingerprintArgs`, these are command-facing resolved paths. A callable
returns one complete shell string. The result is cached on the shared
`RuleCall`, so co-outputs realize it once. In a deduplicated DAG only the
canonical invocation is submitted, so duplicate callbacks are not called.

Static command templates use the same values for placeholder substitution.

## 12. The executor materializes the invocation

The executor creates the workdir, runs a built-in materializer or the resolved
shell string, and verifies every declared co-output exists. The Pipeline's
shellpath, if any, is passed as `subprocess.run(..., executable=shellpath)`.
Success writes state, dependency hashes, invalidator tokens, provenance, and
run statistics under `.rip/`.

## Compact sequence

```text
CLI constructs Pipeline with nodes_dir/fingerprint/shell context
    ↓
factory(P, config)
    ↓
rule(P, parent_nodes..., config...)
    ↓
validate owner and declared inputs
    ↓
create shared RuleCall
    ↓
FingerprintArgs → full digest
    ↓
derive and validate absolute output paths
    ↓
return fully addressed Node(s)
    ↓
P.name = node or P["name"] = node
    ↓
DAG deduplicates by node key
    ↓
classify requested canonical nodes
    ↓
missing/stale only: CommandArgs → realize callback/template
    ↓
execute once and verify every output
```

The central distinction is: identity and paths are eager; command realization
and materialization are lazy.
