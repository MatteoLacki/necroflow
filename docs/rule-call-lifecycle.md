# What Happens When a Rule Is Called in a Pipeline Factory

Calling a rule inside a pipeline factory builds the DAG; it does not run the
command. For a callable command, the callback runs much later—only after
fingerprints and paths are resolved, and only if the node actually needs
execution.

This document follows the callable-command example:

```python
def sort_command(args: CommandArgs) -> str:
    argv = ["sort"]
    if args.config.reverse:
        argv.append("-r")
    if args.config.unique:
        argv.append("-u")
    argv.append(str(args.inputs.source))
    return f"{shlex.join(argv)} > {shlex.quote(str(args.outputs.sorted_text))}"


@command(sort_command)
def sort_text(source: SourceText, reverse: bool, unique: bool):
    sorted_text = output(SortedText)
    return sorted_text


def sorting_pipeline(config: dict) -> Pipeline:
    pipeline = Pipeline()
    pipeline.source = source_text(path=str(config["input"]))
    pipeline.sorted = sort_text(
        pipeline.source,
        reverse=config.get("reverse", False),
        unique=config.get("unique", False),
    )
    return pipeline
```

The complete example is in `examples/callable_fingerprint/pipeline.py`.

## 0. Before the Factory: the Decorator Creates a `Rule`

This happens when Python imports the pipeline module:

```python
@command(sort_command)
def sort_text(...):
    ...
```

It is effectively:

```python
def declaration(...):
    ...

sort_text = command(sort_command)(declaration)
```

Because no factory-style `Inputs` and `Outputs` declarations were passed,
`command()` selects the decorator path:

```python
def command(cmd, *declarations, ...):
    ...
    return _decorator_command(cmd, repeat=repeat, **constraints)
```

The decorator parses the declaration:

```python
def decorator(fn):
    rule_name, inputs, outputs, info = _parse_rule_fn(fn)
    return _make_rule(
        name=rule_name,
        inputs=inputs,
        outputs=outputs,
        command=cmd,
        constraints=constraints,
        info=info,
        repeat=repeat,
    )
```

The declaration body is not executed. `_parse_rule_fn()`:

1. Reads its annotations.
2. Obtains its source with `inspect.getsource()`.
3. Parses the source into an AST.
4. Recognizes assignments such as
   `sorted_text = output(SortedText)`.
5. Checks that the final return contains exactly the declared outputs.

The original `sort_text` function is replaced with a `Rule` object:

```python
sort_text: Rule
```

The rule records:

```python
rule.__name__       # "sort_text"
rule.inputs         # source, reverse, unique
rule.outputs        # sorted_text
rule.command        # sort_command callback
rule.constraints
rule.repeat
```

During `Rule.__init__()`, inputs are divided into two groups:

```python
self._pos_inputs = [
    (name, type)
    for name, type in inputs.specs.items()
    if _is_node_input_contract(type)
]

self._kw_inputs = {
    name: type
    for name, type in inputs.specs.items()
    if not _is_node_input_contract(type)
}
```

For this rule:

```python
_pos_inputs = [("source", SourceText)]

_kw_inputs = {
    "reverse": bool,
    "unique": bool,
}
```

Because the command is callable, it is validated immediately:

```python
if callable(command):
    validate_command_callback(command)
```

Validation requires:

- A source-inspectable function or lambda.
- Exactly one positional argument.
- A module-level definition.
- No closure.
- An unambiguous, parseable AST.

None of this executes `sort_command`.

## 1. The CLI Calls the Pipeline Factory

For a job TOML, the CLI dynamically loads the configured factory and calls it:

```python
factory = _load_factory(job_config.pipeline_spec)
pipeline = factory(job_config.config)
```

Execution enters:

```python
def sorting_pipeline(config: dict) -> Pipeline:
    pipeline = Pipeline()
    ...
```

`Pipeline()` initially contains:

```python
self._nodes_list = []
self._node_names = {}
self._sections = []
self._active_section = None
```

No DAG nodes exist yet.

## 2. Python Evaluates the Right-Hand Side

For:

```python
pipeline.sorted = sort_text(
    pipeline.source,
    reverse=True,
    unique=True,
)
```

Python evaluates the rule call before performing the assignment:

```python
sort_text(
    pipeline.source,
    reverse=True,
    unique=True,
)
```

Because `sort_text` is a `Rule`, this invokes:

```python
Rule.__call__(*args, **kwargs)
```

At this point:

```python
args = (pipeline.source,)

kwargs = {
    "reverse": True,
    "unique": True,
}
```

## 3. The Rule Validates the Call

First, the positional count is checked:

```python
if len(args) < len(self._pos_inputs):
    raise TypeError(...)

if len(args) > len(self._pos_inputs):
    raise TypeError(...)
```

Then all declared configuration arguments must be present:

```python
missing_kw = [
    name for name in self._kw_inputs
    if name not in kwargs
]
if missing_kw:
    raise TypeError(...)
```

Every positional input must be a compatible `Node`:

```python
for (name, expected_type), value in zip(self._pos_inputs, args):
    if not isinstance(value, Node):
        raise TypeError(...)

    if value.node_type is None or not _matches_node_type(
        value.node_type, expected_type
    ):
        raise TypeError(...)
```

Thus `pipeline.source` must be a node whose type satisfies the declared
`SourceText` contract.

Configuration values receive runtime type checks where `isinstance()` supports
the annotation:

```python
for name, value in kwargs.items():
    if name not in self._kw_inputs:
        continue

    expected_type = self._kw_inputs[name]
    try:
        ok = isinstance(value, expected_type)
    except TypeError:
        ok = True

    if not ok:
        raise TypeError(...)
```

One current implementation detail is that unknown keyword arguments are not
rejected here. They are retained in the call's configuration, although the
type-validation loop skips them.

## 4. The Logical Parent List Is Created

After validation:

```python
parents = [arg for arg in args if isinstance(arg, Node)]
```

For the example:

```python
parents = [pipeline.source]
```

The parent is already a concrete in-memory `Node`:

```python
assert parents[0] is pipeline.source
```

The child stores a direct reference to that node, forming an in-memory graph
edge:

```python
pipeline.sorted.parents == [pipeline.source]
```

Only filesystem addressing and materialization are deferred. At this point the
parent node exists, but its final path may not have been calculated and its
output file may not have been produced:

```python
pipeline.source                 # instantiated Node
pipeline.source.path is None    # not yet addressed
```

These are three separate stages:

1. **Instantiated:** the Python `Node` object exists.
2. **Addressed:** `node.path` has been calculated.
3. **Materialized:** execution has produced the output at that path.

## 5. One `RuleCall` Is Created

The rule delegates output creation:

```python
nodes = Node.make_outputs(
    self,
    parents,
    kwargs,
    self.command,
    self.outputs.specs,
)
```

`Node.make_outputs()` first creates a `RuleCall`:

```python
call = RuleCall(
    rule=rule,
    parents=parents,
    config=config,
    command=command,
)
```

The resulting object is approximately:

```python
RuleCall(
    rule=sort_text,
    parents=[pipeline.source],
    config={
        "reverse": True,
        "unique": True,
    },
    command=sort_command,
    execution_context={},
    output_nodes={},
    fingerprint_function=default_fingerprint,
    fingerprint_provider="necroflow.default_fingerprint/v2",
    _full_fingerprint=None,
    _realized_command=None,
    _command_realized=False,
    _resolved_root=None,
)
```

The `RuleCall` represents one concrete invocation of the reusable `Rule`.

## 6. One `Node` Is Created per Declared Output

`Node.make_outputs()` constructs all outputs:

```python
nodes = [
    Node(
        output_name=output_name,
        node_type=output_type,
        parents=parents,
        config=config,
        rule=rule,
        command=command,
        execution_context=call.execution_context,
        rule_call=call,
    )
    for output_name, output_type in outputs_specs.items()
]
```

For the example, that creates approximately:

```python
Node(
    output_name="sorted_text",
    node_type=SortedText,
    parents=[pipeline.source],
    config={
        "reverse": True,
        "unique": True,
    },
    rule=sort_text,
    command=sort_command,
    path=None,
    rule_call=call,
)
```

For a multi-output rule, all nodes share the same `RuleCall`:

```text
RuleCall
 ├── output "bam" → Node
 └── output "log" → Node
```

The sibling mapping is installed on both sides:

```python
all_outputs = {node.output_name: node for node in nodes}

for node in nodes:
    node.output_nodes = all_outputs

call.output_nodes = all_outputs
```

This sharing is why co-outputs have one command realization, one full
fingerprint, and one output directory.

## 7. The `Rule` Returns the Output Shape

For a single-output rule:

```python
value = nodes[0]
```

For multiple outputs, the rule creates and returns a generated named tuple:

```python
value = self._return_type(*nodes)
```

Thus `sort_text(...)` returns one `Node`. A multi-output call can look like:

```python
pipeline.bam, pipeline.log = align(...)
```

Both returned nodes are backed by the same `RuleCall`.

## 8. Assignment Registers the Node in the Pipeline

Only after the right-hand side has returned does Python perform:

```python
pipeline.sorted = returned_node
```

This invokes `Pipeline.__setattr__()`:

```python
def __setattr__(self, name, value):
    if name.startswith("_"):
        object.__setattr__(self, name, value)
        return

    if name in self._node_names:
        raise ValueError(...)

    if isinstance(value, Node):
        self._nodes_list.append(value)
        self._node_names[name] = value
        self._section_by_node_id[id(value)] = self._active_section
        value.pipeline_label = name

    object.__setattr__(self, name, value)
```

After assignment:

```python
pipeline.nodes == [
    pipeline.source,
    pipeline.sorted,
]

pipeline.sorted.pipeline_label == "sorted"
```

The attribute name `sorted` is the user-facing pipeline label. It is separate
from:

```python
node.output_name       # "sorted_text"
node.rule.__name__     # "sort_text"
node.node_type         # SortedText
```

At this moment:

```python
node.path is None
rule_call._full_fingerprint is None
rule_call._realized_command is None
```

The command callback has still not run.

## 9. The Factory Returns the Constructed Pipeline

The factory finishes:

```python
return pipeline
```

The result is an in-memory graph:

```text
source_text RuleCall
 └── SourceText Node: pipeline.source
      └── sort_text RuleCall
           └── SortedText Node: pipeline.sorted
```

It is not yet an execution plan with final paths, but its logical identity is
complete.

## 10. A Project Fingerprint Function May Be Installed

After the factory returns, the CLI checks the job configuration:

```python
if job_config.fingerprint_spec:
    pipeline.set_fingerprint_function(
        _load_fingerprint(job_config.fingerprint_spec),
        provider=job_config.fingerprint_spec,
    )
```

This walks registered nodes and their ancestors, finds every distinct
`RuleCall`, and replaces:

```python
call.fingerprint_function
call.fingerprint_provider
```

It also clears any previously cached fingerprint and realized command. This
happens before normal output addressing.

## 11. The Fingerprint Is Computed Lazily

A fingerprint is not necessarily calculated during `Rule.__call__()` or
assignment. It is calculated when something asks for:

```python
node.full_fingerprint
node.fingerprint
node.key
```

The property chain is:

```text
node.key
    → node.fingerprint
        → node.full_fingerprint
            → node.rule_call.full_fingerprint
```

The relevant code is:

```python
@property
def full_fingerprint(self) -> str:
    if self.rule_call is not None:
        return self.rule_call.full_fingerprint

@property
def fingerprint(self) -> str:
    return self.full_fingerprint[:16]

@property
def key(self) -> str:
    return f"{rule_name}/{self.fingerprint}/{filename}"
```

`RuleCall` constructs a path-free `FingerprintArgs`:

```python
FingerprintArgs(
    rule_name=self.rule.__name__,
    command=self.command,
    inputs=NamedValues(named_parents),
    config=NamedValues(self.config),
    input_types=NamedValues(self.rule.inputs.specs),
    output_types=NamedValues(self.rule.outputs.specs),
    constraints=NamedValues(self._constraints()),
    execution_context=NamedValues(self.execution_context),
    repeat=self.rule.repeat,
    recipe_identity=self.rule.recipe_identity,
)
```

There are deliberately no resolved output paths here.

The selected fingerprint function is called:

```python
value = self.fingerprint_function(self.fingerprint_args())
self._full_fingerprint = validate_fingerprint_result(
    value,
    provider=self.fingerprint_provider,
)
```

It must return exactly 64 lowercase hexadecimal characters.

### Default fingerprint

The default logical identity includes:

```python
identity = {
    "domain": "necroflow.fingerprint/v2",
    "rule": args.rule_name,
    "command": command_identity,
    "config": dict(args.config.items()),
    "execution_context": dict(args.execution_context.items()),
    "parents": parent_identities,
    "input_types": input_type_names,
    "output_types": output_type_names,
}
```

A callable command's identity is:

```python
{
    "kind": "python",
    "python": python_identity(),
    "ast": canonical_callback_ast,
}
```

The callback is inspected but not executed.

Each parent contributes:

```python
{
    "name": input_name,
    "fingerprint": parent.full_fingerprint,
    "output": parent.output_name,
}
```

Therefore fingerprinting recursively fingerprints the upstream graph.

The identity is canonically framed and SHA-256 hashed:

```python
hashlib.sha256(
    canonical_bytes(identity, path="fingerprint")
).hexdigest()
```

The full digest is used for metadata; its first 16 characters are used in
paths. The default fingerprint currently does not consume `constraints` or
`repeat`, although a custom project fingerprint can inspect those fields.

## 12. The Node Key Becomes Its Content Address

Once fingerprinted, the node key is:

```python
f"{rule_name}/{fingerprint_prefix}/{filename}"
```

For example:

```text
sort_text/2c7c9b662643327e/sorted.txt
```

The filename comes from `node.node_type.filename` when declared, otherwise
from `node.output_name`.

DAG insertion uses this key to deduplicate equivalent computations:

```python
self._nodes.setdefault(node.key, node)
```

Two independently constructed nodes with the same key become one canonical
DAG computation.

## 13. Final Filesystem Paths Are Resolved

Before execution:

```python
pipeline.resolve_paths(outdir)
```

This eventually calls:

```python
def resolve_paths(nodes, outdir):
    outdir = Path(outdir)

    for call in distinct_rule_calls:
        call.set_resolved_root(outdir.resolve())

    for node in nodes:
        node.path = outdir / node.key
```

The example path becomes something like:

```text
nodes/
└── sort_text/
    └── 2c7c9b662643327e/
        └── sorted.txt
```

All outputs belonging to one `RuleCall` share:

```text
nodes/sort_text/2c7c9b662643327e/
```

Changing the output root clears the cached realized command because its paths
may now be different. It does not alter the logical fingerprint.

## 14. Necroflow Classifies the Node

Before executing anything, Necroflow checks the filesystem and assigns states
such as:

```python
MISSING
STALE
UP_TO_DATE
ORPHAN
```

If the output already exists and is valid, it becomes `UP_TO_DATE`. In that
case:

- The command is not executed.
- A callable command normally is not called.
- Its output is reused from the cache.

Normal dry-run also does not realize the callback; it only reports which
resolved node paths would run.

## 15. The Scheduler Selects the Node

Once all parents are `UP_TO_DATE`, the node advances:

```text
MISSING or STALE
        ↓
      READY
        ↓
     RUNNING
```

The scheduler checks resource constraints, co-output state, and available
capacity. Once selected:

```python
node.mark_running()
node.state = NodeState.RUNNING

future = pool.submit(_run, node, log_path)
```

Co-outputs are guarded so that two output nodes from the same `RuleCall` do not
launch the shared command twice.

## 16. The Callable Command Receives Final `CommandArgs`

Inside `_run_node()`:

```python
cmd = resolve_command(node)
```

For a callable command:

```python
result = node.command(call.command_args())
```

Only now is `sort_command()` executed.

`command_args()` first requires every input and output path to have been
resolved:

```python
if not self.output_nodes or any(
    node.path is None for node in self.output_nodes.values()
):
    raise RuntimeError("command paths have not been resolved")
```

It then creates:

```python
CommandArgs(
    inputs=NamedValues({
        "source": pipeline.source.path,
    }),
    config=NamedValues({
        "reverse": True,
        "unique": True,
    }),
    outputs=NamedValues({
        "sorted_text": pipeline.sorted.path,
    }),
    constraints=NamedValues({
        "threads": 1,
        ...
    }),
    workdir=pipeline.sorted.path.parent,
)
```

The callback can use mapping or attribute access:

```python
args.inputs.source
args.inputs["source"]

args.config.reverse
args.config["reverse"]

args.outputs.sorted_text
args.constraints.threads
args.workdir
```

`CommandArgs` is frozen, and each `NamedValues` mapping is read-only. The
callback receives final paths directly. It does not modify `CommandArgs` or
resolve paths itself.

## 17. The Returned Command Is Validated and Cached

The callback must return one non-empty shell string:

```python
if not isinstance(result, str) or not result.strip():
    raise TypeError(...)
```

The result is cached on the shared `RuleCall`:

```python
call._realized_command = result
call._command_realized = True
```

If provenance generation or a sibling output asks for the command again, the
callback is not rerun:

```python
if call._command_realized:
    return call._realized_command
```

For the example, the result might be:

```text
sort -r -u nodes/source_text/.../input.txt > nodes/sort_text/.../sorted.txt
```

Unlike string-template commands, callback commands own their quoting
completely.

## 18. The Shell Command Is Executed

`_run_node()` creates the output and log directories, then runs:

```python
subprocess.run(
    cmd,
    shell=True,
    check=True,
    stdout=log,
    stderr=log,
)
```

If an explicit shell path was configured, it also passes:

```python
executable=shellpath
```

The subprocess writes standard output and error to:

```text
<rule-call-directory>/.rip/job.log
```

## 19. Successful Execution Is Verified and Recorded

After the subprocess returns successfully, Necroflow checks that every active
declared output exists:

```python
for conode in node.output_nodes.values():
    if conode.key in active_keys and not conode.path.exists():
        raise RuntimeError(
            f"command succeeded but output missing: {conode.path}"
        )
```

It then writes:

- Execution events.
- `.rip/run.toml`.
- `.rip/dependencies.toml`.
- Content hashes for outputs.
- Invalidation tokens, if configured.
- The ancestor graph.
- The final `up_to_date` state.

Provenance separates logical fingerprint information from realized command
information:

```toml
[fingerprint]
format = "v2"
provider = "..."
digest = "full 64-character digest"

[command]
kind = "python"
realized = "sort -r -u ..."
source = "pipeline.py"
python = "CPython-..."
```

Finally, all active co-output nodes sharing the call are marked `UP_TO_DATE`.

## Condensed Lifecycle

```text
Module import
    @command turns declaration function into Rule
                       ↓
Pipeline factory
    Rule.__call__ validates arguments
                       ↓
    creates one RuleCall + output Node(s)
                       ↓
    Pipeline assignment registers Node(s)
                       ↓
Factory returns Pipeline
                       ↓
Optional project fingerprint is installed
                       ↓
Node key requested
    logical FingerprintArgs → fingerprint
    callback AST hashed, callback not executed
                       ↓
Paths resolved from node key
                       ↓
Cache/state classification
                       ↓
If execution is needed:
    final paths → CommandArgs
                       ↓
    command callback runs once
                       ↓
    returned shell string runs
                       ↓
    outputs verified and provenance written
```

The exact effect of this line inside the factory:

```python
pipeline.sorted = sort_text(...)
```

is only to create and register a logical rule invocation. It performs no
filesystem work and does not call `sort_command()`.
