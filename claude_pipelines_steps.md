# Walkthrough: what a cohort pipeline actually executes

Line-by-line trace of this program against the necroflow source:

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


dag = DAG("nodes")
P = Pipeline(dag)
cohort_pipeline(P, config)
P.finish()
dag.require(P.sinks())
dag.run()
```

## Assumptions

`config` is undefined in the snippet; assume a dict with `reference`, `annotation`, and
`samples` keys. The seven rules are likewise undefined; assume module-level `Rule` objects
built by `@command` / `symlink_file` / `text_file`, with `align` declaring two outputs.
`raw_fastq(S, path=...)` takes no Node inputs and one config kwarg — the shape of
`symlink_file_rule`.

---

## `dag = DAG("nodes")`

Pure bookkeeping. No directory is created; `nodes/` first appears when the executor takes
its lock.

```python
# src/necroflow/pipeline.py:465
def __init__(self, outdir):
    self._calls: dict[Path, RuleCall] = {}
    self._nodes: dict[Path, Node] = {}
    self._required: set[Path] = set()
    self._labels_by_path: dict[Path, set[str]] = {}
    self.outdir = Path(outdir).expanduser().resolve()
    self.last_execution_report = None
```

## `P = Pipeline(dag)`

The Pipeline owns no nodes of its own. It holds a `_PipelineState` that every prefixed view
will later share by reference, and an empty request prefix marking it as the root.

```python
# src/necroflow/pipeline.py:292
def __init__(self, dag: DAG, *, shellpath: str | Path | None = None):
    if not isinstance(dag, DAG):
        raise TypeError(
            f"Pipeline requires an owning DAG, got {type(dag).__name__}"
        )
    self._state = _PipelineState(dag, shellpath)
    self._request_prefix = ""
```

```python
# src/necroflow/pipeline.py:277
class _PipelineState:
    """Mutable construction state shared by one root Pipeline and all its views."""

    def __init__(self, dag: DAG, shellpath: str | Path | None) -> None:
        self.dag = dag
        self.shellpath = _normalize_shellpath(shellpath)
        self.nodes_list: list[Node] = []
        self.node_paths: set[Path] = set()
        self.node_names: dict[str, Node] = {}
        self.finished = False
```

`shellpath=None` short-circuits `_normalize_shellpath`, so no filesystem probe happens here.

---

## `P.reference = prepare_reference(P, path=config["reference"])`

### Calling the rule

`Rule.__call__` validates, then compiles. Note `pipeline._assert_open()` — this is the gate
that `P.finish()` will later slam shut.

```python
# src/necroflow/rules.py:503
def __call__(self, pipeline, /, *args: Any, **kwargs: Any) -> _ReturnT:
    """Validate one invocation and return its canonical output Nodes."""
    self._validate_pipeline(pipeline)
    pipeline._assert_open()
    config = self._effective_config(kwargs)
    self._validate_input_presence(args, config)
    self._validate_parent_nodes(pipeline, args)
    self._validate_config_values(config)
    nodes = self._compile_outputs(pipeline, args, config)
    return self._shape_outputs(nodes)
```

`prepare_reference` declares zero Node inputs, so `_validate_parent_nodes` iterates an empty
`_pos_inputs` and does nothing. `_validate_config_values` checks `path` with `isinstance`
against its annotation, tolerating annotations that `isinstance` cannot handle.

### Compiling to a RuleCall and Nodes

Addresses are computed eagerly. By the time `make_outputs` returns there is no unresolved
path anywhere.

```python
# src/necroflow/nodes.py:135
shellpath = pipeline.shellpath if command is not None else None
call = RuleCall(
    dag=pipeline.dag,
    rule=rule,
    inputs=node_inputs,
    config=config,
    command=command,
    shellpath=shellpath,
)
call._rule_hash, call._provenance_hash = compute_hashes(call)
rule_component = _safe_path_component(rule.__name__, kind="rule name")
call._relative_path = (
    Path(rule_component) / call.rule_hash / call.provenance_hash
)
```

Each declared output becomes one Node under that shared directory, named by its NodeType's
`filename`:

```python
# src/necroflow/nodes.py:152
for oname, otype in outputs_specs.items():
    output_filename = otype.filename
    assert output_filename is not None
    filename = _safe_path_component(
        output_filename, kind=f"output {oname!r} filename"
    )
    relative_path = call.relative_path / filename
```

### The two hashes

The rule hash covers the recipe contract only — no config, no parents:

```python
# src/necroflow/fingerprints.py:283
identity = {
    "domain": RULE_HASH_DOMAIN,
    "rule": rule_name,
    "command": _command_identity(command, recipe_identity),
    "input_types": {
        name: _type_name(annotation) for name, annotation in input_types.items()
    },
    "output_types": {
        name: {
            "type": _type_name(annotation),
            "filename": annotation.filename,
            "mutable": annotation.mutable,
        }
        for name, annotation in output_types.items()
    },
}
```

The provenance hash adds config, shell, and lineage:

```python
# src/necroflow/fingerprints.py:329
identity = {
    "domain": PROVENANCE_HASH_DOMAIN,
    "rule_hash": local_rule_hash,
    "config": call.config,
    "execution_context": (
        {"shellpath": call.shellpath} if call.shellpath is not None else {}
    ),
    "parents": _parent_identity(call),
}
```

For `prepare_reference` the `parents` list is empty. Neither hash sees a Pipeline label or a
subpipeline prefix — that is what makes identical calls in different subpipelines collapse.

### Interning

```python
# src/necroflow/pipeline.py:481
def intern(self, call: RuleCall) -> RuleCall:
    """Return the canonical RuleCall for this relative call path."""
    existing = self._calls.get(call.relative_path)
    if existing is not None:
        ...
        return existing
    self._calls[call.relative_path] = call
    for node in call.output_nodes.values():
        if node.relative_path in self._nodes:
            raise ValueError(f"duplicate output path: {node.relative_path}")
        self._nodes[node.relative_path] = node
    return call
```

`_shape_outputs` then returns a bare Node, because `prepare_reference` has one output:

```python
# src/necroflow/rules.py:494
def _shape_outputs(self, nodes: list[Node]) -> _ReturnT:
    """Return one Node or the rule-specific named tuple of co-outputs."""
    if self._multi:
        assert self._return_type is not None
        value = self._return_type(*nodes)
    else:
        value = nodes[0]
    return cast(_ReturnT, value)
```

### Binding the label

```python
# src/necroflow/pipeline.py:445
def __setattr__(self, name: str, value: object) -> None:
    if name.startswith("_"):
        object.__setattr__(self, name, value)
        return
    if isinstance(value, Node):
        if any(name in cls.__dict__ for cls in type(self).__mro__):
            raise ValueError(
                f"Pipeline attribute {name!r} is reserved; use item syntax "
                f"P[{name!r}] if this label is intentional"
            )
        self._assign_node(name, value)
    object.__setattr__(self, name, value)
```

Two consequences worth naming:

- A label colliding with a Pipeline class member (`nodes`, `dag`, `labels`, `sinks`, `save`,
  …) raises and forces `P["nodes"] = ...`. None of the labels in this program collide.
- The final `object.__setattr__` also stores the Node in the instance `__dict__`. So a later
  `P.reference` reads the instance attribute directly and never reaches `__getattr__`. The
  `__getattr__` path only matters when a *different* view object looks the label up.

```python
# src/necroflow/pipeline.py:410
def _assign_node(self, name: str, value: Node) -> None:
    self._assert_open()
    qualified_name = self._qualified_label(name)
    label_path = _validate_pipeline_label(qualified_name, value.path.name)
    if name in pipeline_keywords.RESERVED:
        raise ValueError(f"Pipeline label {name!r} is reserved")
    if qualified_name in self._state.node_names:
        raise ValueError(f"Pipeline label {qualified_name!r} already assigned")
    if value.rule_call.dag is not self._state.dag:
        raise ValueError(
            f"Node assigned as {qualified_name!r} belongs to a different DAG"
        )
    result_path = label_path / value.path.name
    for existing_name, existing_node in self._state.node_names.items():
        existing_path = PurePosixPath(existing_name) / existing_node.path.name
        if _result_paths_conflict(result_path, existing_path):
            raise ValueError(...)
    if value.relative_path not in self._state.node_paths:
        self._state.nodes_list.append(value)
        self._state.node_paths.add(value.relative_path)
    self._state.node_names[qualified_name] = value
    self._state.dag._record_binding(value, qualified_name)
```

`RESERVED` is currently an empty frozenset (`src/necroflow/keywords.py:3`), so that check
never fires today. `nodes_list` dedups by `relative_path`; `node_names` does not — several
labels may alias one Node.

## `P.annotation = prepare_annotation(...)`

Identical path. The conflict loop now compares `annotation/<filename>` against
`reference/<filename>`; neither nests inside the other, so it passes.

---

## `P.subpipeline(f"samples/{name}")`

A view, not a new pipeline. It shares `_state` by reference — same DAG, same `nodes_list`,
same `node_names`, same `finished` flag.

```python
# src/necroflow/pipeline.py:351
def subpipeline(self, request_prefix: str) -> Pipeline:
    """Return a view that qualifies assignments with a request prefix."""
    self._assert_open()
    prefix = _validate_request_path(
        request_prefix, kind="Subpipeline request prefix"
    ).as_posix()
    if self._request_prefix:
        prefix = f"{self._request_prefix}/{prefix}"
    return type(self)._view(self._state, prefix)
```

`_validate_request_path` rejects absolute paths, empty strings, leading dots, `.`/`..`
components, repeated separators, and over-long components. A sample named `.tmp` or `a//b`
raises here, before any rule runs.

## `S.fastq = raw_fastq(S, path=sample["reads"])`

Same compile path as `prepare_reference`. The label qualifies through the view:

```python
# src/necroflow/pipeline.py:361
def _qualified_label(self, name: str) -> str:
    """Return a view-local label qualified for the root namespace."""
    if self._request_prefix:
        return f"{self._request_prefix}/{name}"
    return name
```

Label becomes `samples/<name>/fastq`. **The prefix is not in either hash.** Two samples
pointing at the same `reads` path produce byte-identical hashes, `intern` returns the
pre-existing `RuleCall`, and both labels alias one Node — and transitively one `align`, one
`counts`, one qc chain.

## `S.bam, S.align_log = align(S, S.fastq, reference)`

Two positional Node arguments. `_validate_parent_nodes` checks each one:

```python
# src/necroflow/rules.py:444
for index, parent in enumerate(values):
    position = f"{pname}[{index}]" if contract.variadic else pname
    if not isinstance(parent, Node):
        raise TypeError(...)
    if not _matches_node_type(parent.node_type, contract.element_type):
        got = parent.node_type.__name__ if parent.node_type else "None"
        raise TypeError(...)
    if parent.rule_call.dag is not pipeline.dag:
        raise ValueError(f"{name}: {position!r} belongs to a different DAG")
```

`reference` is the root-level Node handed in as a plain argument — the sanctioned way to feed
external inputs to a reusable subpipeline factory. The DAG check is what makes it safe.

Now the provenance hash has lineage:

```python
# src/necroflow/fingerprints.py:268
parents.append(
    {
        "name": name,
        "rule_hash": parent.rule_hash,
        "provenance_hash": parent.provenance_hash,
        "output": parent.output_name or "",
        **({"mutable": True} if parent.mutable else {}),
    }
)
```

So the bam and log directories depend on both the fastq and the reference lineage.

`align` declares two outputs, so `_shape_outputs` returns an `align_outputs` namedtuple. Both
Nodes share one `RuleCall`, one workdir, one realized command, and one `output_nodes` dict:

```python
# src/necroflow/nodes.py:181
all_outputs: dict[str, Node] = {n.output_name: n for n in nodes}
for n in nodes:
    n.output_nodes = all_outputs
call.output_nodes = all_outputs
canonical = pipeline.dag.intern(call)
```

The statement is ordinary tuple unpacking, so `_assign_node("samples/<n>/bam")` runs first,
then `_assign_node("samples/<n>/align_log")`. Two labels, two `nodes_list` entries (distinct
filenames), one rule call.

## `qc_pipeline(S.subpipeline("qc"), S.bam)`

Prefixes compose: `self._request_prefix` is `samples/<name>`, so the new view's prefix is
`samples/<name>/qc`. The view object is passed straight in and never stored.

Inside, `Q.metrics = collect_metrics(Q, bam)` labels `samples/<name>/qc/metrics`, then
`Q.report = render_qc(Q, Q.metrics)` reads `Q.metrics` from the instance `__dict__` (see the
`__setattr__` note above) and labels `samples/<name>/qc/report`.

## `S.counts = count_reads(S, S.bam, annotation)`

`S.bam` from the instance dict, `annotation` the root Node. Label `samples/<name>/counts`.

---

## `P.finish()`

```python
# src/necroflow/pipeline.py:345
def finish(self) -> None:
    """Freeze this root Pipeline and every prefixed view over it."""
    if self._request_prefix:
        raise RuntimeError("only the root Pipeline can finish construction")
    self._state.finished = True
```

Because `_state` is shared, every view created during the loop freezes at the same instant.
Any later `Rule.__call__` fails at `pipeline._assert_open()` *before* fingerprinting or
interning; later `_assign_node` and `subpipeline()` fail too.

## `dag.require(P.sinks())`

```python
# src/necroflow/pipeline.py:387
def sinks(self) -> list[Node]:
    """Return labeled Nodes with no labeled dependents after construction."""
    self._assert_finished()
    parent_paths = {
        parent.relative_path for node in self.nodes for parent in node.parents
    }
    return [node for node in self.nodes if node.relative_path not in parent_paths]
```

`self.nodes` is `self._state.nodes_list` — the *shared* list, so this spans the root and every
subpipeline in one pass.

| node | in `parent_paths`? | sink |
|---|---|---|
| `reference` | yes (parent of every `align`) | no |
| `annotation` | yes (parent of every `count_reads`) | no |
| `samples/<s>/fastq` | yes (parent of `align`) | no |
| `samples/<s>/bam` | yes (parent of `collect_metrics`, `count_reads`) | no |
| `samples/<s>/align_log` | no | **yes** |
| `samples/<s>/qc/metrics` | yes (parent of `render_qc`) | no |
| `samples/<s>/qc/report` | no | **yes** |
| `samples/<s>/counts` | no | **yes** |

`align_log` is a sink only because nothing consumes it. It would be produced regardless as an
`align` co-output, but requiring it makes it non-cleanable under `autoclean`.

```python
# src/necroflow/pipeline.py:506
def require(self, nodes) -> None:
    """Add canonical Nodes to the set requested for execution."""
    for node in nodes:
        if not isinstance(node, Node):
            raise TypeError(...)
        if node.rule_call.dag is not self:
            raise ValueError("required Node belongs to a different DAG")
        self._required.add(node.relative_path)
```

---

## `dag.run()`

```python
# src/necroflow/pipeline.py:559
def run(self, **kwargs):
    from necroflow.executor import run

    self.last_execution_report = run(self, **kwargs)
    return self.last_execution_report
```

All defaults: `connected_component_scheduler`, `keep_going=False`, `autoclean=False`,
`dry_run=False`, `resource_caps=None`.

### Lock

The first filesystem write of the entire program:

```python
# src/necroflow/executor.py:281
lock_path = outdir / ".rip" / "necroflow.lock"
lock_path.parent.mkdir(parents=True, exist_ok=True)
fh = open(lock_path, "w")
try:
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
except OSError:
    fh.close()
    raise RuntimeError(...)
```

Classification happens *inside* the lock so no second process can invalidate the filesystem
snapshot before jobs start.

### Classification

The required subgraph is the sinks plus all ancestors, so requiring three nodes per sample
pulls in `bam`, `metrics`, `fastq`, `reference`, and `annotation`. Nothing in this DAG ends up
ORPHAN — every constructed node is an ancestor of some sink.

```python
# src/necroflow/dag.py:205
for node in _topo_sort(list(required.values())):
    if node.path is None or not node.path.exists():
        node.state = NodeState.MISSING
        continue

    node_mtime = _output_mtime(node.path)
    stale = any(
        p.state in (NodeState.MISSING, NodeState.STALE)
        for p in node.parents
        if p.state is not None
    )
    if not stale:
        for p in node.parents:
            if p.path is None or not p.path.exists():
                continue
            if p.mutable:
                continue
            if _output_mtime(p.path) <= node_mtime:
                continue  # fast path: parent not newer
            hash_file = p.path.parent / ".rip" / (p.path.name + ".hash")
            if (
                hash_file.exists()
                and _content_hash(p.path) == hash_file.read_text().strip()
            ):
                continue  # parent re-ran but content unchanged
            stale = True
            break
    if _has_changed_invalidation(node):
        stale = True
    node.state = NodeState.STALE if stale else NodeState.UP_TO_DATE
```

Cold store: everything `MISSING`. Warm store: mtime fast path, then content hash, then the
invalidator token. `_prepare_active` then flips any `UP_TO_DATE` node whose `.rip/state` is not
literally `up_to_date` to `STALE` and propagates that to descendants.

### The run loop

Dependency gating is the executor's, not the scheduler's:

```python
# src/necroflow/executor.py:432
def _promote_states(active: list) -> None:
    """Advance node states one step: MISSING/STALE → READY or FAILED."""
    blocked = {NodeState.FAILED, NodeState.INTERRUPTED}
    for n in active:
        if n.state in (NodeState.MISSING, NodeState.STALE):
            if any(p.state in blocked for p in n.parents):
                n.state = NodeState.FAILED
            elif all(p.state == NodeState.UP_TO_DATE for p in n.parents):
                n.state = NodeState.READY
```

First iteration promotes only the parentless nodes: `reference`, `annotation`, and every
sample's `fastq`.

The scheduler only reorders:

```python
# src/necroflow/schedulers.py:124
return sorted(
    ready,
    key=lambda n: self._sizes.get(
        self._component_of.get(n.relative_path, -1), 0
    ),
)
```

This graph is **one connected component** — `reference` and `annotation` are shared by every
sample — so smallest-component-first is a tie across all ready nodes and degenerates to
registration order.

Submission enforces the co-output rule and resource caps:

```python
# src/necroflow/executor.py:668
# Submit one representative when several co-output Nodes
# of the same rule call are simultaneously READY.
coouts = [
    c
    for c in node.output_nodes.values()
    if c.relative_path in active_keys and c is not node
]
if any(c.state == NodeState.RUNNING for c in coouts):
    continue
job_res = node.rule.resources
# The solo fallback prevents a job declaring more than a
# configured cap from stalling forever.
can_run = (not running) or all(
    running_resources.get(r, 0) + v <= caps[r]
    for r, v in job_res.items()
    if r in caps
)
```

So when `bam` and `align_log` both become READY, only the first is submitted; the second is
skipped because its sibling is RUNNING. One `align` invocation produces both.

### Running one job

```python
# src/necroflow/executor.py:801
node.path.parent.mkdir(parents=True, exist_ok=True)
log_path.parent.mkdir(parents=True, exist_ok=True)
with open(log_path, "w") as log:
    materializer = getattr(node.rule, "materializer", None)
    if materializer is not None:
        materializer(node, log)
        return
    cmd = resolve_command(node)
```

`resolve_command` memoizes on the `RuleCall` (`src/necroflow/dag.py:278`), so co-outputs
realize exactly one command string. Substitutions cover input names → shlex-quoted parent
paths, config keys, constraints, `{constraint:name}`, every output name → its path, and
`{workdir}`.

`_run_with_retries` retries only `subprocess.CalledProcessError`, up to `rule.repeat`; the
default of 1 means a single attempt.

### Completion

```python
# src/necroflow/executor.py:456
for conode in node.output_nodes.values():
    if conode.relative_path in active_keys and not conode.path.exists():
        raise RuntimeError(f"command succeeded but output missing: {conode.path}")
_record_success_events(...)
write_dependencies(node)
write_ancestor_graph(node)
```

Exit 0 with a missing declared output is a failure. On success the rule-call directory gains
`.rip/dependencies.toml`, per-output `.hash` files, invalidation tokens, `.rip/graph.txt`, and
`.rip/run.toml`; siblings and self are marked `up_to_date`.

With `keep_going=False`, the first failure re-raises from inside the `with pool` block. The
`ThreadPoolExecutor` context manager waits for in-flight jobs, `_logger.summary` runs in the
`finally`, and the lock releases. Nodes blocked by a failed parent get no report entry at all.

### Result

`run()` returns `dict[str, ExecutionEvent]` keyed by `node.relative_path.as_posix()`, also
stored on `dag.last_execution_report`. This program discards it.

---

## Cold-run wave order

```
wave 0   prepare_reference, prepare_annotation, raw_fastq(<each sample>)
wave 1   align(<each sample>)          one submission each; bam + align_log together
wave 2   collect_metrics(<each>), count_reads(<each>)
wave 3   render_qc(<each>)
```

Thread caps permitting; the default cap is `os.cpu_count()` threads, and each rule claims
`threads=1` unless it declares otherwise.
