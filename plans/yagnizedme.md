# YAGNI review of necroflow

**Scope:** `src/necroflow/*.py` only — 22 modules, 5975 lines. Tests and docs consulted only
to measure blast radius.

**Method:** read every module; verified each claim against the source; ran two probes where
reading alone was not conclusive.

**Bias declared up front:** prefer plain dicts, tuples and functions over classes; delete
speculative generality; keep abstractions that carry real weight. Two findings below are
explicit recommendations *not* to change something.

---

## Status update — 2026-08-03

- **Y1 is complete** in commit `5e0bbea`. The fix gives every `execute()` call a fresh
  scheduler closure while retaining the incremental component index and affected-component
  re-BFS optimization. The historical analysis remains below, but the shared singleton and
  `ConnectedComponentScheduler` no longer exist.
- The original itemised estimate was **~250 removable lines, not ~600**. Because Y1 was
  fixed without deleting the incremental algorithm, its projected −100-line saving was not
  realised; the remaining deletion estimate is therefore closer to ~150 lines.
- **Y16 is a rename, not a dead-code deletion.** The `fingerprint` aliases are used broadly
  throughout the tests, so the blast radius is larger than the initial ten-assertion count.
- **N1 remains “do not change.”** `NamedValues.items` collision behaviour is specified and
  tested, with bracket access providing the declared value.
- **N2 remains “do not change.”** `Inputs`, `Outputs`, and `Constraints` are thin but
  load-bearing declaration vocabulary with hundreds of call sites.
- **Y4 must precede Y3.** `RuleCall.parents` must become cached before removing the hot
  `Node.parents` field.
- The original top-three payoff order was **Y1, Y2, Y11**. Y1 and Y2 are complete; Y11
  is now the highest-priority remaining item.
- **Y6 needs reassessment.** Production config loading leaves `short_names=False`, but
  `iter_configs(short_names=True)` is an explicitly tested API, so deleting
  `_compute_value_only_keys` is not currently zero-risk.

## Status update — 2026-08-05

Y3, Y4, Y5, Y7, Y8, Y9, Y10, Y11, Y12, Y14 are all complete (see the scoreboard for commit
hashes). `class DAG` also moved from `pipeline.py` into `dag.py` as a follow-up once Y8 removed
the shared `_GraphBase` base class that was the only reason the two lived together; ASCII
rendering moved to a new `ascii_render.py` to avoid a `dag.py`/`pipeline.py` import cycle.

**Y6 resolved differently than either scoreboard option.** Rather than delete
`_compute_value_only_keys` or leave it dead, `short_names=True` became the CLI's default
(`--long-names` opts back into the old long-form labels), so the function is now exercised on
every real job. Remaining open items are Y13, Y15, Y16, Y17, Y18 — all explicitly "later" or
"decide" in the original review, not zero-risk auto-fixes.

---

## Verdict

The core is sound. Eager addressing, content-addressed identity, filesystem-as-state, one
canonical `RuleCall` per invocation, framed canonical hashing — none of that needs
rethinking, and most of it is better than average.

The remaining cost sits in the periphery: several zero-behaviour wrappers and duplicated
state on `Node`.

**The original realistic estimate was ~250 lines, not the ~600 estimated in conversation.**
After resolving Y1 without deleting its incremental algorithm, the remaining estimate is
closer to ~150 lines.

---

## Scoreboard

| ID | Finding | Lines | Risk | Do it? |
|---|---|---|---|---|
| Y1 | Scheduler leaks state across `execute()` calls | n/a | low | **done — `5e0bbea`** |
| Y2 | `_content_hash` reads whole files into memory | ~0 | low | **done — `f9b22c4`** |
| Y3 | `Node` duplicates five `RuleCall` fields | −10 | med | **done — `9640e8a`** |
| Y4 | `RuleCall` is a two-phase constructor | −15 | low | **done — `7e21bd7`** |
| Y5 | `keywords.py` is an empty frozenset | −5 | none | **done — `7f074c2`** |
| Y6 | `_compute_value_only_keys` is production-unused but API-tested | −23 | low | **made live, not deleted — see below** |
| Y7 | Dead local `import Path` | −1 | none | **done — `7f074c2`** |
| Y8 | `_GraphBase` is inheritance-as-code-sharing | −20 | low | **done — `2533a97`** |
| Y9 | `__str__` renderer keys layout on `id()` | ~0 | low | **done — `2533a97`** |
| Y10 | `grid.py` keys labels on `id()` | ~0 | med | **done — `b0b08bc`** |
| Y11 | `gc.py` finds rules by bytecode introspection | ~0 | med | **done — `2986855`** |
| Y12 | `_accumulated_config` is exponential on diamonds | +2 | low | **done — `5f80809`** |
| Y13 | Label assignment is O(n²) | +5 | low | later |
| Y14 | `Pipeline.__setattr__` writes the label twice | −2 | med | **done — `c488c78`** |
| Y15 | Four entry points for two built-in rules | −40 | med | later |
| Y16 | `fingerprint` compatibility aliases | −10 | low | later |
| Y17 | `_compat.py` for Python 3.10 | −9 | low | decide |
| Y18 | `autoclean` threaded through `execute()` | −30 | med | later |
| N1 | `NamedValues` → plain dict | −32 | high | **no** |
| N2 | `Inputs`/`Outputs`/`Constraints` → dicts | −30 | high | **no** |

---

## Bugs

### Y1 — the default scheduler leaks state between runs

**Status: resolved in `5e0bbea`.** The analysis below describes the pre-fix implementation.
The resolution kept incremental re-BFS and isolated its state in a fresh closure per execution.

`schedulers.py:132`, reached via `executor.py:552`.

Full analysis with a reproduction is in [`bugs/scheduler.md`](../bugs/scheduler.md). In brief:

```python
# src/necroflow/schedulers.py:115
if self._adj is None or (not self._component_of and remaining):
```

`_adj` is initialised to `{}` and never set to `None`, so the first disjunct is dead. The
rebuild therefore depends entirely on `_component_of` being empty — but it never is, because
`executor.py:654` checks its `while` condition before calling the scheduler, so the final
call always leaves keys behind. The next `execute()` treats a new graph as an incremental
update of the old one.

The chained defaults on the sort key are what make it silent:

```python
# src/necroflow/schedulers.py:124
key=lambda n: self._sizes.get(self._component_of.get(n.relative_path, -1), 0)
```

Verified: identical input, two different orderings.

```
fresh instance        : [x, b, c, d]     singleton x first, as designed
shared, after warmup  : [b, c, x, d]     wrong
shared, unseen keys   : [p, q, r]        silently degrades to fifo
```

**Severity is lower than it first appears.** `cli.py:213` accumulates every job and grid
variant into one `DAG`, and `_run` calls `dag.execute()` once (`cli.py:656`). `necroflow run`
cannot trigger this today. It is a trap for library embedders and for any future change that
executes more than one DAG per process.

**Why this is a YAGNI finding, not just a bug.** The 110 lines of incremental re-BFS with
component-id recycling exist to make a *sort key* cheaper. Nothing in the repo measures that
it was needed. The executor loop already performs three O(active) scans per iteration
(`executor.py:657-664`), and `nodes.py:218` already contains a correct stateless component
walk. There are two connected-component implementations in this codebase; keep the one that
cannot hold state.

```python
def connected_component_scheduler(ready, remaining, available_resources):
    """Prioritise nodes from the smallest connected component of remaining work."""
    size_of = {}
    for component in iter_connected_components(remaining):
        for node in component:
            size_of[node.relative_path] = len(component)
    return sorted(ready, key=lambda n: size_of[n.relative_path])
```

Plain `[]`, not `.get(..., 0)` — every node in `ready` is by definition in `remaining`, so a
miss is a bug and should say so.

### Y2 — `_content_hash` loads entire files into RAM

**Status: complete.** Files are streamed through SHA-256 in bounded
1 MiB chunks.

```python
# Pre-fix implementation
def _content_hash(path: Path) -> str:
    """SHA-256 of a file's bytes, or of all non-.rip files in a directory."""
    h = hashlib.sha256()
    if path.is_file():
        h.update(path.read_bytes())
```

For a framework whose worked examples are BAMs and FASTQs this is an operational limit, not
a style point. It runs on every staleness check that fails the mtime fast path, and again in
`write_dependencies` after every successful job. Chunk it.

**Deferred optimization path:** if profiling later shows repeated cross-invocation hashing is
material, separate a current parent-digest cache from the digest each child consumed. Validate
the cache against an inode, size, `mtime_ns`, and `ctime_ns` snapshot; stat before and after
hashing; publish updates with temporary-file-plus-atomic-rename. The existing node-store
`flock` serializes necroflow executions, while external writers require snapshot validation
rather than a `.rip` lock. This is deliberately deferred; Y2 implements chunking only.

### Y11 — `gc.py` discovers rules by walking bytecode names

```python
# src/necroflow/gc.py:43
for name in factory.__code__.co_names:
    value = factory.__globals__.get(name)
    if isinstance(value, Rule):
        rules.add(value)
```

This misses any rule reached through an attribute, a dict lookup, a closure, or a local
alias. The consequence of missing one is that its `declared_rule_hash` is absent from
`valid_rule_hashes`, `_incompatible_keys` marks its nodes obsolete, and `collect()` **deletes
them**.

A destructive operation should not rest on a reflective heuristic. Either require rules to be
declared explicitly in the GC script, or refuse to delete when discovery looks incomplete.
The current `raise ValueError` when *zero* rules are found only catches the total failure,
not the partial one — and partial is the dangerous case.

---

## Delete now — zero risk

### Y5 — `keywords.py`

An entire module for:

```python
# src/necroflow/keywords.py:3
RESERVED: frozenset[str] = frozenset()
```

plus a check on the hot assignment path (`pipeline.py:414`) that cannot fire. Textbook
speculative generality. Delete both.

### Y6 — `_compute_value_only_keys` is unreachable

**Status: resolved by making it live, not by deleting it.** `config.py:145` (`iter_job_configs`)
now accepts a `short_names` passthrough, and the CLI (`cli.py`, shared by `run`/`graph`/`outputs`/
`doctor`/`explain` via `_add_run_options`) defaults to `short_names=True`, with a new
`--long-names` flag to opt back into the old long-form labels. `_compute_value_only_keys` now runs
on every real job by default instead of being gated behind a flag nobody set.

While wiring this up: the one existing test for `short_names=True` used only int-valued grid
dimensions, which never reach `_compute_value_only_keys`'s bare-value branch — so the function's
actual disambiguation logic had zero test coverage even though its early-return guard did.
Added `test_short_names_omits_key_for_unambiguous_string_values` to close that gap, plus
`test_short_names_rejects_colliding_leaf_names` for a real but previously-unguarded risk:
`shorten_param_name` takes only the trailing dotted component (`model.width` -> `width`), so two
different nested parameters could shorten to the same key and silently collide in the generated
label. `iter_configs` now raises `ValueError` up front when that happens.

Original analysis, still accurate about why nothing in production used to reach this code:

`grid.py:284`, ~23 lines. It returns `set()` immediately unless `short_names=True`:

```python
# src/necroflow/grid.py:288
if not short_names:
    return set()
```

The only caller of `iter_configs` was `config.py:145`, which passed neither `short_names` nor
`equal_sign`.

### Y7 — dead local import

```python
# src/necroflow/pipeline.py:241
from pathlib import Path
```

inside `_GraphBase.save`. `Path` is already imported at `pipeline.py:5`.

---

## Simplify

### Y3 — `Node` duplicates its `RuleCall`

`Node` stores `config`, `rule`, `command`, `parents` and `mutable` (`nodes.py:63-77`), copied
in at construction:

```python
# src/necroflow/nodes.py:166
Node(
    output_name=oname,
    node_type=otype,
    mutable=otype.mutable,
    parents=call.parents,
    config=config,
    rule=rule,
    command=command,
    ...
)
```

Every one is already reachable: `rule_call.config`, `.rule`, `.command`, `.parents`, and
`node_type.mutable`. Node genuinely needs `output_name`, `node_type`, `relative_path`,
`path`, `rule_call`, `state`. Five redundant fields per node, and two places to keep in sync.

**One caveat that stops this being a naive delegation.** `RuleCall.parents` is a *computed
property* (`rule_call.py:41`) that rebuilds a list on every access, and `node.parents` is read
in hot loops — `classify_nodes`, `_topo_sort`, `_promote_states`, `iter_connected_components`.
Delegating `Node.parents` straight through would turn an attribute read into a rebuild.
Compute it once in `RuleCall.__post_init__` and store it; then delegate.

`node.output_nodes` (`nodes.py:181-184`) is likewise a second copy of `call.output_nodes`, and
it makes every Node reference itself. Three sites then need `conode is not node` guards
(`executor.py:229`, `:473`, and `_cleanup_parents`). Read through `node.rule_call.output_nodes`.

### Y4 — `RuleCall` is a two-phase constructor

```python
# src/necroflow/rule_call.py:25
_rule_hash: str | None = None
_provenance_hash: str | None = None
_relative_path: Path | None = None
```

assigned from outside the class five lines later:

```python
# src/necroflow/nodes.py:144
call._rule_hash, call._provenance_hash = compute_hashes(call)
rule_component = _safe_path_component(rule.__name__, kind="rule name")
call._relative_path = (
    Path(rule_component) / call.rule_hash / call.provenance_hash
)
```

Three property getters exist solely to raise `RuntimeError("... was not compiled")` for a
window that lasts those five lines. Everything needed is available at construction — compute
in `__post_init__`, delete the `None` states and all three guards.

`_command_realized: bool` alongside `_realized_command: str | None` is redundant: the callback
contract already rejects empty strings (`dag.py:284`), so `None` unambiguously means "not
realized".

### Y14 — `Pipeline.__setattr__` writes the label twice

```python
# src/necroflow/pipeline.py:449
if isinstance(value, Node):
    if any(name in cls.__dict__ for cls in type(self).__mro__):
        raise ValueError(...)
    self._assign_node(name, value)
object.__setattr__(self, name, value)
```

The label lands in `_state.node_names` *and* in this view object's instance `__dict__`. So
`__getattr__` — the documented lookup path, which is the thing that applies the request
prefix — is dead for any label read from the same object that assigned it. Lookup semantics
now depend on which view object you happen to be holding.

Drop the `object.__setattr__` for Node values. One dict, one path.

### Y12 — `_accumulated_config` is exponential

```python
# src/necroflow/dag.py:69
def _accumulated_config(node: Node) -> dict:
    config = {}
    for parent in node.parents:
        config.update(_accumulated_config(parent))
    config.update(node.config)
    return config
```

No memo, so a DAG with *k* diamonds costs 2^k. It runs once per successful job inside
`write_dependencies`. A `visited` dict fixes it in two lines.

### Y13 — label assignment is O(n²)

```python
# src/necroflow/pipeline.py:423
for existing_name, existing_node in self._state.node_names.items():
    existing_path = PurePosixPath(existing_name) / existing_node.path.name
    if _result_paths_conflict(result_path, existing_path):
```

Every assignment scans every prior label. A cohort of 1000 samples × 6 labels is ~18M
comparisons. Two sets — one of result paths, one of all directory prefixes — give O(depth)
per insert. Not urgent; flag it before someone runs a large cohort.

### Y8 / Y9 — the ASCII renderer

`_GraphBase` (`pipeline.py:107-243`) is inheritance used for code sharing, not polymorphism:
`Pipeline`, `DAG` and `_AncestorView` share only `__str__` and `save`. A
`render_ascii(nodes, header)` function called from three sites removes the base class and
`_AncestorView` entirely.

While in there: the renderer keys its whole layout on `id()`.

```python
# src/necroflow/pipeline.py:142
id_to_node = {id(n): n for n in nodes}
```

This contradicts the project's own stated invariant — "Identity via `node.relative_path`,
never `id()`". It works within one process, but it is exactly the pattern banned elsewhere,
and the dummy-node insertion at `pipeline.py:179-192` then has to mint negative integers to
share the same keyspace. Using `relative_path` and a separate dummy counter is no harder.

### Y10 — `grid.py` keys generated filenames on `id()`

```python
# src/necroflow/grid.py:182
return {id(v): sanitize_for_filename(v[key]) for v in vals}
# src/necroflow/grid.py:237
val_str = str(grid_indices[key][id(value)])
```

Same anti-pattern, but here it is load-bearing for user-visible output filenames. It only
works because `grid_params` keeps the objects alive; any reparse or `copy.deepcopy` silently
produces wrong labels. Worse, `iter_configs` mutates those very objects after indexing them:

```python
# src/necroflow/grid.py:341
for vals in grid_params.values():
    for v in vals:
        if isinstance(v, dict) and "__label" in v:
            del v["__label"]
```

Key by position in the `vals` list instead. `enumerate` already provides a stable index, and
the code falls back to exactly that in two of the three branches.

---

## Structural

### Y15 — four entry points for two behaviours

`text_file`, `text_file_rule`, `symlink_file`, `symlink_file_rule`. `text_file` additionally
supports both the bare and configured decorator forms, which costs two `@overload` stubs and
a `# pyright: ignore[reportInconsistentOverload]` (`rules.py:822-836`). `symlink_file`
supports only the bare form. The asymmetry is not motivated by anything in the source.

Pick one shape for both. Moderate blast radius — public API.

### Y18 — `autoclean` is woven through `execute()`

`execute()` is 240 lines with 9 parameters. `autoclean` alone touches six places: conditional
`children`/`final_keys` construction (`executor.py:640-648`), three `_cleanup_parents` calls,
and `_prepare_active`. The `on_complete` hook already exists and is the natural home for a
post-run sweep. The only thing lost is cleaning intermediates *during* a long run to save
peak disk — which may well be the point, in which case keep it. Worth deciding deliberately
rather than by inertia.

### Y16 — `fingerprint` compatibility aliases

```python
# src/necroflow/nodes.py:92
@property
def fingerprint(self) -> str:
    """Compatibility alias for :attr:`provenance_hash`."""
```

plus the twin at `rule_call.py:61`. Version is `0.0.4` — there is no released compatibility
to preserve.

**Correction to what I said in conversation:** these are not dead. `tests/test_dag_core.py`
and `tests/test_variadic_inputs.py` contain the originally counted assertions, but a current
repository audit finds `.fingerprint` used across several additional test modules. Renaming
it is still worth considering because two names impose a permanent tax, but this is a broad
rename, not a deletion.

### Y17 — `_compat.py`

`requires-python = ">=3.10"` and `exceptiongroup>=1.0.0; python_version < '3.11'`, while
`make venv` builds Python 3.14. If nothing actually tests 3.10, bump to `>=3.11`, delete the
module and drop the dependency. If 3.10 support is real, keep it — it is correct as written.
This is a decision to make, not a defect.

---

## Deliberately not recommended

A YAGNI review that only ever says "delete it" is not worth much. These two look like
obvious targets and should be left alone.

### N1 — `NamedValues` should stay

`contexts.py:12` — 32 lines with `__slots__`, `MappingProxyType`, `Generic[_T]` and
`__getattr__`, where a dict would do. Its docstring concedes the sharp edge: "Mapping methods
win when a declared name collides with the Mapping API", so an input named `items` is
shadowed by the method.

I was going to call that a footgun. It is not an accident — it is specified and tested:

```python
# tests/test_fingerprints.py:99
values = NamedValues({"sample": "S1", "items": "declared"})
...
assert callable(values.items)
assert values["items"] == "declared"
```

The collision has a defined resolution, bracket access always works, and the type is part of
the public `CommandArgs` contract that user callbacks are written against. That is a design
disagreement on my part, not a defect, and it is not worth breaking every Python command
callback to win. Leave it.

### N2 — `Inputs` / `Outputs` / `Constraints` should stay

`rules.py:52-74` — three identical zero-behaviour classes wrapping `**kwargs` into
`self.specs`, existing only so `command()` can `isinstance`-dispatch on positional
`*declarations`. That dispatch is genuinely clumsy (`rules.py:685-714`: an arity check for
`(2, 3)`, three `isinstance` guards, ordering assumptions, and a
`constraints.pop("input_defaults")` hack).

But: **317 occurrences across 13 files.** The classes are the declaration vocabulary of the
whole test suite and the typing fixtures. The abstraction is thin, but it is load-bearing and
it reads well at the call site. Churning 300+ sites to save ~30 lines is a bad trade.

If the dispatch clumsiness ever needs fixing, add keyword parameters alongside the positional
form rather than replacing it.

---

## Suggested sequence

1. **Y1 — completed in `5e0bbea`.** Fresh per-execution closures fix the leak while retaining
   the incremental optimization.
2. **Y2 — completed.** SHA-256 now streams files in 1 MiB chunks.
3. **Y11 — next priority.** Decide the destructive rule-discovery policy before someone
   loses a node store.
4. **Y5, Y7** — small cleanup commits after checking documentation blast radius.
5. **Y6** — reassess whether the tested `short_names` API should remain before deleting it.
6. **Y4 then Y3** — do `RuleCall.__post_init__` first so `parents` is cached before `Node`
   starts delegating to it.
7. **Y14, Y12** — small, local, independent.
8. **Y8/Y9, Y10** — the two `id()` sites; mechanical but touches rendering and filenames, so
   they want their own commits.
9. Everything else on the scoreboard is discretionary.

Regression tests land in the same commit as each fix, per the project convention.
