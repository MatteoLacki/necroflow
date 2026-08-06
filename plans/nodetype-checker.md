# NodeType output field-checker hook

## Context

User wants a way to validate the *contents* of a rule's output file once it exists —
e.g. "is this actually valid CSV/TSV" — as a simple post-rule trigger, user-defined per
`NodeType`, with a couple of basic built-ins shipped (CSV/TSV checks). It must be excluded
from fingerprinting entirely (same rationale as `constraints`/`repeat`: validation policy is
not computation identity). The user asked for the simplest YAGNI-compatible design.

necroflow already has a directly analogous hook, `NodeType.invalidator` (staleness token
callback, read lazily via `getattr`, never copied onto `Node`, never touched by
`fingerprints.py`). The new `checker` hook follows the same shape, so this is a small,
additive, low-risk feature — no new `NodeState`, no new `ExecutionEvent` field, no
fingerprint-mechanism changes, no `Node` dataclass changes. Estimated ~150-200 lines total
including docs and tests, across ~6 files plus one new module.

## Design decisions (confirmed with user)

- **Name:** `checker` (mirrors the single-noun `invalidator` sibling on `NodeType`;
  `validator` is avoided because it collides with the existing distinct "config validation
  callbacks" concept in `docs/config-validation.md`).
- **Signature:** `Callable[[Path], None]`, called as `checker(conode.path)`. Only the `Path`
  is passed (not the `Node`) — the stated use case only needs file bytes. Diverges
  deliberately from `invalidator(node)`, which needs the full `Node` for config-derived
  tokens.
- **Failure mode:** raise-to-fail, no custom exception class required — a checker is just a
  normal function that raises whatever built-in exception fits (e.g. `ValueError("row 3 has
  4 columns, expected 3")`).
- **Wrapping:** the executor wraps the checker's exception with output-path context:
  `RuntimeError(f"output failed validation: {conode.path}: {exc}") from exc`. This is a
  deliberate divergence from `invalidator`'s raw/unwrapped propagation, chosen because
  multi-output rules need to show which co-output failed.
- **Eager validation:** none. `Rule._validate_outputs` gets no new check for `checker`,
  matching `invalidator` (which also gets none today). A non-callable `checker` fails
  naturally with `TypeError` the first time the executor calls it.
- **Built-ins:** included now, per explicit request — `is_csv`/`is_tsv` in a new
  `src/necroflow/checkers.py`.

## Implementation

### 1. `src/necroflow/nodes.py`
- Add `checker = None` to `NodeType` right after `invalidator = None` (currently line 60).
- Extend the class docstring (lines 47-56) with one sentence: `checker` receives the output
  `Path` after the file exists and raises to fail; runs once per active output, has no effect
  on fingerprints or caching.
- No change to the `Node` dataclass or `Node.make_outputs` — `checker` is read lazily via
  `getattr(node.node_type, "checker", None)` at the point of use, never copied onto `Node`.

### 2. `src/necroflow/executor.py`
In `_on_job_done` (currently lines 443-458), extend the *existing* missing-output loop rather
than adding a new one — it already walks `node.output_nodes.values()` and checks
`path.exists()`, so the checker call is one more statement per iteration, not a new pass:

```python
for conode in node.output_nodes.values():
    if conode.relative_path in active_keys:
        if not conode.path.exists():
            raise RuntimeError(f"command succeeded but output missing: {conode.path}")
        checker = getattr(conode.node_type, "checker", None)
        if checker is not None:
            try:
                checker(conode.path)
            except Exception as exc:
                raise RuntimeError(
                    f"output failed validation: {conode.path}: {exc}"
                ) from exc
```

**Why this can't just be a wrapper around the command-running function instead:**
`_run_node` (executor.py:796-828, the function that actually runs the command/materializer)
is deliberately not where output validation happens, per its own docstring: "Output
validation and state transitions remain in the parent executor thread." Two concrete reasons
this boundary exists and must hold for `checker` too:
- `_run_node` is pluggable via `execute(..., node_runner=...)` (executor.py:556, 601), which
  can replace it entirely to intercept subprocess execution. A checker call embedded in
  `_run_node` would silently not run for any custom `node_runner`.
- `_run_node` executes inside `_run_with_retries` (executor.py:536-546), which retries on
  `subprocess.CalledProcessError` up to `rule.repeat` times. A checker call there would either
  run once per retry attempt or need special-casing to distinguish checker failures from
  process failures — both add complexity the existing `_on_job_done` boundary avoids for free,
  since it only ever runs once, after the runner's `Future` has already resolved successfully.

So the checker call stays in `_on_job_done`, folded into the existing loop instead of a
separate one.

- Guard condition (`conode.relative_path in active_keys`) is unchanged from today — checkers
  only run for outputs actually required by this `execute()` call.
- No new helper in `dag.py` — unlike `invalidator`, the checker has no role in node
  classification/staleness, so it belongs entirely in `executor.py`.
- Failure propagation needs zero new machinery: the raised `RuntimeError` flows to the
  existing `except Exception as exc:` block (around line 734) and its generic `else` branch
  (759-772), which already sets `node.state = NodeState.FAILED`, calls
  `node.mark_done("failed")`, and records `ExecutionEvent(state="failed", exit_code=None,
  error=str(exc))` — identical to how "command succeeded but output missing" is handled today.

### 3. `src/necroflow/fingerprints.py`
No changes. `_rule_hash` (284-303) and `_parent_identity` (253-281) are explicit allowlists;
`checker` is excluded automatically by never being added to either identity dict, same as
`constraints`/`repeat`/`invalidator` today.

### 4. `src/necroflow/rules.py`
No changes (per the "skip eager validation" decision above).

### 5. `src/necroflow/checkers.py` (new file)
Two small functions built on a shared private helper, stdlib `csv` module only:

```python
from __future__ import annotations
import csv
from pathlib import Path


def _check_delimited(path: Path, delimiter: str, label: str) -> None:
    with path.open(newline="") as stream:
        rows = list(csv.reader(stream, delimiter=delimiter))
    if not rows:
        raise ValueError(f"{label} file is empty: {path}")
    width = len(rows[0])
    for i, row in enumerate(rows):
        if len(row) != width:
            raise ValueError(
                f"{label} file {path} has inconsistent column count: "
                f"row 0 has {width} columns, row {i} has {len(row)}"
            )


def is_csv(path: Path) -> None:
    _check_delimited(path, ",", "CSV")


def is_tsv(path: Path) -> None:
    _check_delimited(path, "\t", "TSV")
```

Scope is deliberately minimal: "parses as rows with a consistent column count," not
schema/type/header validation.

No `__init__.py` export — unlike schedulers (a core `execute(scheduler=...)` argument),
checkers are a niche opt-in utility; a direct import is enough and avoids growing the
top-level `necroflow.*` public API surface for two small helpers. Usage:

```python
from necroflow.checkers import is_csv

class Csv(NodeType):
    filename = "x.csv"
    checker = is_csv
```

No `staticmethod()` wrapping — confirmed by every existing `invalidator` usage
(docs/caching.md:107, examples/custom_invalidation.py:33, tests/test_state.py) that a plain
function assigned as a `NodeType` class attribute is read back unbound via `getattr`, since
`NodeType` subclasses are never instantiated (`NodeTypeMeta.__call__` raises) and Python's
function descriptor only binds `self` on instance access, not class access.

### 6. Documentation
- `docs/rules.md`: new section immediately after the existing "Mutable NodeTypes" section
  (currently ~399-445, before "Multi-output rules" at ~447). Declaration example, note the
  callback receives the output `Path` and must raise to fail, runs once per active output
  after the file is confirmed to exist, pointer to `is_csv`/`is_tsv`.
- `docs/execution.md`: extend "Failure handling" (~97-111), directly after the existing
  "necroflow verifies that the declared output file exists" sentence: "If the output
  `NodeType` defines a `checker`, necroflow also calls it with the output path immediately
  afterward; a raising checker fails the job the same way a missing output does."
- `CLAUDE.md` doc-of-docs table: no new row — the existing "failure handling" row already
  covers this.
- `docs/rule-call-lifecycle.md`: **not updated**, deliberately. This feature touches none of
  the lifecycle stages CLAUDE.md flags (compilation, fingerprint/path derivation, interning,
  label assignment, request selection, execution handoff) — it's a post-success side effect
  strictly after the handoff has already completed, exactly analogous to the neighboring
  "verify output exists" check, which itself required no lifecycle-doc update.

### 7. Tests
- `tests/test_state.py`, following the existing "NodeType invalidators" integration block
  (~275-428):
  - `test_nodetype_checker_runs_after_output_created` — checker records the `Path` it's
    called with; assert called once, file exists with expected content at call time.
  - `test_nodetype_checker_failure_fails_job` — checker raises `ValueError`; assert
    `pytest.raises(RuntimeError, match="output failed validation")` and `node.state ==
    NodeState.FAILED` afterward.
  - `test_multi_output_checker_runs_once_per_output` — two co-outputs each with a distinct
    checker; assert both called.
- `tests/test_fingerprint_v3.py` — `test_checker_does_not_change_rule_hash`, structured as
  the inverse of the existing `test_output_mutability_changes_rule_hash` (~55-71): build a
  rule, set a nontrivial `checker`, assert `rule_hash` is unchanged. This directly locks in
  the "excluded from fingerprinting" requirement rather than relying on the allowlist design
  being correct by inspection alone.
- `tests/test_checkers.py` (new file, ~4 tests, parametrized over delimiter where possible):
  happy path, empty file, ragged row counts, and one cross-delimiter edge case (a comma-only
  file fed to `is_tsv` trivially passes as single-column TSV — documents this is a permissive
  check, not strict schema validation). No separate `not_called_when_command_fails` test:
  when the command itself fails, `f.result()` raises before `_on_job_done` is ever called, so
  the checker is structurally unreachable — already covered by existing failed-command tests,
  not a new failure mode worth a dedicated test.

## Verification
- `pytest tests/test_state.py tests/test_fingerprint_v3.py tests/test_checkers.py -v`
- `pytest` full suite (pre-commit hook runs this on staged Python changes anyway).
- Manual smoke check: write a tiny pipeline with a `text_file` or `command` rule whose output
  `NodeType` sets `checker = staticmethod(is_csv)`, run via `necroflow run`, confirm success
  on valid CSV and a `FAILED` state with the wrapped error message on invalid CSV.
