---
name: debug-stale-classification
description: Diagnose why a necroflow RuleCall did or did not rerun using consumed hashes, invalidators, and forced invalidation.
---

# Debugging necroflow cache classification

## Ask live tools first

```bash
necroflow explain job.toml
necroflow explain --node counts job.toml
necroflow explain --json job.toml
necroflow doctor job.toml
```

Explain reports RuleCalls in registration order with nested output Nodes. Reasons include `output_missing`, `up_to_date`, `parent_will_run`, `consumed_hash_missing`, `consumed_hash_other_hasher`, `parent_content_changed`, `parent_hash_unchecked`, `parent_undecided`, `dependency_metadata_missing_or_invalid`, `dependency_metadata_mismatch`, `forced_invalidation`, `invalidator_changed`, and `compromised_prior_state`.

## Classification model

`planning.py` classifies canonical RuleCalls, not individual output Nodes.

1. Request closure starts from selected Nodes, then follows owning calls and `parent_calls`.
2. Any missing declared co-output makes complete call `MISSING`.
3. Non-`up_to_date` persisted state, forced call key, or changed output invalidator makes call `STALE`.
4. A parent scheduled for execution also marks active descendants for replay by default. Every child still waits until all parents settle successfully.
5. For every parent Node, compare recorded `dependencies.toml: consumed_hash` with current bytes.
6. Missing or malformed consumed metadata is stale. Matching hashes preserve a cache when parents did not rerun, or when `reuse_unchanged=True` explicitly allows reuse after rebuilding them.
7. Existing inactive call workdirs are listed in `plan.orphans`; they never run.
8. Verdicts and reasons are in `plan.verdicts` / `plan.reasons` from `plan_execution(dag)`, not on the RuleCall.

Current hashes trust `.rip/{filename}.hash` only while output mtime is no newer than hash-file mtime. Newer outputs are rehashed. External edits preserving or backdating mtime are unsupported.

## Frequent causes

- Different identity: command, config, contracts, shell path, or parent lineage changed. New path is missing; old path may be orphan.
- Missing sibling: one absent co-output makes atomic call missing.
- Missing consumed hash: legacy or malformed dependency metadata forces safe replay.
- Parent rebuilt, child cached: `--reuse-unchanged` was enabled and output bytes were identical.
- Parent rebuilt, child replayed: default cascading policy, changed output bytes, or another child cache check required replay.
- Bare external path string: fingerprint includes path text, not external bytes. Ingest with `@symlink_file`, use an invalidator, or include content in config.
- Leftover `.rip/state = running`: compromised call reruns (a crash or kill; a failure elsewhere no longer leaves running siblings unrecorded).
- Constraints and `repeat`: execution policy, excluded from identity.

## Force replay

```bash
necroflow run --invalidate LABEL job.toml
necroflow run --reap NAME job.toml
```

Python:

```python
dag.run(forced_stale_call_keys={node.rule_call.relative_path})
```

Forced labels do not add requirements.

## Inspect metadata

```text
<workdir>/.rip/state
<workdir>/.rip/dependencies.toml
<workdir>/.rip/{filename}.hash
<workdir>/.rip/{filename}.invalidation
<workdir>/.rip/job.log
<workdir>/.rip/run.toml
<workdir>/.rip/graph.tgf
```
