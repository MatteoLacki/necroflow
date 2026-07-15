---
name: debug-stale-classification
description: Diagnose why a necroflow node did or did not re-run — NodeState classification, STALE detection, invalidators, forced invalidation. Load when outputs are unexpectedly cached or unexpectedly re-executed.
---

# Debugging necroflow state classification

## First move: ask the tool, not the code

```bash
necroflow explain job.toml            # per-node: state, predicted path, would-run + reason
necroflow explain --node counts job.toml
necroflow doctor job.toml             # preflight issues with stable NF_* codes
```

`explain` reasons include `output_missing`, `up_to_date`, `parent_not_up_to_date`,
`parent_content_changed`, `forced_invalidation`, `invalidator_changed`,
`compromised_prior_state`.

## How classification works (`classify_nodes` in `src/necroflow/dag.py`)

For each node in the required subgraph (requested nodes + ancestors):

1. Output missing → `MISSING`.
2. `.rip/state` contains `running`/`failed`/`interrupted` (compromised prior run) → re-run.
3. Parent check, per parent — mtime fast path, content-hash fallback:
   - parent mtime ≤ node mtime → not newer, skip;
   - parent mtime > node mtime → compare parent's stored `.rip/{filename}.hash` with its
     current content; identical → parent re-ran but output unchanged, skip; different → `STALE`.
4. `NodeType.invalidator` set and stored `.rip/{filename}.invalidation` token missing or
   changed → `STALE`.
5. Otherwise `UP_TO_DATE`. STALE/MISSING propagates to all descendants.

Outside the required subgraph: output exists → `ORPHAN` (deleted by `autoclean`), else `None`.

## Frequent causes of "wrong" caching

- **Different fingerprint, not stale cache.** Any change to command text, config value,
  parent fingerprint, Inputs/Outputs types, or (for string commands) explicit `shellpath`
  yields a NEW output directory — the old one becomes ORPHAN, nothing is "re-run in place".
  Compare `necroflow outputs --json` between the two runs.
- **Constraints don't invalidate.** `threads`/`ram` are excluded from fingerprints by design.
- **External file referenced by a path config value.** The fingerprint hashes the path
  *string*, never the file content. Editing a config/dataset file at the same path does not
  change the fingerprint, and if the path was passed as a bare string with no ingestion node
  at all, nothing ever notices — the node stays `UP_TO_DATE` forever. Countermeasures: for a
  dataset, ingest it with `R.symlink_file(name, OutputType)` so the normal mtime/hash STALE
  machinery covers it (`docs/caching.md#external-dataset-ingestion`) — a `cp`-based import
  does NOT get this for free, since the copy freezes content and nothing revisits the source
  path again. Other options: `NodeType.invalidator` (`examples/custom_invalidation.py`),
  inline config content via `Rules.text_file` (`docs/generated-config-files.md`), or
  `--invalidate LABEL` to force.
- **Crash left `.rip/state` = `running`.** Node is compromised → re-runs even though the
  output exists. That is intended.
- **Parent re-ran, children didn't.** Parent produced byte-identical output — the content-hash
  fallback deliberately stops propagation.

## Forcing re-runs

```bash
necroflow --invalidate LABEL job.toml     # mark node (by pipeline label) + descendants STALE
necroflow --reap NAME job.toml            # label sets from reap.toml
```

Python: `execute(..., forced_stale_keys={node.key})`.

## Inspecting on disk

Everything is a plain file under the node dir: `.rip/state`, `.rip/{filename}.hash`,
`.rip/{filename}.invalidation`, `.rip/dependencies.toml` (accumulated ancestor config),
`.rip/job.log`, `.rip/run.toml`, `.rip/graph.txt` (ancestor render).
