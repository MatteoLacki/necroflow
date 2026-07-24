# Safe, Length-Bounded Path Labels

## Summary

Allow canonical relative path strings as Pipeline labels:

```python
for dataset, config in combinations:
    P[f"{dataset}/{config}"] = build(P, dataset=dataset, config=config)
```

Requests remain strings:

```toml
".requests" = ["dataset/config"]
```

Labels remain outside fingerprints and cached node identity.

## Label Validation

- Continue accepting only strings; do not add tuple keys.
- Allow multiple `/`-separated components while rejecting absolute paths,
  empty labels, repeated/trailing separators, `.`, `..`, and dot-prefixed
  components.
- Validate encoded byte lengths, not character counts:
  - every component must fit Linux `NAME_MAX` of 255 bytes;
  - the relative label plus output filename must fit Linux `PATH_MAX` of 4096
    bytes.
- Reject invalid labels during Pipeline assignment, before they enter Pipeline
  or DAG label registries.
- Detect file-versus-directory conflicts between labeled result paths during
  assignment.

## Final Filesystem Preflight

- Once the CLI knows `results_dir` and the expanded job label, validate every
  requested absolute result path against the actual filesystem's `PC_NAME_MAX`
  and `PC_PATH_MAX`.
- Perform this preflight before DAG execution so an impossible result path
  cannot run expensive commands first.
- Reuse the existing filesystem-limit logic and check defensively again before
  creating result links.
- Make `doctor` report an explicit result-path error; `run` and `outputs` fail
  with the offending label, encoded length, and applicable limit.
- Validate `.requests` as a list of strings with clear errors for malformed
  metadata.

## Result Representation

- Map `dataset/config` to
  `results/<job>/dataset/config/<filename>`.
- Preserve the exact string in manifests, JSON, CLI selectors, invalidation,
  explain output, `Pipeline.labels`, and `labels_for()`.
- Existing single-component labels retain their current behavior.
- Labels do not enter `FingerprintArgs`; renaming or aliasing them does not
  affect fingerprints, cached node paths, or deduplication.

## Tests and Documentation

- Test loop-generated path labels, lookup, explicit requests, sink selection,
  result links, manifests, JSON, invalidation, and explain.
- Test multibyte labels by encoded byte length.
- Test component and total-path boundaries, actual-filesystem limits,
  pre-execution failure, unsafe paths, malformed requests, and result-path
  conflicts.
- Update rules, job-TOML, lifecycle, feature, and stable-invariant
  documentation.
- Run the complete tests, Black check, and Pyright.

## Assumptions

- `/` is the only hierarchy separator because Necroflow targets POSIX systems.
- Labels are validated, never silently normalized.
- Linux's 255-byte component and 4096-byte path limits are portable early
  ceilings; the destination filesystem may impose stricter limits during CLI
  preflight.
- Existing labels and caches require no migration.
