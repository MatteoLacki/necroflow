# necroflow AI Notes

## Rule command placeholders

Rule commands are validated when a `Rule` is registered. Placeholders are limited to declared input names, declared output names, and built-in command placeholders.

Built-in placeholders:

- `{workdir}` resolves to the rule-call output directory, `outdir/{rule}/{hash16}`. Use it for tools that need to write side directories or scratch files that should be retained with the cached result. The name `workdir` is reserved and cannot be used as an input or output name.

`{workdir}` is created before the command subprocess starts. Its contents are kept by default. With `autoclean=True`, intermediate rule-call directories are removed as whole directories once all active children are up to date, so `{workdir}` side files are cleaned together with declared outputs.

## NodeType invalidators

`NodeType.invalidator` is optional and defaults to `None`. When set, it is a callable receiving the concrete `Node` and returning a stable `str` token. Necroflow stores the token at `.rip/{filename}.invalidation` after a successful run. During classification, an existing output with a missing or changed token is marked `STALE`; callback exceptions fail fast. The token does not participate in the node fingerprint.

## Path limit checks

`resolve_paths()` validates each generated path before assigning `node.path`. It checks component byte lengths against `PC_NAME_MAX` and the full path byte length against `PC_PATH_MAX`, using `os.pathconf()` on the nearest existing parent. Violations raise `ValueError` before execution. Tests monkeypatch `_filesystem_limits()` for deterministic `NAME_MAX` and `PATH_MAX` cases.
