# Doctor: Preflight Checks

[Previous: Development](development.md) | [README](../README.md) | [Next: README](../README.md)

`necroflow doctor` is a preflight compiler and environment check. It answers:

> Can Necroflow understand and prepare this job sufficiently to begin
> execution?

It does not promise that every command will successfully complete.

```bash
necroflow doctor job.toml
necroflow doctor --json job.toml
```

## What Doctor checks

Doctor follows the same configuration and Pipeline-compilation path as a real
run:

1. It validates CLI configuration, including the shell path, resource caps,
   and constraint syntax.
2. It loads job TOMLs, resolves table inheritance, and expands `__grid`
   configurations.
3. It runs configured validation callbacks.
4. It imports and invokes each Pipeline factory. Rule calls are fingerprinted
   and interned into the shared DAG.
5. It resolves requested outputs and forced invalidations.
6. It performs structural diagnostics on each compiled Pipeline.
7. It validates result paths against the destination filesystem.
8. It checks that the node and result roots are writable and that the node
   store is not locked by another run.

Doctor reports findings with stable `NF_*` codes. Text output is intended for
people; `--json` exposes the same findings for scripts and CI.

## Errors and informational findings

Error findings make `"ok"` false and cause exit status 1. Examples include:

- `NF_CONFIG_MISSING_PIPELINE`
- `NF_PIPELINE_IMPORT_FAILED`
- `NF_CONFIG_PARSE_FAILED`
- `NF_VALIDATION_FAILED`
- `NF_REQUEST_LABEL_NOT_FOUND`
- `NF_SHELLPATH_INVALID`
- `NF_RESOURCE_INVALID`
- `NF_RESULT_PATH_INVALID`
- `NF_OUTPUT_ROOT_NOT_WRITABLE`
- `NF_NODESTORE_LOCKED`

Informational findings are printed but keep `"ok": true` and exit status 0.
`NF_MULTIPLE_LABELS` reports when several labels in one Pipeline refer to the
same canonical Node:

```python
P.result = calculate(P)
P.alternative_name = P.result
```

The finding includes the expanded job name, all Pipeline-local labels, and the
canonical relative node path. Pipelines from different expanded configurations
are checked independently: different configurations using different labels for
one shared DAG Node are not treated as aliases.

Doctor can identify that labels are aliases, but it cannot determine whether
the alias was deliberate. The finding is therefore informational rather than
an error.

## Execution boundary and side effects

Doctor never executes rule commands, submits scheduler jobs, creates declared
outputs, creates result symlinks, or changes node run-state files.

It is not a static TOML linter, however. Pipeline factories are ordinary Python
and must run to construct the DAG. Doctor also invokes configured validation
and fingerprint callbacks. Those callbacks can have their own side effects.

To verify destination writability, Doctor creates missing node and result root
directories, writes a temporary probe file in each, and immediately removes
the probe. It also attempts a non-blocking lock on an existing node-store lock
file.

## What Doctor does not guarantee

A successful Doctor result means that Necroflow compiled the requested jobs and
passed its deterministic preflight checks. It does not guarantee:

- that external programs referenced by commands are installed or compatible;
- that runtime inputs remain available;
- that commands exit successfully;
- that successful commands create every declared output;
- that sufficient resources will remain available during execution; or
- that outputs are currently cached or stale.

Use [`necroflow explain`](cli.md) to inspect cache state and why individual Nodes would or would not run. Use a dry run when command
realization and scheduling order are the relevant questions.

[Previous: Development](development.md) | [README](../README.md) | [Next: README](../README.md)
