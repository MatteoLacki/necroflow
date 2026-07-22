# Callable command and project fingerprint

This example shows how a command can depend on rule config instead of being a
fixed template. The command callback is ordinary Python: it can use `if`
statements, loops, helper functions, or any other normal Python control flow to
decide which command to return. The example also composes Necroflow's default
fingerprint with a project policy identifier.

From this directory, run:

```bash
python -m necroflow.cli --nodes-dir nodes --results-dir results job.toml
cat results/job/sorted/sorted.txt
```

The result is reverse-sorted with duplicates removed:

```text
pear
banana
apple
```

The values in `job.toml` become rule config:

```toml
reverse = true
unique = true
```

`sort_command()` in `pipeline.py` receives those values through the immutable
`CommandArgs`. Normal Python `if` statements inspect `args.config.reverse` and
`args.config.unique` and add `-r` or `-u` to the command only when requested.
The callback also reads the resolved source from `args.inputs` and writes to
the resolved path in `args.outputs`.

Changing either config value changes both the command produced at runtime and
the default fingerprint incorporated by the project policy, so the alternative
result receives its own cache address. The callback returns a complete shell
string and therefore owns shell quoting.

`project_fingerprint()` in `fingerprint.py` receives the logical
`FingerprintArgs` before paths are resolved. It length-frames a project policy
identifier and `default_fingerprint(args)` into a new full SHA-256 digest. This
preserves Necroflow's standard command/config/lineage identity while allowing
the project to invalidate every address by changing `PROJECT_POLICY`.

After execution, inspect:

```text
nodes/sort_text/<hash16>/.rip/dependencies.toml
```

The provenance separates the project fingerprint provider from the realized
shell command.
