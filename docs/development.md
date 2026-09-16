# Development

[Previous: Release Checklist](release.md) | [README](../README.md) | [Next: Doctor](doctor.md)

Use the local development environment for tests, builds, and releases:

```bash
make venv
```

This creates `.venv` with the package installed in editable mode and the `dev`
extra installed.

## Keep the tomlkit dependency

Decision (2026-09-16): retain `tomlkit`. Do not propose removing or replacing it
solely to reduce dependency count. Revisit only for a concrete problem, such as
a measured performance issue, an upstream maintenance problem, or an explicit
requirement for zero third-party runtime dependencies.

At review time, `tomlkit` is Necroflow's only required third-party runtime
dependency. Upstream `tomlkit` 0.15.1 has no third-party runtime dependencies of
its own and requires Python >=3.9. Its source build uses `poetry-core`; its
development and test dependencies are not runtime requirements. See
[upstream package metadata](https://raw.githubusercontent.com/python-poetry/tomlkit/master/pyproject.toml).

Necroflow already requires Python >=3.11, so reads could use the standard
library's `tomllib`. However, [tomllib does not write TOML](https://docs.python.org/3/library/tomllib.html).
The dependency also supports behavior that a parser swap would not preserve:

- Metadata and report writers generate `dependencies.toml`, `run.toml`,
  `execution.toml`, and `manifest.toml`.
- `src/necroflow/grid.py` distinguishes arrays of tables (`[[table]]`) from
  lists of inline tables using `tomlkit.items.AoT`. `tomllib` represents both as
  lists of dictionaries, so a replacement must resolve that semantic difference
  without silently changing grid expansion. Serialization-based copies could
  instead use `copy.deepcopy`.
- `src/necroflow/tools/config_set.py` edits TOML while preserving comments and
  formatting. Parsing into plain dictionaries and serializing again would lose
  that behavior.

The source inspection covered eight source modules and six test files using
`tomlkit`. Rough effort estimates, including adapting tests, were half to one
developer-day to replace it with `tomllib` plus another TOML writer, or one to
three developer-days to remove runtime dependencies by maintaining our own
writer, assuming loss of config comments and formatting is acceptable.
Preserving those as well would be substantially more work. These are planning
estimates, not results from an implemented migration.

Keeping one dependency with no transitive runtime dependencies is preferable to
owning TOML serialization edge cases and compatibility work without a concrete
benefit.

## Tests

Run the test suite with:

```bash
make test
```

The repository also has a pre-commit hook in `.githooks/pre-commit` when
`core.hooksPath` is configured. When staged changes include Python files, it
runs Black over existing changed Python paths and then runs the full
`pytest` suite before allowing a commit. Commits without staged Python changes skip both.

## Release Builds

Build and check the package without uploading:

```bash
make check-dist
```

This removes old build artifacts, builds a fresh source distribution and wheel,
and runs `twine check dist/*`.

## PyPI Releases

Release a new version to PyPI in one command:

```bash
make release_pypi 0.0.4
```

The positional version is used to update `src/necroflow/__init__.py` before the
release is built. The same command can also be written with an explicit Make
variable:

```bash
make release_pypi VERSION=0.0.4
```

Both forms run the same flow: update `__version__` when a version is provided,
run tests, clean and rebuild `dist/`, check the artifacts with Twine, and upload
to PyPI.

If `__version__` has already been updated, omit the version:

```bash
make release_pypi
```

Use TestPyPI with the matching target:

```bash
make release_test_pypi 0.0.4
```

Update only `__version__` without building or uploading:

```bash
make bump-version 0.0.4
```

Create the release tag after the release commit is clean:

```bash
make tag-release
```

The default tag is `v<provided-version>` when a version is passed on the Make
command line, otherwise `v<necroflow.__version__>`. Override it with
`TAG=vX.Y.Z` when needed.

[Previous: Release Checklist](release.md) | [README](../README.md) | [Next: Doctor](doctor.md)
