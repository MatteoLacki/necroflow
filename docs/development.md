# Development

[Previous: Release Checklist](release.md) | [README](../README.md) | [Next: README](../README.md)

Use the local development environment for tests, builds, and releases:

```bash
make venv
```

This creates `.venv` with the package installed in editable mode and the `dev`
extra installed.

## Tests

Run the test suite with:

```bash
make test
```

The repository also has a pre-commit hook in `.githooks/pre-commit` when
`core.hooksPath` is configured. It runs Black over the Python codebase and then
runs `pytest` before allowing a commit.

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

[Previous: Release Checklist](release.md) | [README](../README.md) | [Next: README](../README.md)
