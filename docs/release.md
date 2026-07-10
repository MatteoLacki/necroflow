# Release Checklist

[Previous: Execution, Scheduling, and Cleanup](execution.md) | [README](../README.md) | [Next: Development](development.md)

1. Build, check, and upload a new version to PyPI:

```bash
make release_pypi 0.0.4
```

The explicit `VERSION=` form is also supported:

```bash
make release_pypi VERSION=0.0.4
```

When a version is provided, the release target first updates `__version__` in
`src/necroflow/__init__.py`, then runs the full test suite, removes old artifacts
from `dist/`, builds fresh source and wheel distributions, runs `twine check`,
and uploads the checked artifacts to PyPI.

If `__version__` was already updated, omit the version:

```bash
make release_pypi
```

For TestPyPI, use:

```bash
make release_test_pypi 0.0.4
```

To update only the version without uploading, use:

```bash
make bump-version 0.0.4
```

To create the version tag after the release commit is clean, use:

```bash
make tag-release
```

The tag defaults to `v<provided-version>` when a version is provided, otherwise
to `v<necroflow.__version__>`. It can be overridden with `TAG=vX.Y.Z`.

Manual equivalent:

```bash
.venv/bin/pytest -q
.venv/bin/python -m build
.venv/bin/twine check dist/*
.venv/bin/twine upload dist/*
```

2. Verify template files are present in the wheel or installed package.

[Previous: Execution, Scheduling, and Cleanup](execution.md) | [README](../README.md) | [Next: Development](development.md)
