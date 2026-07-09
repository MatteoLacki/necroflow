# Release Checklist

[Back to README](../README.md)

1. Update `__version__` in `src/necroflow/__init__.py`.
2. Run the full test suite:

```bash
.venv/bin/pytest -q
```

3. Build the package:

```bash
python -m build
```

4. Check package metadata:

```bash
twine check dist/*
```

5. Verify template files are present in the wheel or installed package.
6. Upload to the selected package index.
