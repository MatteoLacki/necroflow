# Canonical necroflow workflow

This is the small reference workflow for the recommended CLI-first project shape.

```bash
necroflow --nodes-dir nodes --results-dir results --validation schema.py:validate job.toml
```

Try a parameter grid:

```bash
necroflow --nodes-dir nodes --results-dir results --validation schema.py:validate job_grid.toml
```

Inspect without running:

```bash
necroflow graph job.toml
necroflow outputs job.toml
```

Create a new copy of this workflow elsewhere with:

```bash
necroflow init my-workflow
```
