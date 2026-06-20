# AI Agent Notes

This repository is a single git project at the workspace root. It contains two Python packages, `necroflow` and `necroflow_gui`, but they are not separate git repositories.

## What This Project Is

`necroflow` is a Python pipeline framework inspired by Snakemake. Users define typed `Rules`, call those rules inside Python functions to build a `Pipeline`, then add pipelines to a `DAG`. The important target-selection API is:

```python
dag = DAG(outdir)
dag.add(pipeline, request=[pipeline.some_node])
dag.execute()
```

If `request` is omitted, `DAG.add` uses sink nodes. The GUI should generally pass explicit selected nodes as `request=[...]`.

`necroflow_gui` is a local browser GUI for necroflow. It is intentionally Python-only right now: standard library HTTP server, server-rendered HTML, server-generated SVG, no JavaScript.

## Current Package Boundaries

Core package:

- `necroflow/src/necroflow/dag.py` - node types, nodes, rules, path resolution, command resolution, cache/provenance helpers.
- `necroflow/src/necroflow/pipeline.py` - `Pipeline`, `DAG`, graph rendering/save behavior.
- `necroflow/src/necroflow/executor.py` - execution, schedulers, thread budgets, failure behavior.
- `necroflow/src/necroflow/state_db.py` - persistent run state under `outdir/.rip/state.db`.
- `necroflow/src/necroflow/logger.py` - run logging and job log paths.

GUI package:

- `necroflow_gui/src/necroflow_gui/app.py` - HTTP server, routes, page rendering, theme links, default sink selection, run launching.
- `necroflow_gui/src/necroflow_gui/graph.py` - stable GUI node IDs, graph extraction, SVG rendering.
- `necroflow_gui/src/necroflow_gui/registry.py` - `PipelineSpec`, `PipelineConfig`, registry loading.
- `necroflow_gui/src/necroflow_gui/selection.py` - process-local selection memory.
- `necroflow_gui/src/necroflow_gui/example_registry.py` - bundled examples, including `necroflow/examples/necroalchemy.py` loaded by file path.

## GUI Behavior To Preserve

- No JavaScript dependency unless the user explicitly asks to change that design.
- Dark theme is default.
- Theme switching is immediate via normal links, not a select that requires clicking Open.
- First page load for a pipeline/config defaults selected targets to graph sinks: visible nodes with no children.
- Once a user toggles any node, preserve that explicit selection in process memory, even if it becomes empty.
- Node IDs must be stable across pipeline rebuilds. They come from `Pipeline` attribute names, including tuple outputs such as `stats.audit` when possible.
- Runs rebuild the pipeline and map selected GUI IDs back to live `Node` objects, then call `DAG.add(..., request=[...])`.
- The GUI uses `fifo_scheduler` to avoid requiring `networkx` for the local GUI run path.

## Important Gotchas

- The repo root may have untracked package directories. Check `git status --short` from the root.
- Do not assume `pytest` is installed. Direct smoke checks with `PYTHONPATH=necroflow/src:necroflow_gui/src python3 -c ...` have been used when needed.
- `necroflow_gui/example_registry.py` loads `necroflow/examples/necroalchemy.py` relative to the repo root. If the layout changes, update that path.
- Avoid committing `__pycache__`, result directories, or `/tmp` outputs.
- The local GUI server may be running in a tool session. Restart it after code changes before HTTP checks.

## Useful Commands

```bash
PYTHONPATH=necroflow/src:necroflow_gui/src python3 -m necroflow_gui.cli.main serve
python3 -m py_compile necroflow_gui/src/necroflow_gui/app.py necroflow_gui/src/necroflow_gui/graph.py
PYTHONPATH=necroflow/src:necroflow_gui/src python3 -c "from necroflow_gui.registry import load_pipeline_specs; print([s.id for s in load_pipeline_specs()])"
```

## Current Design Intent

Keep the implementation small and inspectable. The goal is not a general web platform; it is a local control surface for plotting necroflow pipelines, selecting target nodes, and launching the exact requested DAG.
