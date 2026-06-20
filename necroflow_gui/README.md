# necroflow_gui

Local Python GUI for selecting and running `necroflow` pipeline targets.

The first version is deliberately Python-only: it serves HTML with the standard library HTTP server and renders the pipeline graph as clickable SVG.

## Run the bundled example

```bash
necroflow-gui serve
```

Open <http://127.0.0.1:8000>, choose a pipeline/config, click nodes to select targets, then run the selected targets.

## Use a project registry

Provide a module or Python file exposing `PIPELINES`:

```bash
necroflow-gui serve path.to.registry:PIPELINES
necroflow-gui serve ./my_registry.py:PIPELINES
```

Each entry should be a `necroflow_gui.registry.PipelineSpec`.
