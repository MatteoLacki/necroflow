from __future__ import annotations

import html
import threading
import traceback
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from necroflow import DAG, fifo_scheduler

from necroflow_gui.graph import build_node_lookup, extract_graph, render_svg
from necroflow_gui.registry import PipelineConfig, PipelineSpec, load_pipeline_specs
from necroflow_gui.selection import SelectionMemory


@dataclass
class RunStatus:
    id: str
    pipeline_id: str
    config_id: str
    state: str
    selected: list[str]
    error: str | None = None


class GuiState:
    def __init__(self, specs: list[PipelineSpec]):
        self.specs = specs
        self.specs_by_id = {spec.id: spec for spec in specs}
        self.selection = SelectionMemory()
        self.runs: dict[str, RunStatus] = {}
        self.lock = threading.Lock()

    def default_pipeline_id(self) -> str:
        return self.specs[0].id

    def default_config_id(self, spec: PipelineSpec) -> str:
        return spec.configs[0].id

    def get_spec(self, pipeline_id: str | None) -> PipelineSpec:
        if pipeline_id and pipeline_id in self.specs_by_id:
            return self.specs_by_id[pipeline_id]
        return self.specs_by_id[self.default_pipeline_id()]

    def get_config(self, spec: PipelineSpec, config_id: str | None) -> PipelineConfig:
        for config in spec.configs:
            if config.id == config_id:
                return config
        return spec.configs[0]

    def start_run(self, spec: PipelineSpec, config: PipelineConfig) -> RunStatus:
        selected = sorted(self.selection.list(spec.id, config.id))
        run_id = uuid.uuid4().hex[:10]
        status = RunStatus(
            id=run_id,
            pipeline_id=spec.id,
            config_id=config.id,
            state="queued",
            selected=selected,
        )
        with self.lock:
            self.runs[run_id] = status
        thread = threading.Thread(
            target=self._run_pipeline,
            args=(run_id, spec, config, selected),
            daemon=True,
        )
        thread.start()
        return status

    def _run_pipeline(
        self,
        run_id: str,
        spec: PipelineSpec,
        config: PipelineConfig,
        selected: list[str],
    ) -> None:
        with self.lock:
            self.runs[run_id].state = "running"
        try:
            if not selected:
                raise ValueError("Select at least one target node before running.")
            pipeline = spec.build(config.values, spec.rules)
            lookup = build_node_lookup(pipeline)
            missing = [node_id for node_id in selected if node_id not in lookup]
            if missing:
                raise ValueError(f"Selected nodes no longer exist: {', '.join(missing)}")
            dag = DAG(spec.outdir)
            dag.add(pipeline, request=[lookup[node_id] for node_id in selected])
            dag.execute(scheduler=fifo_scheduler)
        except Exception:
            with self.lock:
                self.runs[run_id].state = "failed"
                self.runs[run_id].error = traceback.format_exc()
            return
        with self.lock:
            self.runs[run_id].state = "succeeded"


def create_handler(state: GuiState) -> type[BaseHTTPRequestHandler]:
    class NecroflowGuiHandler(BaseHTTPRequestHandler):
        server_version = "necroflow-gui/0.0.1"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._handle_index(parsed.query)
            elif parsed.path == "/toggle":
                self._handle_toggle(parsed.query)
            elif parsed.path == "/style.css":
                self._send_css()
            else:
                self.send_error(HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/run":
                self._handle_run()
            else:
                self.send_error(HTTPStatus.NOT_FOUND)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _handle_index(self, query: str) -> None:
            params = _params(query)
            spec = state.get_spec(params.get("pipeline_id"))
            config = state.get_config(spec, params.get("config_id"))
            theme = _theme(params.get("theme"))
            pipeline = spec.build(config.values, spec.rules)
            if not state.selection.has(spec.id, config.id):
                initial_graph = extract_graph(pipeline, set())
                state.selection.replace(spec.id, config.id, _sink_node_ids(initial_graph))
            selected = state.selection.list(spec.id, config.id)
            graph = extract_graph(pipeline, selected)
            svg = render_svg(graph, spec.id, config.id)
            body = _render_page(
                state, spec, config, graph, svg, params.get("run_id"), theme
            )
            self._send_html(body)

        def _handle_toggle(self, query: str) -> None:
            params = _params(query)
            spec = state.get_spec(params.get("pipeline_id"))
            config = state.get_config(spec, params.get("config_id"))
            theme = _theme(params.get("theme"))
            node_id = params.get("node_id")
            if node_id:
                pipeline = spec.build(config.values, spec.rules)
                if node_id in build_node_lookup(pipeline):
                    state.selection.toggle(spec.id, config.id, node_id)
            self._redirect(_index_url(spec.id, config.id, theme=theme))

        def _handle_run(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(length).decode()
            params = _params(payload)
            spec = state.get_spec(params.get("pipeline_id"))
            config = state.get_config(spec, params.get("config_id"))
            theme = _theme(params.get("theme"))
            run = state.start_run(spec, config)
            self._redirect(_index_url(spec.id, config.id, run.id, theme))

        def _send_html(self, body: str) -> None:
            data = body.encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_css(self) -> None:
            data = CSS.encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/css; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _redirect(self, location: str) -> None:
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", location)
            self.end_headers()

    return NecroflowGuiHandler


def serve(
    target: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    state = GuiState(load_pipeline_specs(target))
    server = ThreadingHTTPServer((host, port), create_handler(state))
    url = f"http://{host}:{port}/"
    print(f"Serving necroflow_gui at {url}")
    server.serve_forever()


def _render_page(
    state: GuiState,
    active_spec: PipelineSpec,
    active_config: PipelineConfig,
    graph,
    svg: str,
    run_id: str | None,
    theme: str,
) -> str:
    run = state.runs.get(run_id or "")
    selected = [node for node in graph.nodes if node.selected]
    pipeline_options = "\n".join(
        _option(spec.id, spec.label, spec.id == active_spec.id) for spec in state.specs
    )
    config_options = "\n".join(
        _option(config.id, config.label, config.id == active_config.id)
        for config in active_spec.configs
    )
    theme_links = _theme_links(active_spec.id, active_config.id, theme, run_id)
    selected_items = "\n".join(
        f"<li>{html.escape(node.label)}</li>" for node in selected
    ) or "<li>No target nodes selected.</li>"
    run_html = _render_run(run)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>necroflow_gui</title>
  <link rel="stylesheet" href="/style.css">
</head>
<body class="theme-{html.escape(theme)}">
  <header>
    <h1>necroflow_gui</h1>
    <div class="toolbar">
      <form method="get" action="/" class="selectors">
        <input type="hidden" name="theme" value="{html.escape(theme)}">
        <label>Pipeline <select name="pipeline_id">{pipeline_options}</select></label>
        <label>Config <select name="config_id">{config_options}</select></label>
        <button type="submit">Open</button>
      </form>
      <nav class="theme-switch" aria-label="Theme">{theme_links}</nav>
    </div>
  </header>
  <main>
    <section class="graph-wrap">
      {svg}
    </section>
    <aside>
      <h2>Selected targets</h2>
      <ul>{selected_items}</ul>
      <form method="post" action="/run">
        <input type="hidden" name="pipeline_id" value="{html.escape(active_spec.id)}">
        <input type="hidden" name="config_id" value="{html.escape(active_config.id)}">
        <input type="hidden" name="theme" value="{html.escape(theme)}">
        <button type="submit">Run selected targets</button>
      </form>
      {run_html}
    </aside>
  </main>
</body>
</html>"""


def _sink_node_ids(graph) -> set[str]:
    parents = {edge.source for edge in graph.edges}
    return {node.id for node in graph.nodes if node.id not in parents}


def _render_run(run: RunStatus | None) -> str:
    if run is None:
        return ""
    selected = ", ".join(run.selected) or "none"
    error = ""
    if run.error:
        error = f"<pre>{html.escape(run.error)}</pre>"
    return (
        "<section class=\"run-status\">"
        f"<h2>Run {html.escape(run.id)}</h2>"
        f"<p>Status: <strong>{html.escape(run.state)}</strong></p>"
        f"<p>Targets: {html.escape(selected)}</p>"
        f"{error}</section>"
    )


def _option(value: str, label: str, selected: bool) -> str:
    attr = " selected" if selected else ""
    return f'<option value="{html.escape(value)}"{attr}>{html.escape(label)}</option>'


def _theme_links(
    pipeline_id: str, config_id: str, active_theme: str, run_id: str | None
) -> str:
    links = []
    for value, label in (("dark", "Dark"), ("light", "Light")):
        cls = "active" if value == active_theme else ""
        href = _index_url(pipeline_id, config_id, run_id, value)
        links.append(
            f'<a class="{cls}" href="{html.escape(href)}">{html.escape(label)}</a>'
        )
    return "".join(links)


def _params(query: str) -> dict[str, str]:
    parsed = parse_qs(query)
    return {key: values[-1] for key, values in parsed.items() if values}


def _theme(value: str | None) -> str:
    return value if value in {"dark", "light"} else "dark"


def _index_url(
    pipeline_id: str,
    config_id: str,
    run_id: str | None = None,
    theme: str = "dark",
) -> str:
    params = {
        "pipeline_id": pipeline_id,
        "config_id": config_id,
        "theme": _theme(theme),
    }
    if run_id:
        params["run_id"] = run_id
    return "/?" + urlencode(params)


CSS = """
:root {
  color-scheme: dark;
  --ink: #e8edf7;
  --muted: #9da8ba;
  --line: #2b3547;
  --panel: #151d2b;
  --panel-2: #101827;
  --bg: #0b101a;
  --control: #0f1724;
  --blue: #62a8ff;
  --orange: #f6a23a;
  --button-bg: #e8edf7;
  --button-ink: #101827;
  --node-fill: #121c2d;
  --node-stroke: #42546f;
  --node-hover: #182842;
  --node-selected: #3a2813;
  --node-selected-stroke: #f6a23a;
}
.theme-light {
  color-scheme: light;
  --ink: #172033;
  --muted: #687386;
  --line: #d8dee8;
  --panel: #ffffff;
  --panel-2: #f8fafc;
  --bg: #f5f7fb;
  --control: #ffffff;
  --blue: #2b6cb0;
  --orange: #d97706;
  --button-bg: #172033;
  --button-ink: #ffffff;
  --node-fill: #f9fbff;
  --node-stroke: #9fb1c8;
  --node-hover: #edf5ff;
  --node-selected: #fff7ed;
  --node-selected-stroke: #d97706;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: var(--ink);
  background: var(--bg);
}
header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  padding: 18px 24px;
  border-bottom: 1px solid var(--line);
  background: var(--panel);
}
h1 { font-size: 22px; margin: 0; }
h2 { font-size: 16px; margin: 0 0 12px; }
.toolbar { display: flex; gap: 12px; align-items: end; flex-wrap: wrap; justify-content: flex-end; }
.selectors { display: flex; gap: 12px; align-items: end; flex-wrap: wrap; }
.theme-switch {
  display: inline-flex;
  min-height: 36px;
  border: 1px solid var(--line);
  border-radius: 6px;
  overflow: hidden;
  background: var(--control);
}
.theme-switch a {
  display: inline-flex;
  align-items: center;
  padding: 0 12px;
  color: var(--muted);
  text-decoration: none;
  border-left: 1px solid var(--line);
}
.theme-switch a:first-child { border-left: 0; }
.theme-switch a.active {
  background: var(--button-bg);
  color: var(--button-ink);
}
label { display: grid; gap: 5px; color: var(--muted); font-size: 13px; }
select, button {
  min-height: 36px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--control);
  color: var(--ink);
  padding: 0 10px;
  font: inherit;
}
button {
  background: var(--button-bg);
  color: var(--button-ink);
  border-color: var(--button-bg);
  cursor: pointer;
}
main {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  gap: 20px;
  padding: 20px;
}
.graph-wrap, aside {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
}
.graph-wrap {
  overflow: auto;
  min-height: 560px;
}
aside {
  padding: 18px;
  align-self: start;
}
ul { padding-left: 18px; color: var(--muted); }
li { margin: 8px 0; }
.pipeline-graph {
  min-width: 900px;
  width: 100%;
  height: auto;
}
.edge-wrap { cursor: help; }
.edge-hit {
  fill: none;
  stroke: transparent;
  stroke-width: 16;
  pointer-events: stroke;
}
.edge {
  fill: none;
  stroke: #8b98aa;
  stroke-width: 2;
  pointer-events: none;
}
.edge-wrap:hover .edge {
  stroke: var(--blue);
  stroke-width: 3;
}
.node { cursor: help; }
.node rect {
  fill: var(--node-fill);
  stroke: var(--node-stroke);
  stroke-width: 1.5;
  transition: fill 120ms ease, stroke 120ms ease;
}
.node:hover rect {
  fill: var(--node-hover);
  stroke: var(--blue);
}
.node.selected rect {
  fill: var(--node-selected);
  stroke: var(--node-selected-stroke);
  stroke-width: 2.5;
}
.node-label {
  fill: var(--ink);
  font-size: 13px;
  font-weight: 700;
}
.node-detail {
  fill: var(--muted);
  font-size: 12px;
}
.run-status {
  margin-top: 20px;
  border-top: 1px solid var(--line);
  padding-top: 16px;
}
pre {
  max-height: 280px;
  overflow: auto;
  padding: 12px;
  background: var(--panel-2);
  color: var(--ink);
  border: 1px solid var(--line);
  border-radius: 6px;
  font-size: 12px;
}
@media (max-width: 900px) {
  header { align-items: stretch; flex-direction: column; }
  main { grid-template-columns: 1fr; }
}
"""
