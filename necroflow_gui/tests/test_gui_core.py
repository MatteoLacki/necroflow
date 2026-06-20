from pathlib import Path
import time

from necroflow_gui.app import GuiState, _sink_node_ids, _theme_links
from necroflow_gui.example_registry import PIPELINES
from necroflow_gui.graph import build_node_lookup, extract_graph, render_svg
from necroflow_gui.registry import load_pipeline_specs
from necroflow_gui.selection import SelectionMemory


def test_loads_bundled_pipeline_specs():
    specs = load_pipeline_specs()
    assert [spec.id for spec in specs] == ["basic", "diamond", "necroalchemy"]
    assert specs[0].configs[0].id == "sample1"


def test_extracts_basic_pipeline_graph_edges():
    spec = PIPELINES[0]
    config = spec.configs[0]
    pipeline = spec.build(config.values, spec.rules)

    graph = extract_graph(pipeline, {"counts"})

    node_ids = {node.id for node in graph.nodes}
    assert {"fastq", "bam", "align_log", "sorted_bam", "counts", "qc"} <= node_ids
    assert any(node.id == "counts" and node.selected for node in graph.nodes)
    edges = {(edge.source, edge.target) for edge in graph.edges}
    assert ("fastq", "bam") in edges
    assert ("bam", "sorted_bam") in edges
    assert ("sorted_bam", "counts") in edges
    assert ("sorted_bam", "qc") in edges


def test_extracts_necroalchemy_pipeline_graph():
    spec = next(spec for spec in PIPELINES if spec.id == "necroalchemy")
    config = spec.configs[0]
    pipeline = spec.build(config.values, spec.rules)

    graph = extract_graph(pipeline, set())

    node_ids = {node.id for node in graph.nodes}
    assert {"seed", "combined", "stats", "audit", "summary"} <= node_ids
    assert len(graph.nodes) == 17
    assert "summary" in _sink_node_ids(graph)


def test_svg_node_text_is_fitted_inside_boxes():
    spec = PIPELINES[0]
    config = spec.configs[0]
    pipeline = spec.build(config.values, spec.rules)
    graph = extract_graph(pipeline, {"counts"})

    svg = render_svg(graph, spec.id, config.id)

    assert "quantify.counts" in svg
    assert "gene_model=" in svg
    assert "height=\"98\"" in svg


def test_svg_contains_node_and_edge_tooltips_from_info():
    spec = next(spec for spec in PIPELINES if spec.id == "necroalchemy")
    config = spec.configs[0]
    graph = extract_graph(spec.build(config.values, spec.rules), set())

    svg = render_svg(graph, spec.id, config.id)

    assert "<title>seed make_seed.seed" in svg
    assert "Rule: Write the input word to a file" in svg
    assert "seed -&gt; upper" in svg
    assert "Consumes into rule: Convert all characters to uppercase." in svg


def test_node_ids_resolve_after_rebuild():
    spec = PIPELINES[1]
    config = spec.configs[0]
    first = build_node_lookup(spec.build(config.values, spec.rules))
    second = build_node_lookup(spec.build(config.values, spec.rules))

    assert sorted(first) == sorted(second)
    assert "merged" in second
    assert second["merged"].rule.__name__ == "merge_annotations"


def test_sink_nodes_are_default_targets():
    spec = PIPELINES[0]
    config = spec.configs[0]
    graph = extract_graph(spec.build(config.values, spec.rules), set())

    assert _sink_node_ids(graph) == {"align_log", "counts", "qc"}


def test_theme_links_change_theme_immediately():
    links = _theme_links("basic", "sample1", "dark", None)

    assert "theme=light" in links
    assert "theme=dark" in links
    assert "theme-switch" not in links


def test_selection_memory_toggles_nodes():
    memory = SelectionMemory()

    assert memory.toggle("basic", "sample1", "counts") is True
    assert memory.list("basic", "sample1") == {"counts"}
    assert memory.toggle("basic", "sample1", "counts") is False
    assert memory.list("basic", "sample1") == set()


def test_run_selected_targets(tmp_path):
    original = PIPELINES[0]
    spec = type(original)(
        id=original.id,
        label=original.label,
        rules=original.rules,
        build=original.build,
        configs=original.configs,
        outdir=tmp_path,
    )
    state = GuiState([spec])
    config = spec.configs[0]
    state.selection.toggle(spec.id, config.id, "counts")

    run = state.start_run(spec, config)
    deadline = time.time() + 5
    while state.runs[run.id].state in {"queued", "running"} and time.time() < deadline:
        time.sleep(0.05)

    assert state.runs[run.id].state == "succeeded"
    assert list(Path(tmp_path).rglob("counts.txt"))
