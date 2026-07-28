"""Tests for Pipeline and DAG."""

import necroflow.keywords as pipeline_keywords
from necroflow.rules import Constraints, Inputs, Outputs, Rule

import pytest
from pathlib import Path
from necroflow import (
    DAG,
    NodeType,
    Pipeline,
    command,
    iter_connected_components,
    output,
)


class A(NodeType):
    filename = "a.txt"


class B(NodeType):
    filename = "b.txt"


class C(NodeType):
    filename = "c.txt"


class D(NodeType):
    filename = "d.txt"


class E(NodeType):
    filename = "e.txt"


R_make_a = Rule("make_a", Inputs(x=str), Outputs(a=A), "touch {a}")
R_make_b = Rule("make_b", Inputs(a=A), Outputs(b=B), "touch {b}")
R_make_c = Rule("make_c", Inputs(a=A), Outputs(c=C), "touch {c}")
R_make_d = Rule("make_d", Inputs(b=B, c=C), Outputs(d=D), "touch {d}")
R_make_c_from_b = Rule("make_c_from_b", Inputs(b=B), Outputs(c=C), "touch {c}")
R_make_e_from_ac = Rule("make_e_from_ac", Inputs(a=A, c=C), Outputs(e=E), "touch {e}")
TEST_NODES_DIR = "/tmp/necroflow-test-pipeline"


def diamond(owner=TEST_NODES_DIR):
    """A → B, A → C, (B,C) → D"""
    dag = owner if isinstance(owner, DAG) else DAG(owner)
    P = Pipeline(dag)
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c(P, P.a)
    P.d = R_make_d(P, P.b, P.c)
    return P


# ── Pipeline.sinks ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────


def test_sinks_source_node():
    # single node with no parents and no children — must be a sink
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    assert P.sinks() == [P.a]


def test_sinks_linear():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    assert P.sinks() == [P.b]


def test_sinks_diamond():
    P = diamond()
    assert P.sinks() == [P.d]


def test_sinks_multiple():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c(P, P.a)
    # b and c are both sinks (nothing depends on them)
    assert set(id(n) for n in P.sinks()) == {id(P.b), id(P.c)}


def test_sinks_excludes_intermediate():
    P = diamond()
    sinks = P.sinks()
    assert P.a not in sinks
    assert P.b not in sinks
    assert P.c not in sinks


# ── Pipeline attribute assignment ─────────────────────────────────────────────


def test_pipeline_nodes_accumulate():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    assert len(P.nodes) == 2


def test_pipeline_dot_prefix_raises():
    """Label starting with '.' must raise — reserved for .rip internal folder."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    with pytest.raises(ValueError, match=r"must not start with '\.'"):
        setattr(P, ".hidden", R_make_a(P, x="x"))


def test_pipeline_duplicate_raises():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    with pytest.raises(ValueError):
        P.a = R_make_a(P, x="y")


def test_pipeline_single_label():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    assert P.labels_for(P.a) == ("a",)


def test_pipeline_item_and_attribute_labels_share_the_same_namespace():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["source"] = R_make_a(P, x="x")
    P.result = R_make_b(P, P["source"])

    assert P.source is P["source"]
    assert P["result"] is P.result
    assert P.labels_for(P.source) == ("source",)
    assert P.labels_for(P.result) == ("result",)


def test_equivalent_calls_can_have_multiple_pipeline_aliases():
    dag = DAG(TEST_NODES_DIR)
    P = Pipeline(dag)
    P.first = R_make_a(P, x="same")
    P.alias = R_make_a(P, x="same")

    assert P.first is P.alias
    assert P.labels_for(P.first) == ("first", "alias")
    assert P.nodes == [P.first]
    assert len(dag.calls) == 1


def test_pipeline_item_labels_support_non_identifiers():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["sample-1 raw"] = R_make_a(P, x="x")

    assert P.labels_for(P["sample-1 raw"]) == ("sample-1 raw",)


def test_pipeline_item_labels_support_canonical_relative_paths():
    P = Pipeline(DAG(TEST_NODES_DIR))
    labels = [
        f"{dataset}/{config}"
        for dataset in ("sample-1", "sample-2")
        for config in ("strict", "relaxed")
    ]

    for label in labels:
        P[label] = R_make_a(P, x=label)

    assert P.labels == tuple(labels)
    assert all(P.labels_for(P[label]) == (label,) for label in labels)
    assert [node.relative_path for node in P.sinks()] == [
        P[label].relative_path for label in labels
    ]


@pytest.mark.parametrize(
    "label",
    [
        "/absolute",
        "../outside",
        "dataset/../outside",
        "dataset/./config",
        "dataset//config",
        "dataset/config/",
        "dataset/.hidden",
        "dataset/\0bad",
    ],
)
def test_pipeline_item_labels_reject_unsafe_or_noncanonical_paths(label):
    P = Pipeline(DAG(TEST_NODES_DIR))

    with pytest.raises(ValueError):
        P[label] = R_make_a(P, x="x")


def test_pipeline_label_component_limits_use_encoded_bytes():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["€" * 85] = R_make_a(P, x="fits")

    with pytest.raises(ValueError, match=r"258 > NAME_MAX 255 bytes"):
        P["€" * 86] = R_make_a(P, x="too-long")


def test_pipeline_label_rejects_portably_overlong_result_path():
    P = Pipeline(DAG(TEST_NODES_DIR))
    exact_limit = "/".join(["x" * 255] * 15 + ["x" * 250])
    P[exact_limit] = R_make_a(P, x="fits-exactly")

    label = "/".join(["x" * 255] * 16)

    with pytest.raises(ValueError, match="PATH_MAX 4096"):
        P[label] = R_make_a(P, x="too-long")


def test_pipeline_labels_reject_result_file_directory_conflicts():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["dataset"] = R_make_a(P, x="first")

    with pytest.raises(ValueError, match="conflicts"):
        P["dataset/a.txt"] = R_make_a(P, x="second")


def test_pipeline_item_labels_can_use_reserved_attribute_names():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["nodes"] = R_make_a(P, x="x")

    assert P.labels_for(P["nodes"]) == ("nodes",)
    assert isinstance(P.nodes, list)


def test_pipeline_attribute_labels_reject_reserved_names_without_partial_assignment():
    P = Pipeline(DAG(TEST_NODES_DIR))

    with pytest.raises(ValueError, match="reserved"):
        P.nodes = R_make_a(P, x="x")
    assert len(P.nodes) == 0


def test_pipeline_rejects_labels_declared_in_keywords(monkeypatch):
    """Every keywords.py entry must be unusable through either label syntax."""
    monkeypatch.setattr(pipeline_keywords, "RESERVED", frozenset({"reserved_keyword"}))
    P = Pipeline(DAG(TEST_NODES_DIR))
    node = R_make_a(P, x="x")

    with pytest.raises(ValueError, match="'reserved_keyword' is reserved"):
        P.reserved_keyword = node
    with pytest.raises(ValueError, match="'reserved_keyword' is reserved"):
        P["reserved_keyword"] = node

    assert P.labels == ()
    assert P.nodes == []


def test_rule_call_compiles_path_and_fingerprint_immediately(tmp_path):
    dag = DAG(tmp_path)
    P = Pipeline(dag)
    node = R_make_a(P, x="x")

    assert node.path.is_absolute()
    assert node.path.parent.parent == tmp_path.resolve() / "make_a"
    assert len(node.fingerprint) == 64
    assert node.path.parent.name == node.fingerprint


def test_pipeline_rejects_nodes_from_another_dag(tmp_path):
    owner = Pipeline(DAG(tmp_path))
    other = Pipeline(DAG(tmp_path))
    node = R_make_a(owner, x="x")

    with pytest.raises(ValueError, match="different DAG"):
        other.a = node


def test_pipeline_can_alias_canonical_node_from_shared_dag(tmp_path):
    dag = DAG(tmp_path)
    first = Pipeline(dag)
    second = Pipeline(dag)
    first.a = R_make_a(first, x="x")
    second.source = first.a

    assert second.source is first.a
    assert second.labels_for(first.a) == ("source",)


def test_direct_node_construction_is_rejected():
    from necroflow.nodes import Node

    with pytest.raises(TypeError):
        Node()


def test_direct_nodetype_construction_is_rejected():
    """NodeType declarations create nodes only through managed Rule calls."""

    with pytest.raises(TypeError, match="NodeType declaration, not a Node constructor"):
        A()


def test_nodetype_representation_is_its_declaration_name():
    """Diagnostics should render a NodeType by its concise declaration name."""

    assert repr(A) == "A"


def test_rule_rejects_cooutputs_with_the_same_realized_filename(tmp_path):
    """One rule call cannot map two output names onto the same filesystem path."""

    class AlsoA(NodeType):
        filename = "a.txt"

    rule = Rule(
        "duplicate_outputs",
        Inputs(value=str),
        Outputs(first=A, second=AlsoA),
        "touch {first} {second}",
    )

    with pytest.raises(ValueError, match="declares duplicate output path"):
        rule(Pipeline(DAG(tmp_path)), value="x")


def test_connected_components_use_only_edges_inside_the_supplied_subgraph(tmp_path):
    """A shared parent outside the requested subgraph must not connect its children."""

    pipeline = Pipeline(DAG(tmp_path))
    pipeline.a = R_make_a(pipeline, x="x")
    pipeline.b = R_make_b(pipeline, pipeline.a)
    pipeline.c = R_make_c(pipeline, pipeline.a)

    components = list(iter_connected_components([pipeline.b, pipeline.c]))

    assert [{node.relative_path for node in component} for component in components] == [
        {pipeline.b.relative_path},
        {pipeline.c.relative_path},
    ]


def test_connected_components_group_diamonds_and_isolated_nodes(tmp_path):
    """Connected DAG shapes form one component while isolated nodes remain separate."""

    pipeline = diamond(DAG(tmp_path))
    pipeline.isolated = R_make_a(pipeline, x="isolated")

    components = list(iter_connected_components(pipeline.nodes))

    assert {
        frozenset(node.relative_path for node in component) for component in components
    } == {
        frozenset(
            node.relative_path
            for node in [pipeline.a, pipeline.b, pipeline.c, pipeline.d]
        ),
        frozenset([pipeline.isolated.relative_path]),
    }


def test_pipeline_requires_a_dag_owner():
    """A Pipeline cannot be created without the DAG that owns its node identity."""

    with pytest.raises(TypeError, match="Pipeline requires an owning DAG"):
        Pipeline("not-a-dag")


def test_pipeline_item_access_requires_string_labels(tmp_path):
    """Both item reads and writes require canonical string labels."""

    pipeline = Pipeline(DAG(tmp_path))
    node = R_make_a(pipeline, x="x")

    with pytest.raises(TypeError, match="Pipeline label must be a string"):
        pipeline[1] = node
    with pytest.raises(TypeError, match="Pipeline label must be a string"):
        _ = pipeline[1]


def test_pipeline_item_assignment_requires_a_node(tmp_path):
    """Item labels cannot silently accept ordinary local Python values."""

    pipeline = Pipeline(DAG(tmp_path))
    with pytest.raises(TypeError, match="Pipeline labels require Node values"):
        pipeline["value"] = "not-a-node"


def test_dag_require_rejects_non_nodes_and_foreign_nodes(tmp_path):
    """Execution requirements must be Nodes owned by the selected DAG."""

    dag = DAG(tmp_path / "owner")
    foreign_pipeline = Pipeline(DAG(tmp_path / "foreign"))
    foreign = R_make_a(foreign_pipeline, x="x")

    with pytest.raises(TypeError, match="DAG requirements must be Nodes"):
        dag.require(["not-a-node"])
    with pytest.raises(ValueError, match="required Node belongs to a different DAG"):
        dag.require([foreign])


def test_pipeline_repr_matches_its_ascii_render(tmp_path):
    """Interactive representations must expose the same DAG view as string output."""

    pipeline = Pipeline(DAG(tmp_path))
    pipeline.a = R_make_a(pipeline, x="x")

    assert repr(pipeline) == str(pipeline)


def test_execute_rejects_pipeline_view(tmp_path):
    from necroflow import execute

    P = Pipeline(DAG(tmp_path))
    P.a = R_make_a(P, x="x")

    with pytest.raises(TypeError, match="requires a DAG"):
        execute(P)


def test_pipeline_missing_attribute_still_raises():
    """Typing dynamic pipeline reads as Node must not invent missing values."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    with pytest.raises(AttributeError, match="missing"):
        _ = P.missing


def test_pipeline_save(tmp_path):
    P = diamond()
    out = tmp_path / "out.txt"
    P.save(out)
    assert out.exists()
    assert "Pipeline" in out.read_text()


def test_workdir_is_reserved_input_output_name():
    with pytest.raises(ValueError, match="reserved command placeholder"):

        @command("touch {a}")
        def bad_input(workdir: str):
            a = output(A)
            return a

    with pytest.raises(ValueError, match="reserved command placeholder"):

        @command("touch {workdir}")
        def bad_output(x: str):
            workdir = output(A)
            return workdir


def test_pipeline_sections_tag_subsequent_nodes_only():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.section("Preparation")
    P.b = R_make_b(P, P.a)
    P.section("Analysis")
    P.c = R_make_c(P, P.a)

    assert P.sections == ("Preparation", "Analysis")
    assert P.section_for(P.a) is None
    assert P.section_for(P.b) == "Preparation"
    assert P.section_for(P.c) == "Analysis"


def test_pipeline_section_rejects_invalid_or_duplicate_names():
    P = Pipeline(DAG(TEST_NODES_DIR))
    with pytest.raises(TypeError, match="must be a string"):
        P.section(1)
    with pytest.raises(ValueError, match="must not be empty"):
        P.section("  ")
    P.section("Preparation")
    with pytest.raises(ValueError, match="already exists"):
        P.section("Preparation")


def test_png_renderer_clusters_unambiguous_pipeline_sections(tmp_path, monkeypatch):
    import sys
    from necroflow import graphviz_render, output

    class FakeGraph:
        def __init__(self):
            self.nodes = []
            self.edges = []

        def add_nodes_from(self, nodes):
            self.nodes.extend(nodes)

        def add_edges_from(self, edges):
            self.edges.extend(edges)

        def in_degree(self, node):
            return sum(target == node for _source, target in self.edges)

    class FakeNetworkX:
        DiGraph = FakeGraph

        def is_directed_acyclic_graph(graph):
            return True

        def topological_generations(graph):
            yield graph.nodes

    dag = DAG(tmp_path)
    P = Pipeline(dag)
    P.section("Preparation")
    P.a = R_make_a(P, x="x")
    P.section("Analysis")
    P.b = R_make_b(P, P.a)
    dag.require(P.sinks())

    captured = {}
    monkeypatch.setitem(sys.modules, "networkx", FakeNetworkX)
    monkeypatch.setattr(graphviz_render.shutil, "which", lambda _name: "dot")
    monkeypatch.setattr(
        graphviz_render.subprocess,
        "run",
        lambda _args, **kwargs: captured.setdefault("dot", kwargs["input"]),
    )

    graphviz_render.render_png(dag, output_path=tmp_path / "dag.png")

    assert "subgraph cluster_section_0" in captured["dot"]
    assert 'label="Preparation";' in captured["dot"]
    assert 'label="Analysis";' in captured["dot"]


# ── DAG deduplication ─────────────────────────────────────────────────────────


def test_dag_deduplicates_shared_nodes():
    dag = DAG(TEST_NODES_DIR)
    P1 = Pipeline(dag)
    P1.a = R_make_a(P1, x="shared")
    P1.b = R_make_b(P1, P1.a)

    P2 = Pipeline(dag)
    P2.a = R_make_a(P2, x="shared")
    P2.b = R_make_b(P2, P2.a)

    dag.require(P1.sinks())
    dag.require(P2.sinks())
    # same config → same hash → 2 unique nodes, not 4
    assert len(dag.nodes) == 2
    assert P1.a is P2.a
    assert P1.b is P2.b


def test_dag_interns_multioutput_rule_calls_atomically():
    rule = Rule(
        "make_ab",
        Inputs(x=str),
        Outputs(a=A, b=B),
        "touch {a} {b}",
    )
    dag = DAG(TEST_NODES_DIR)
    first = Pipeline(dag)
    second = Pipeline(dag)
    first.a, first.b = rule(first, x="same")
    second.a, second.b = rule(second, x="same")

    assert first.a is second.a
    assert first.b is second.b
    assert first.a.rule_call is first.b.rule_call
    assert len(dag.calls) == 1


def test_dag_section_is_none_when_shared_nodes_have_conflicting_sections():
    dag = DAG(TEST_NODES_DIR)
    P1 = Pipeline(dag)
    P1.section("Preparation")
    P1.a = R_make_a(P1, x="shared")

    P2 = Pipeline(dag)
    P2.section("Alternative preparation")
    P2.a = R_make_a(P2, x="shared")

    assert dag.section_for(dag.nodes[0]) is None


def test_dag_keeps_distinct_nodes():
    dag = DAG(TEST_NODES_DIR)
    P1 = Pipeline(dag)
    P1.a = R_make_a(P1, x="x1")
    P1.b = R_make_b(P1, P1.a)

    P2 = Pipeline(dag)
    P2.a = R_make_a(P2, x="x2")
    P2.b = R_make_b(P2, P2.a)

    dag.require(P1.sinks())
    dag.require(P2.sinks())
    assert len(dag.nodes) == 4


def test_dag_required_defaults_to_sinks():
    dag = DAG(TEST_NODES_DIR)
    P = diamond(dag)
    dag.require(P.sinks())
    assert len(dag.required_nodes) == 1
    assert dag.required_nodes[0].rule.__name__ == "make_d"


def test_dag_explicit_request():
    dag = DAG(TEST_NODES_DIR)
    P = diamond(dag)
    dag.require([P.b, P.c])
    req_rules = {n.rule.__name__ for n in dag.required_nodes}
    assert req_rules == {"make_b", "make_c"}


def test_str_long_range_edge():
    """Long-range edges (spanning >1 layer) render as │ pass-throughs, not silently dropped.

    Chain: a(0)→b(1)→c(2), plus direct a→e(3). The a→e edge skips two layers; dummy
    pass-through nodes are inserted so the connector is drawn through all intermediate layers.
    """
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c_from_b(P, P.b)
    P.e = R_make_e_from_ac(P, P.a, P.c)
    rendered = str(P)
    # all node labels present
    for label in ("make_a", "make_b", "make_c_from_b", "make_e_from_ac"):
        assert label in rendered
    # dummy pass-throughs add an extra │ to the mid row of intermediate layers,
    # e.g. "│ make_b[B:b] │   │" has 3 pipe chars vs 2 for a plain box row
    rows_with_dummy = [
        l for l in rendered.splitlines() if "make_" in l and l.count("│") >= 3
    ]
    assert (
        len(rows_with_dummy) > 0
    ), "expected dummy │ pass-through in intermediate layer rows"


def test_dag_save(tmp_path):
    dag = DAG(tmp_path)
    P = diamond(dag)
    dag.require(P.sinks())
    out = tmp_path / "dag.txt"
    dag.save(out)
    assert out.exists()
    assert "DAG" in out.read_text()
