"""Tests for Pipeline and DAG."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule

import pytest
from pathlib import Path
from necroflow import (
    DAG,
    NodeType,
    Pipeline,
    command,
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
    P.finish()
    assert P.sinks() == [P.a]


def test_sinks_linear():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.finish()
    assert P.sinks() == [P.b]


def test_sinks_diamond():
    P = diamond()
    P.finish()
    assert P.sinks() == [P.d]


def test_sinks_multiple():
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c(P, P.a)
    P.finish()
    # b and c are both sinks (nothing depends on them)
    assert set(id(n) for n in P.sinks()) == {id(P.b), id(P.c)}


def test_sinks_excludes_intermediate():
    P = diamond()
    P.finish()
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


def test_node_label_replaces_an_existing_plain_python_attribute():
    """Attribute-form labels must not remain shadowed by an earlier local value."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.result = "not yet a node"
    node = R_make_a(P, x="x")

    P.result = node

    assert P.result is node
    assert P["result"] is node


def test_subpipeline_assignments_use_qualified_root_labels():
    """A subpipeline is a prefixed view over one shared Pipeline namespace."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.shared = R_make_a(P, x="shared")
    sample = P.subpipeline("samples/A")

    sample.raw = R_make_a(sample, x="A")
    sample["result"] = R_make_b(sample, P.shared)
    P.finish()

    assert sample.raw is P["samples/A/raw"]
    assert sample["result"] is P["samples/A/result"]
    assert sample.labels == (
        "shared",
        "samples/A/raw",
        "samples/A/result",
    )
    assert sample.nodes == P.nodes
    assert sample.sinks() == P.sinks()
    assert sample.finished


def test_nested_subpipeline_prefixes_compose():
    """Nested reusable factories must extend, rather than replace, their prefix."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    nested = P.subpipeline("samples/A").subpipeline("qc")

    nested.report = R_make_a(nested, x="report")

    assert nested.report is P["samples/A/qc/report"]


def test_subpipelines_preserve_eager_dag_deduplication():
    """Request prefixes must never create distinct computational identities."""
    dag = DAG(TEST_NODES_DIR)
    P = Pipeline(dag)
    first = P.subpipeline("samples/A")
    second = P.subpipeline("samples/B")

    first.result = R_make_a(first, x="shared")
    second.result = R_make_a(second, x="shared")

    assert first.result is second.result
    assert len(dag.calls) == 1
    assert P.labels_for(first.result) == (
        "samples/A/result",
        "samples/B/result",
    )


def test_subpipeline_reports_qualified_label_collisions_during_assignment():
    """Two views cannot publish different Nodes under one qualified request name."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    first = P.subpipeline("samples/A")
    second = P.subpipeline("samples/A")
    first.result = R_make_a(first, x="first")

    with pytest.raises(ValueError, match="samples/A/result.*already assigned"):
        second.result = R_make_a(second, x="second")


@pytest.mark.parametrize(
    "prefix", ["", "/absolute", "../outside", "dataset/./sample", "dataset//sample"]
)
def test_subpipeline_rejects_invalid_request_prefixes(prefix):
    """A request prefix must be a non-empty canonical relative POSIX path."""
    P = Pipeline(DAG(TEST_NODES_DIR))

    with pytest.raises(ValueError):
        P.subpipeline(prefix)


def test_pipeline_finish_freezes_root_and_all_subpipeline_views():
    """No rule call or label binding may mutate a Pipeline after compilation."""
    dag = DAG(TEST_NODES_DIR)
    P = Pipeline(dag)
    sample = P.subpipeline("sample")
    sample.result = R_make_a(sample, x="before")
    P.finish()
    calls_before = len(dag.calls)

    with pytest.raises(RuntimeError, match="construction has finished"):
        R_make_a(sample, x="after")
    assert len(dag.calls) == calls_before

    with pytest.raises(RuntimeError, match="construction has finished"):
        sample.alias = sample.result
    with pytest.raises(RuntimeError, match="construction has finished"):
        P.subpipeline("late")


def test_only_root_pipeline_can_finish_shared_construction():
    """A nested workflow must not accidentally freeze its caller's Pipeline."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    sample = P.subpipeline("sample")

    with pytest.raises(RuntimeError, match="root Pipeline"):
        sample.finish()

    P.finish()
    P.finish()


def test_pipeline_sinks_require_finished_construction():
    """Sink selection is meaningful only after the complete label graph exists."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")

    with pytest.raises(RuntimeError, match="not finished"):
        P.sinks()

    P.finish()
    assert P.sinks() == [P.a]


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

    P.finish()
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


def test_pipeline_labels_reject_result_file_directory_conflicts_reverse_order():
    """A shallow label assigned after a deeper one under it must also be rejected."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["dataset/a.txt"] = R_make_a(P, x="first")

    with pytest.raises(ValueError, match="conflicts"):
        P["dataset"] = R_make_a(P, x="second")


def test_pipeline_labels_allow_sibling_paths_under_shared_directory():
    """Two labels nested under the same directory prefix must not conflict."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    P["group/one"] = R_make_a(P, x="first")
    P["group/two"] = R_make_a(P, x="second")

    assert P.labels == ("group/one", "group/two")


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


def test_rule_call_compiles_path_and_fingerprint_immediately(tmp_path):
    dag = DAG(tmp_path)
    P = Pipeline(dag)
    node = R_make_a(P, x="x")

    assert node.path.is_absolute()
    assert node.path.parent.parent == tmp_path.resolve() / "make_a"
    assert len(node.rule_hash) == 64
    assert len(node.provenance_hash) == 64
    assert node.path.parent.name == node.provenance_hash


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


@pytest.mark.parametrize("same_type", [False, True])
def test_rule_rejects_cooutputs_with_the_same_realized_filename(tmp_path, same_type):
    """Co-output collisions explain the fix before DAG or filesystem mutation."""

    class AlsoA(NodeType):
        filename = "a.txt"

    second_type = A if same_type else AlsoA
    rule = Rule(
        "duplicate_outputs",
        Inputs(value=str),
        Outputs(first=A, second=second_type),
        "touch {first} {second}",
    )
    nodes_dir = tmp_path / "nodes"
    dag = DAG(nodes_dir)

    with pytest.raises(ValueError, match="declares duplicate output path") as error:
        rule(Pipeline(dag), value="x")

    message = str(error.value)
    assert "rule 'duplicate_outputs'" in message
    assert "outputs 'first' (A)" in message
    assert f"'second' ({second_type.__name__})" in message
    assert "same filename 'a.txt'" in message
    assert "Change one output's NodeType.filename to a distinct filename" in message
    assert "separate NodeType subclass" in message
    assert "renaming only the output variable does not change its filename" in message
    assert dag.calls == {}
    assert dag.nodes == []
    assert not nodes_dir.exists()


def test_rule_allows_matching_input_and_output_basenames_in_distinct_workdirs(tmp_path):
    """Inputs keep their parent paths, so equal basenames across calls are safe."""
    pipeline = Pipeline(DAG(tmp_path / "nodes"))
    first = R_make_a(pipeline, x="first")
    second = R_make_a(pipeline, x="second")
    consume = Rule(
        "consume",
        Inputs(first=A, second=A),
        Outputs(result=A),
        "cat {first} {second} > {result}",
    )

    result = consume(pipeline, first, second)

    assert first.path.name == second.path.name == result.path.name == "a.txt"
    assert len({first.path.parent, second.path.parent, result.path.parent}) == 3
    args = result.rule_call.command_args()
    assert args.inputs.first == first.path
    assert args.inputs.second == second.path
    assert args.outputs.result == result.path


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


def test_pipeline_repr_matches_its_tgf_render(tmp_path):
    """Interactive representations must expose the same DAG view as string output."""

    pipeline = Pipeline(DAG(tmp_path))
    pipeline.a = R_make_a(pipeline, x="x")

    assert repr(pipeline) == str(pipeline)


def test_run_rejects_pipeline_view(tmp_path):
    from necroflow import run

    P = Pipeline(DAG(tmp_path))
    P.a = R_make_a(P, x="x")

    with pytest.raises(TypeError, match="requires a DAG"):
        run(P)


def test_pipeline_missing_attribute_still_raises():
    """Typing dynamic pipeline reads as Node must not invent missing values."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    with pytest.raises(AttributeError, match="missing"):
        _ = P.missing


def test_pipeline_save(tmp_path):
    """Pipeline.save must persist exactly its TGF string representation."""
    P = diamond()
    out = tmp_path / "pipeline.tgf"
    P.save(out)
    assert out.exists()
    assert out.read_text() == str(P) + "\n"


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


# ── DAG deduplication ─────────────────────────────────────────────────────────


def test_dag_deduplicates_shared_nodes():
    dag = DAG(TEST_NODES_DIR)
    P1 = Pipeline(dag)
    P1.a = R_make_a(P1, x="shared")
    P1.b = R_make_b(P1, P1.a)

    P2 = Pipeline(dag)
    P2.a = R_make_a(P2, x="shared")
    P2.b = R_make_b(P2, P2.a)

    P1.finish()
    P2.finish()
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


def test_dag_keeps_distinct_nodes():
    dag = DAG(TEST_NODES_DIR)
    P1 = Pipeline(dag)
    P1.a = R_make_a(P1, x="x1")
    P1.b = R_make_b(P1, P1.a)

    P2 = Pipeline(dag)
    P2.a = R_make_a(P2, x="x2")
    P2.b = R_make_b(P2, P2.a)

    P1.finish()
    P2.finish()
    dag.require(P1.sinks())
    dag.require(P2.sinks())
    assert len(dag.nodes) == 4


def test_dag_required_defaults_to_sinks():
    dag = DAG(TEST_NODES_DIR)
    P = diamond(dag)
    P.finish()
    dag.require(P.sinks())
    assert len(dag.required_nodes) == 1
    assert dag.required_nodes[0].rule.__name__ == "make_d"


def test_dag_explicit_request():
    dag = DAG(TEST_NODES_DIR)
    P = diamond(dag)
    dag.require([P.b, P.c])
    req_rules = {n.rule.__name__ for n in dag.required_nodes}
    assert req_rules == {"make_b", "make_c"}


def test_tgf_preserves_long_range_edge():
    """TGF must retain direct dependencies regardless of topological distance."""
    P = Pipeline(DAG(TEST_NODES_DIR))
    P.a = R_make_a(P, x="x")
    P.b = R_make_b(P, P.a)
    P.c = R_make_c_from_b(P, P.b)
    P.e = R_make_e_from_ac(P, P.a, P.c)
    rendered = str(P)
    for label in ("make_a", "make_b", "make_c_from_b", "make_e_from_ac"):
        assert label in rendered
    assert "1 4" in rendered.splitlines()


def test_dag_save(tmp_path):
    """DAG.save must persist TGF and retain requested-node labels."""
    dag = DAG(tmp_path)
    P = diamond(dag)
    P.finish()
    dag.require(P.sinks())
    out = tmp_path / "dag.tgf"
    dag.save(out)
    assert out.exists()
    assert out.read_text() == str(dag) + "\n"
    assert "4 make_d[D:d] [required]" in out.read_text().splitlines()
