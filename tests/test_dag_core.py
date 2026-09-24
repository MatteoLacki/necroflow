"""Tests for DAG internals: paths, commands, types, and labels."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule
from necroflow import command, output

import pytest
from typing import Literal, Union
import necroflow.dag as dag_core
import necroflow.fs as fs_core
from necroflow import DAG, NodeType, Pipeline

# ── fixtures ──────────────────────────────────────────────────────────────────


class Txt(NodeType):
    filename = "out.txt"


class Upper(NodeType):
    filename = "upper.txt"


class Log(NodeType):
    filename = "log.txt"


class SortedTxt(Txt):
    filename = "sorted.txt"


class Joined(NodeType):
    filename = "joined.txt"


R_make_txt = Rule("make_txt", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}")
R_make_sorted_txt = Rule(
    "make_sorted_txt",
    Inputs(word=str),
    Outputs(stxt=SortedTxt),
    "echo {word} | sort > {stxt}",
)
R_to_upper = Rule(
    "to_upper",
    Inputs(txt=Txt, n=int),
    Outputs(upper=Upper, log=Log),
    "tr a-z A-Z < {txt} | head -{n} | tee {log} > {upper}",
)
R_sort_txt = Rule(
    "sort_txt",
    Inputs(txt=SortedTxt),
    Outputs(sorted_txt=SortedTxt),
    "sort {txt} > {sorted_txt}",
)
R_join_upper = Rule(
    "join_upper",
    Inputs(a=Upper, b=Upper),
    Outputs(joined=Joined),
    "cat {a} {b} > {joined}",
)
P = Pipeline(DAG("/tmp/necroflow-test-dag-core"))


# ── path generation ───────────────────────────────────────────────────────────


def test_compiled_paths_structure(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    assert txt.path == (
        tmp_path.resolve() / "make_txt" / txt.provenance_hash / "out.txt"
    )


def test_compiled_paths_cooutputs_share_dir(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    upper, log = R_to_upper(P, txt, n=3)
    assert upper.path.parent == log.path.parent
    assert upper.path != log.path


def test_compiled_paths_use_full_64_character_fingerprint(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    assert len(txt.path.parent.name) == 64


def test_rule_call_rejects_component_over_name_max(tmp_path, monkeypatch):
    class LongName(NodeType):
        filename = "x" * 11

    r_make_long_name = Rule(
        "make_long_name", Inputs(word=str), Outputs(out=LongName), "echo {word} > {out}"
    )
    monkeypatch.setattr(fs_core, "_filesystem_limits", lambda path: (10, 4096))

    with pytest.raises(ValueError, match="NAME_MAX"):
        r_make_long_name(Pipeline(DAG(tmp_path)), word="hi")


def test_rule_call_rejects_total_path_over_path_max(tmp_path, monkeypatch):
    monkeypatch.setattr(fs_core, "_filesystem_limits", lambda path: (255, 20))

    with pytest.raises(ValueError, match="PATH_MAX"):
        R_make_txt(Pipeline(DAG(tmp_path)), word="hi")


# ── fingerprinting ────────────────────────────────────────────────────────────


def test_fingerprint_stable():
    txt1 = R_make_txt(P, word="hi")
    txt2 = R_make_txt(P, word="hi")
    assert txt1.provenance_hash == txt2.provenance_hash


def test_fingerprint_differs_on_config():
    txt_a = R_make_txt(P, word="hello")
    txt_b = R_make_txt(P, word="world")
    assert txt_a.provenance_hash != txt_b.provenance_hash


def test_fingerprint_differs_on_parent():
    txt_a = R_make_txt(P, word="hello")
    txt_b = R_make_txt(P, word="world")
    upper_a, _ = R_to_upper(P, txt_a, n=1)
    upper_b, _ = R_to_upper(P, txt_b, n=1)
    assert upper_a.provenance_hash != upper_b.provenance_hash


def test_fingerprint_changes_on_inputs_type_change():
    """Changing the declared Inputs NodeType must change the fingerprint."""

    class FastqA(NodeType):
        filename = "fastq"

    class FastqB(NodeType):
        filename = "fastq"

    Ra_raw = Rule("raw", Inputs(path=str), Outputs(fastq=FastqA), "touch {fastq}")
    Ra_align = Rule(
        "align", Inputs(fastq=FastqA, ref=str), Outputs(txt=Txt), "touch {txt}"
    )
    Rb_raw = Rule("raw", Inputs(path=str), Outputs(fastq=FastqB), "touch {fastq}")
    Rb_align = Rule(
        "align", Inputs(fastq=FastqB, ref=str), Outputs(txt=Txt), "touch {txt}"
    )

    bam_a = Ra_align(P, Ra_raw(P, path="/d/s.fq"), ref="hg38")
    bam_b = Rb_align(P, Rb_raw(P, path="/d/s.fq"), ref="hg38")
    assert bam_a.provenance_hash != bam_b.provenance_hash


def test_fingerprint_changes_on_outputs_type_change():
    """Changing the declared Outputs NodeType must change the fingerprint."""

    class BamA(NodeType):
        filename = "aligned.bam"

    class BamB(NodeType):
        filename = "aligned.bam"

    Ra_align = Rule("align", Inputs(path=str), Outputs(bam=BamA), "touch {bam}")
    Rb_align = Rule("align", Inputs(path=str), Outputs(bam=BamB), "touch {bam}")

    bam_a = Ra_align(P, path="/d/s.fq")
    bam_b = Rb_align(P, path="/d/s.fq")
    assert bam_a.provenance_hash != bam_b.provenance_hash


def test_node_key_unique_for_cooutputs():
    txt = R_make_txt(P, word="hi")
    upper, log = R_to_upper(P, txt, n=1)
    assert upper.relative_path != log.relative_path


def test_node_relative_path_contains_rule_and_filename():
    txt = R_make_txt(P, word="hi")
    assert txt.relative_path.parts[0] == "make_txt"
    assert txt.relative_path.name == "out.txt"


def test_output_filename_must_be_one_safe_relative_component(tmp_path):
    class Escaping(NodeType):
        filename = "../escape.txt"

    rule = Rule("escape", Inputs(x=str), Outputs(out=Escaping), "touch {out}")

    with pytest.raises(ValueError, match="one relative path component"):
        rule(Pipeline(DAG(tmp_path)), x="x")


def test_rule_rejects_filename_less_output_type_at_declaration():
    """A filename-less NodeType is an input contract, not a concrete output."""

    class MmappetDataset(NodeType):
        pass

    with pytest.raises(
        TypeError,
        match="output 'dataset'.*MmappetDataset.*must define filename",
    ):
        Rule(
            "emit_dataset",
            Inputs(),
            Outputs(dataset=MmappetDataset),
            "mkdir {dataset}",
        )


def test_command_decorator_rejects_filename_less_output_type():
    """Decorator sugar must reject abstract outputs while the module loads."""

    class MmappetDataset(NodeType):
        pass

    with pytest.raises(
        TypeError,
        match="output 'dataset'.*MmappetDataset.*must define filename",
    ):

        @command("mkdir {dataset}")
        def emit_dataset():
            dataset = output(MmappetDataset)
            return dataset


def test_filename_less_nodetype_remains_a_valid_input_contract(tmp_path):
    """Abstract format families must continue accepting concrete subclasses."""

    class MmappetDataset(NodeType):
        pass

    class PrecursorTable(MmappetDataset):
        filename = "precursors.mmappet"

    produce = Rule(
        "produce_precursors",
        Inputs(),
        Outputs(dataset=PrecursorTable),
        "mkdir {dataset}",
    )
    consume = Rule(
        "consume_mmappet",
        Inputs(dataset=MmappetDataset),
        Outputs(txt=Txt),
        "printf consumed > {txt}",
    )
    pipeline = Pipeline(DAG(tmp_path))

    dataset = produce(pipeline)
    consumed = consume(pipeline, dataset)

    assert consumed.parents == [dataset]


def test_rule_name_must_be_one_safe_relative_component(tmp_path):
    rule = Rule("../escape", Inputs(x=str), Outputs(out=Txt), "touch {out}")

    with pytest.raises(ValueError, match="rule name"):
        rule(Pipeline(DAG(tmp_path)), x="x")


# ── command resolution ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "declaration, message",
    [
        (lambda: command(42), "requires a shell string or Python callback"),
        (lambda: command("touch", Inputs()), "requires Inputs, Outputs"),
        (lambda: command("touch", Inputs(), Outputs()), "requires an explicit name"),
        (
            lambda: command("touch", Inputs(), Outputs(), name="factory", threads=2),
            "cannot use constraint keywords",
        ),
        (
            lambda: command("touch", object(), Outputs(), name="factory"),
            "requires Inputs and Outputs declarations",
        ),
        (
            lambda: command("touch", Inputs(), Outputs(), object(), name="factory"),
            "constraints must be a Constraints object",
        ),
        (lambda: command("touch", name="decorator"), "only valid for factory commands"),
    ],
)
def test_command_factory_rejects_ambiguous_declarations(declaration, message):
    """Factory and decorator command forms must fail clearly when mixed or incomplete."""

    with pytest.raises(TypeError, match=message):
        declaration()


def test_rule_call_requires_pipeline_and_node_inputs(tmp_path):
    """Rules need an explicit or scoped owner and correctly typed managed inputs."""

    with pytest.raises(RuntimeError, match="active @workflow or an explicit Pipeline"):
        R_make_txt("not-a-pipeline", word="x")

    pipeline = Pipeline(DAG(tmp_path))
    with pytest.raises(TypeError, match="expected Node"):
        R_to_upper(pipeline, "not-a-node", n=1)


def test_runtime_uncheckable_config_annotation_remains_fingerprintable(tmp_path):
    """Typing-only config contracts may skip isinstance checks but retain identity."""

    rule = Rule(
        "literal_config",
        Inputs(mode=Literal["strict", "relaxed"]),
        Outputs(txt=Txt),
        "touch {txt}",
    )

    node = rule(Pipeline(DAG(tmp_path)), mode="strict")

    assert len(node.provenance_hash) == 64


def test_resolve_command_input_substitution(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    upper, _ = R_to_upper(P, txt, n=2)
    cmd = upper.rule_call.resolve()
    assert str(txt.path) in cmd


def test_resolve_command_config_substitution(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hello")
    cmd = txt.rule_call.resolve()
    assert "hello" in cmd


def test_resolve_command_quotes_string_config_for_shell_commands(tmp_path):
    P = Pipeline(DAG(tmp_path))
    r_filter_txt = Rule(
        "filter_txt",
        Inputs(filter=str),
        Outputs(txt=Txt),
        "tool --filter {filter} > {txt}",
    )
    txt = r_filter_txt(P, filter="a > b")

    assert txt.rule_call.resolve() == f"tool --filter 'a > b' > {txt.path}"


def test_resolve_command_scalar_config_stays_bare_when_shell_safe(tmp_path):
    P = Pipeline(DAG(tmp_path))
    r_number_txt = Rule(
        "number_txt", Inputs(n=int), Outputs(txt=Txt), "tool -n {n} > {txt}"
    )
    txt = r_number_txt(P, n=5)

    assert txt.rule_call.resolve() == f"tool -n 5 > {txt.path}"


def test_list_commands_are_rejected():
    with pytest.raises(TypeError, match="argv list commands are unsupported"):
        Rule(
            "list_filter",
            Inputs(filter=str),
            Outputs(txt=Txt),
            ["tool", "--filter", "{filter}", "--out", "{txt}"],
        )


def test_resolve_command_output_substitution(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    upper, log = R_to_upper(P, txt, n=1)
    cmd = upper.rule_call.resolve()
    assert str(upper.path) in cmd
    assert str(log.path) in cmd


R_no_command = Rule("no_command", Inputs(word=str), Outputs(txt=Txt), None)


def test_resolve_command_none_for_no_command(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_no_command(P, word="hi")
    assert txt.rule_call.resolve() is None


def test_resolve_command_direct_constraint_placeholders(tmp_path):
    P = Pipeline(DAG(tmp_path))
    r_constrained = Rule(
        "constrained",
        Inputs(word=str),
        Outputs(txt=Txt),
        "tool --threads {threads} --ram {ram} --gpu {constraint:gpu} --word {word} > {txt}",
        Constraints(threads=8, ram="4Gi", gpu=2),
    )
    txt = r_constrained(P, word="hi")

    assert (
        txt.rule_call.resolve()
        == f"tool --threads 8 --ram 4Gi --gpu 2 --word hi > {txt.path}"
    )


def test_resolve_command_threads_defaults_to_one(tmp_path):
    P = Pipeline(DAG(tmp_path))
    r_default_threads = Rule(
        "default_threads",
        Inputs(word=str),
        Outputs(txt=Txt),
        "tool --threads {threads} > {txt}",
    )
    txt = r_default_threads(P, word="hi")

    assert txt.rule_call.resolve() == f"tool --threads 1 > {txt.path}"


def test_resolve_command_preserves_escaped_shell_braces(tmp_path):
    P = Pipeline(DAG(tmp_path))
    r_brace = Rule(
        "brace",
        Inputs(word=str),
        Outputs(txt=Txt),
        "printf '%s\n' {{left,right}} {word} > {txt}",
    )
    txt = r_brace(P, word="hi")

    assert txt.rule_call.resolve() == f"printf '%s\n' {{left,right}} hi > {txt.path}"


def test_resolve_command_substitutes_union_typed_input(tmp_path):
    P = Pipeline(DAG(tmp_path))

    # Regression test: RuleCall.resolve used to build its {name} substitution dict by
    # filtering node.rule.inputs.specs with _is_nodetype(), which is a strict
    # isclass()-and-issubclass()-NodeType check -- False for a `TypeA | TypeB` union,
    # even though docs/rules.md documents unions as a supported "either format is
    # fine" input contract (and rule-call-time validation already accepted them via
    # _is_node_input_contract). A union-typed positional input's placeholder was
    # therefore silently dropped from the substitution dict, and {name} in the
    # command template raised a bare KeyError at execution time.
    @command("cat {doc} > {txt}")
    def read_either(doc: Txt | Upper):
        txt = output(Txt)
        return txt

    src = R_make_txt(P, word="hi")
    doc = read_either(P, src)
    cmd = doc.rule_call.resolve()
    assert str(src.path) in cmd
    assert str(doc.path) in cmd


def test_input_name_colliding_with_constraint_is_rejected():
    with pytest.raises(
        ValueError, match=r"collides with a resource constraint name: \['threads'\]"
    ):
        Rule(
            "colliding_threads",
            Inputs(threads=int),
            Outputs(txt=Txt),
            "tool --arg {threads} --scheduler {constraint:threads} > {txt}",
            Constraints(threads=8),
        )


def test_output_name_colliding_with_declared_constraint_is_rejected():
    with pytest.raises(
        ValueError, match=r"collides with a resource constraint name: \['ram'\]"
    ):
        Rule(
            "colliding_ram",
            Inputs(word=str),
            Outputs(ram=Txt),
            "tool --ram {ram} --word {word} > {ram}",
            Constraints(ram="4Gi"),
        )


def test_input_name_colliding_with_implicit_threads_default_is_rejected():
    """threads=1 applies even without an explicit Constraints(), so it stays reserved."""
    with pytest.raises(
        ValueError, match=r"collides with a resource constraint name: \['threads'\]"
    ):
        Rule(
            "implicit_threads_collision",
            Inputs(threads=int),
            Outputs(txt=Txt),
            "tool --arg {threads} > {txt}",
        )


def test_unknown_constraint_placeholder_is_rejected():
    with pytest.raises(ValueError, match=r"unknown placeholders: \['ram'\]"):

        @command("tool --ram {ram} > {txt}")
        def bad_ram(word: str):
            txt = output(Txt)
            return txt

    with pytest.raises(ValueError, match=r"unknown constraint placeholders: \['gpu'\]"):

        @command("tool --gpu {constraint:gpu} > {txt}")
        def bad_gpu(word: str):
            txt = output(Txt)
            return txt


# ── accumulated config ────────────────────────────────────────────────────────


def test_accumulated_config_single_node():
    txt = R_make_txt(P, word="hello")
    cfg = txt.rule_call._accumulated_config()
    assert cfg["word"] == "hello"


def test_accumulated_config_multi_hop():
    txt = R_make_txt(P, word="hello")
    upper, _ = R_to_upper(P, txt, n=5)
    cfg = upper.rule_call._accumulated_config()
    assert cfg["word"] == "hello"
    assert cfg["n"] == 5


def test_accumulated_config_diamond_visits_shared_ancestor_once():
    """Two branches sharing one ancestor must still merge that ancestor's config."""
    root = R_make_txt(P, word="hello")
    branch_a, _ = R_to_upper(P, root, n=2)
    branch_b, _ = R_to_upper(P, root, n=3)
    joined = R_join_upper(P, branch_a, branch_b)
    cfg = joined.rule_call._accumulated_config()
    assert cfg["word"] == "hello"
    assert cfg["n"] == 3


# ── provenance ────────────────────────────────────────────────────────────────


def test_write_dependencies_creates_file(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    txt.rule_call.write_dependencies()
    assert (txt.path.parent / ".rip" / "dependencies.toml").exists()


def test_write_dependencies_content(tmp_path):
    P = Pipeline(DAG(tmp_path))
    txt = R_make_txt(P, word="hi")
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    txt.rule_call.write_dependencies()
    content = (txt.path.parent / ".rip" / "dependencies.toml").read_text()
    assert "make_txt" in content
    assert "hi" in content


# ── Inputs/Outputs validation ─────────────────────────────────────────────────


def test_wrong_nodetype_raises():
    txt = R_make_txt(P, word="hi")
    upper, _ = R_to_upper(P, txt, n=1)
    with pytest.raises(TypeError):
        R_to_upper(P, upper, n=1)  # Upper passed where Txt expected


def test_wrong_config_type_raises():
    txt = R_make_txt(P, word="hi")
    with pytest.raises(TypeError):
        R_to_upper(P, txt, n="not_an_int")  # str passed where int expected


def test_missing_positional_input_raises():
    with pytest.raises(TypeError, match="missing a required argument"):
        R_to_upper(P, n=1)  # txt input omitted


def test_missing_config_input_raises():
    """Rule calls must reject omitted required config inputs immediately.

    This keeps malformed DAGs from being accepted and then failing later
    during command formatting or execution.
    """
    with pytest.raises(TypeError, match="missing a required.*argument"):
        R_make_txt(
            P,
        )  # word config omitted


def test_explicit_rule_rejects_unexpected_config_before_interning(tmp_path):
    """Undeclared config must not silently enter identity or create a DAG call."""
    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)

    with pytest.raises(TypeError, match="unexpected keyword argument 'extra'"):
        R_make_txt(pipeline, word="hi", extra=True)

    assert dag.calls == {}
    assert dag.nodes == []


def test_decorated_rule_rejects_unexpected_config_before_interning(tmp_path):
    """Decorator sugar and explicit Rules must enforce the same call schema."""

    @command("echo {word} > {txt}")
    def make_txt(word: str):
        txt = output(Txt)
        return txt

    dag = DAG(tmp_path)
    pipeline = Pipeline(dag)

    with pytest.raises(TypeError, match="unexpected keyword argument 'extra'"):
        make_txt(pipeline, word="hi", extra=True)

    assert dag.calls == {}
    assert dag.nodes == []


def test_extra_positional_input_raises():
    """Rule calls must reject undeclared positional node inputs.

    Extra nodes should not silently become parents, because that creates
    dependencies outside the rule's declared input contract.
    """
    txt = R_make_txt(P, word="hi")
    extra = R_make_txt(P, word="extra")
    with pytest.raises(TypeError, match="too many positional arguments"):
        R_to_upper(P, txt, extra, n=1)


def test_node_input_accepted_by_keyword():
    """Node inputs may now be named, not just positional."""
    txt = R_make_txt(P, word="hi")

    by_position, _ = R_to_upper(P, txt, n=1)
    by_keyword, _ = R_to_upper(P, txt=txt, n=1)
    mixed_order, _ = R_to_upper(P, n=1, txt=txt)

    assert by_position is by_keyword is mixed_order


def test_node_input_duplicate_positional_and_keyword_raises():
    """Supplying the same Node input both positionally and by name must fail."""
    txt = R_make_txt(P, word="hi")
    with pytest.raises(TypeError, match="multiple values for argument 'txt'"):
        R_to_upper(P, txt, txt=txt, n=1)


def test_mixed_input_accepted_by_keyword(tmp_path):
    """A mixed Node/value input may be named too, for either arm."""

    @command("printf %s {source} > {txt}")
    def consume(source: Txt | str | None = None):
        txt = output(Txt)
        return txt

    pipeline = Pipeline(DAG(tmp_path))
    txt_node = R_make_txt(pipeline, word="hi")

    node_by_position = consume(pipeline, txt_node)
    node_by_keyword = consume(pipeline, source=txt_node)
    value_by_position = consume(pipeline, "external")
    value_by_keyword = consume(pipeline, source="external")

    assert node_by_position is node_by_keyword
    assert value_by_position is value_by_keyword


def test_subtype_accepted():
    # SortedTxt is a subclass of Txt — to_upper accepts Txt and must accept SortedTxt too
    stxt = R_make_sorted_txt(P, word="hi")
    result, _ = R_to_upper(P, stxt, n=1)
    assert result is not None


def test_nodetype_union_accepts_either_member():
    r_use_txt_or_upper = Rule(
        "use_txt_or_upper", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )

    txt = R_make_txt(P, word="hi")
    upper, _ = R_to_upper(P, txt, n=1)

    assert r_use_txt_or_upper(P, txt) is not None
    assert r_use_txt_or_upper(P, upper) is not None


def test_nodetype_union_accepts_subclass_of_member():
    r_use_txt_or_upper = Rule(
        "use_txt_or_upper", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    stxt = R_make_sorted_txt(P, word="hi")

    assert r_use_txt_or_upper(P, stxt) is not None


def test_nodetype_union_rejects_unrelated_type():
    r_use_txt_or_upper = Rule(
        "use_txt_or_upper", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    txt = R_make_txt(P, word="hi")
    log = r_use_txt_or_upper(P, txt)

    with pytest.raises(TypeError, match=r"expected Txt \| Upper"):
        r_use_txt_or_upper(P, log)


def test_mixed_nodetype_union_selects_parent_or_scalar_branch(tmp_path):
    """A mixed fixed input is a dependency only when its runtime value is a Node."""

    rule = Rule(
        "consume_mixed",
        Inputs(source=Txt | str | None),
        Outputs(log=Log),
        "printf %s {source} > {log}",
    )
    pipeline = Pipeline(DAG(tmp_path))
    source = R_make_txt(pipeline, word="managed")

    managed = rule(pipeline, source)
    external = rule(pipeline, "external")
    absent = rule(pipeline, None)

    assert managed.parents == [source]
    assert dict(managed.rule_call.input_values) == {}
    assert external.parents == []
    assert dict(external.rule_call.input_values) == {"source": "external"}
    assert absent.parents == []
    assert dict(absent.rule_call.input_values) == {"source": None}
    assert managed.rule_call.command_args().inputs.source == source.path
    assert external.rule_call.command_args().inputs.source == "external"
    assert absent.rule_call.command_args().inputs.source is None
    assert external.rule_call.resolve() == (f"printf %s external > {external.path}")
    assert absent.rule_call.resolve() == f"printf %s None > {absent.path}"
    assert (
        len({managed.provenance_hash, external.provenance_hash, absent.provenance_hash})
        == 3
    )
    assert rule(pipeline, "external") is external

    absent.rule_call.write_dependencies()
    metadata = (absent.path.parent / ".rip" / "dependencies.toml").read_text()
    assert 'name = "source"' in metadata
    assert 'type = "builtins.NoneType"' in metadata
    assert 'value = "None"' in metadata
    assert 'input_order = ["source"]' in metadata


def test_mixed_nodetype_union_supports_typing_union(tmp_path):
    """Legacy typing.Union spelling must have the same mixed-input behavior."""

    rule = Rule(
        "consume_typing_union",
        Inputs(source=Union[Txt, str]),
        Outputs(log=Log),
        "printf %s {source} > {log}",
    )
    pipeline = Pipeline(DAG(tmp_path))

    assert rule(pipeline, "external").parents == []


def test_mixed_plain_value_fingerprint_retains_positional_order(tmp_path):
    """Callback-visible mixed input order must remain part of call identity."""

    left_first = Rule(
        "ordered_mixed",
        Inputs(left=Txt | str, right=Txt | str),
        Outputs(log=Log),
        "touch {log}",
    )
    right_first = Rule(
        "ordered_mixed",
        Inputs(right=Txt | str, left=Txt | str),
        Outputs(log=Log),
        "touch {log}",
    )
    first_pipeline = Pipeline(DAG(tmp_path / "left-first"))
    second_pipeline = Pipeline(DAG(tmp_path / "right-first"))

    first = left_first(first_pipeline, "left", "right")
    second = right_first(second_pipeline, "right", "left")

    assert dict(first.rule_call.input_values) == dict(second.rule_call.input_values)
    assert first.provenance_hash != second.provenance_hash


def test_mixed_nodetype_union_rejects_unmatched_node_and_scalar(tmp_path):
    """Each mixed-input runtime value must match its Node or scalar union arm."""

    rule = Rule(
        "consume_mixed",
        Inputs(source=Txt | str | None),
        Outputs(log=Log),
        "touch {log}",
    )
    pipeline = Pipeline(DAG(tmp_path))
    unrelated = rule(pipeline, "seed")

    with pytest.raises(TypeError, match="expected NoneType \\| Txt \\| str"):
        rule(pipeline, unrelated)
    with pytest.raises(TypeError, match="expected NoneType \\| Txt \\| str"):
        rule(pipeline, 3)


def test_mixed_nodetype_union_checks_dag_ownership_only_for_nodes(tmp_path):
    """Scalar alternatives have no DAG owner; managed alternatives still must match."""

    rule = Rule(
        "consume_mixed",
        Inputs(source=Txt | str),
        Outputs(log=Log),
        "touch {log}",
    )
    pipeline = Pipeline(DAG(tmp_path / "local"))
    foreign_pipeline = Pipeline(DAG(tmp_path / "foreign"))
    foreign = R_make_txt(foreign_pipeline, word="foreign")

    assert rule(pipeline, "external").parents == []
    with pytest.raises(ValueError, match="different DAG"):
        rule(pipeline, foreign)


def test_config_union_still_supported():
    r_config_union = Rule(
        "config_union",
        Inputs(value=str | int),
        Outputs(txt=Txt),
        "echo {value} > {txt}",
    )

    assert r_config_union(P, value="hi") is not None
    assert r_config_union(P, value=3) is not None
    with pytest.raises(TypeError):
        r_config_union(P, value=object())


def test_fingerprint_changes_for_nodetype_union_contract():
    r_single_consume = Rule(
        "consume", Inputs(data=Txt), Outputs(log=Log), "touch {log}"
    )
    r_union_consume = Rule(
        "consume", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    txt = R_make_txt(P, word="hi")

    assert (
        r_single_consume(P, txt).provenance_hash
        != r_union_consume(P, txt).provenance_hash
    )


def test_nodetype_union_fingerprint_order_is_stable():
    r_ab_consume = Rule(
        "consume", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    r_ba_consume = Rule(
        "consume", Inputs(data=Upper | Txt), Outputs(log=Log), "touch {log}"
    )
    txt = R_make_txt(P, word="hi")

    assert r_ab_consume(P, txt).provenance_hash == r_ba_consume(P, txt).provenance_hash


# ── Pipeline labels ──────────────────────────────────────────────────────────


def test_pipeline_stores_label_binding(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.txt = R_make_txt(P, word="hi")
    assert P.labels_for(P.txt) == ("txt",)


def test_pipeline_stores_cooutput_label_bindings(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.txt = R_make_txt(P, word="hi")
    P.upper, P.log = R_to_upper(P, P.txt, n=1)
    assert P.labels_for(P.upper) == ("upper",)
    assert P.labels_for(P.log) == ("log",)


def test_pipeline_duplicate_raises(tmp_path):
    P = Pipeline(DAG(tmp_path))
    P.txt = R_make_txt(P, word="hi")
    with pytest.raises(ValueError):
        P.txt = R_make_txt(P, word="world")


# ── command placeholder validation ────────────────────────────────────────────


def test_command_unknown_placeholder_raises():
    with pytest.raises(ValueError, match="unknown placeholders"):
        r_bad = Rule(
            "bad", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt} {typo}"
        )


def test_command_missing_output_is_allowed():
    r_ok = Rule(
        "bad", Inputs(word=str), Outputs(txt=Txt, log=Log), "echo {word} > {txt}"
    )
    assert r_ok.outputs.specs == {"txt": Txt, "log": Log}


def test_command_factory_rejects_list_commands():
    with pytest.raises(TypeError, match="argv list commands are unsupported"):
        command(
            ["echo {word} > {txt}", "echo done"],
            Inputs(word=str),
            Outputs(txt=Txt, log=Log),
            name="bad",
        )


def test_command_unreferenced_input_is_allowed():
    r_ok = Rule("ok", Inputs(word=str), Outputs(txt=Txt), "touch {txt}")
    assert r_ok is not None


def test_command_valid_declaration_ok():
    r_good = Rule("good", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}")
    assert r_good is not None


def test_command_repeat_metadata():
    r_repeat_rule = Rule(
        "repeat_rule",
        Inputs(word=str),
        Outputs(txt=Txt),
        "echo {word} > {txt}",
        repeat=3,
    )
    assert r_repeat_rule.repeat == 3
    assert "repeat" not in r_repeat_rule.resources


def test_command_repeat_must_be_positive_int():
    with pytest.raises(ValueError, match="repeat must be a positive integer"):
        r_bad_repeat = Rule(
            "bad_repeat",
            Inputs(word=str),
            Outputs(txt=Txt),
            "echo {word} > {txt}",
            repeat=0,
        )
    with pytest.raises(ValueError, match="repeat must be a positive integer"):
        r_bad_repeat_bool = Rule(
            "bad_repeat_bool",
            Inputs(word=str),
            Outputs(txt=Txt),
            "echo {word} > {txt}",
            repeat=True,
        )


def test_repeat_does_not_affect_fingerprint():
    r1_make = Rule(
        "make", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=1
    )
    r2_make = Rule(
        "make", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=3
    )
    assert (
        r1_make(P, word="hi").provenance_hash == r2_make(P, word="hi").provenance_hash
    )


# ── body return style ─────────────────────────────────────────────────────────


def test_command_decorator_body_return_single():
    @command("echo {word} > {txt}")
    def make_txt(word: str):
        txt = output(Txt)
        return txt

    assert make_txt.outputs.specs == {"txt": Txt}


def test_command_decorator_accepts_repeat_and_constraints():
    @command("echo {word} > {txt}", threads=2, repeat=4)
    def make_txt(word: str):
        txt = output(Txt)
        return txt

    assert make_txt.repeat == 4
    assert make_txt.resources == {"threads": 2}


def test_command_decorator_applies_config_defaults_before_fingerprinting(tmp_path):
    """Omitted and explicit defaults must describe one canonical rule call.

    Defaults are effective config, not declaration-only metadata: they must be
    available on the Node and an explicit override must change its identity.
    """

    @command("printf '%s %s %s' {word} {threshold} {plugin} > {txt}")
    def make_txt(
        word: str,
        threshold: float = 0.05,
        plugin: str | None = None,
    ):
        txt = output(Txt)
        return txt

    pipeline = Pipeline(DAG(tmp_path))
    omitted = make_txt(pipeline, word="value")
    explicit = make_txt(
        pipeline,
        word="value",
        threshold=0.05,
        plugin=None,
    )
    overridden = make_txt(
        pipeline,
        word="value",
        threshold=0.01,
        plugin="adapter",
    )

    assert omitted is explicit
    assert omitted.config == {
        "threshold": 0.05,
        "plugin": None,
        "word": "value",
    }
    assert overridden.provenance_hash != omitted.provenance_hash


def test_declared_default_affects_only_omitted_effective_config(tmp_path):
    """Defaults affect identity through effective config, not extra metadata."""

    lower_default = Rule(
        "thresholded",
        Inputs(threshold=float),
        Outputs(txt=Txt),
        "printf %s {threshold} > {txt}",
        input_defaults={"threshold": 0.05},
    )
    higher_default = Rule(
        "thresholded",
        Inputs(threshold=float),
        Outputs(txt=Txt),
        "printf %s {threshold} > {txt}",
        input_defaults={"threshold": 0.10},
    )
    lower_pipeline = Pipeline(DAG(tmp_path / "lower"))
    higher_pipeline = Pipeline(DAG(tmp_path / "higher"))

    assert (
        lower_default(lower_pipeline).provenance_hash
        != higher_default(higher_pipeline).provenance_hash
    )
    assert (
        lower_default(lower_pipeline, threshold=0.20).provenance_hash
        == higher_default(higher_pipeline, threshold=0.20).provenance_hash
    )


def test_command_decorator_rejects_wrongly_typed_config_default():
    """An invalid default is a broken rule schema and must fail at declaration."""

    with pytest.raises(TypeError, match="'count' expected"):

        @command("printf %s {count} > {txt}")
        def make_txt(count: int = "invalid"):
            txt = output(Txt)
            return txt


def test_mixed_nodetype_union_accepts_matching_scalar_default(tmp_path):
    """A hybrid default may select a declared scalar arm, including None."""

    @command("printf %s {source} > {txt}")
    def consume(source: Txt | None = None):
        txt = output(Txt)
        return txt

    pipeline = Pipeline(DAG(tmp_path))
    omitted = consume(pipeline)
    explicit = consume(pipeline, None)

    assert omitted is explicit
    assert omitted.parents == []
    assert omitted.config == {}
    assert dict(omitted.rule_call.input_values) == {"source": None}


def test_mixed_nodetype_union_rejects_unmatched_defaults():
    """Hybrid defaults must match a non-Node arm when the Rule is declared."""

    with pytest.raises(TypeError, match="default for hybrid input 'source'.*expected"):

        @command("touch {txt}")
        def wrong_scalar(source: Txt | str = 42):
            txt = output(Txt)
            return txt

    with pytest.raises(TypeError, match="default for hybrid input 'source'.*expected"):

        @command("touch {txt}")
        def missing_none_arm(source: Txt | str = None):
            txt = output(Txt)
            return txt


def test_mixed_nodetype_union_accepts_factory_default(tmp_path):
    """Explicit Rule construction exposes the same hybrid-default semantics."""

    rule = Rule(
        "hybrid_default",
        Inputs(source=Txt | str),
        Outputs(log=Log),
        "printf %s {source} > {log}",
        input_defaults={"source": "auto"},
    )
    pipeline = Pipeline(DAG(tmp_path))

    assert rule(pipeline) is rule(pipeline, "auto")


def test_hybrid_positional_defaults_must_be_trailing():
    """Omitting a positional default must not shift a later required Node input."""

    with pytest.raises(TypeError, match="without a default follows"):
        Rule(
            "ambiguous_defaults",
            Inputs(optional=Txt | None, required=Upper),
            Outputs(log=Log),
            "touch {log}",
            input_defaults={"optional": None},
        )


def test_mixed_nodetype_union_rejects_node_valued_default(tmp_path):
    """Defaults cannot capture managed Nodes tied to one particular DAG."""

    pipeline = Pipeline(DAG(tmp_path))
    source = R_make_txt(pipeline, word="managed")

    with pytest.raises(TypeError, match="Node-valued default"):
        Rule(
            "captured_default",
            Inputs(source=Txt | None),
            Outputs(log=Log),
            "touch {log}",
            input_defaults={"source": source},
        )


def test_command_decorator_rejects_node_input_defaults():
    """Fixed and variadic Node inputs are dependencies and must stay explicit."""

    with pytest.raises(TypeError, match="Node input 'source' must not have a default"):

        @command("cat {source} > {txt}")
        def consume(source: Txt = None):
            txt = output(Txt)
            return txt

    with pytest.raises(TypeError, match="Node input 'sources' must not have a default"):

        @command("cat {sources} > {txt}")
        def merge(sources: tuple[Txt, ...] = ()):
            txt = output(Txt)
            return txt


def test_command_decorator_body_return_multi():
    @command("tr a-z A-Z < {txt} | tee {log} > {upper}")
    def to_upper(txt: Txt):
        upper = output(Upper)
        log = output(Log)
        return upper, log

    assert to_upper.outputs.specs == {"upper": Upper, "log": Log}


def test_command_decorator_output_assignments_ignore_arrow_annotation():
    @command("echo {word} > {txt}")
    def make_txt2(word: str) -> Upper:  # -> annotation should be ignored
        txt = output(Txt)
        return txt

    assert make_txt2.outputs.specs == {"txt": Txt}


def test_command_decorator_rejects_removed_annotation_fallback():
    """Return annotations must not silently recreate the removed output DSL."""

    with pytest.raises(ValueError, match="declaration must end with return"):

        @command("echo {word} > {txt}")
        def fallback(word: str) -> Txt:
            pass


def test_command_decorator_accepts_imported_output_alias():
    """The parser recognizes output declarations by identity, not spelling."""
    declare_output = output

    @command("echo {word} > {txt}")
    def make_txt(word: str):
        txt = declare_output(Txt)
        return txt

    assert make_txt.outputs.specs == {"txt": Txt}


def test_command_decorator_rejects_invalid_output_declarations():
    """Malformed declarations fail at import time with actionable errors."""
    with pytest.raises(ValueError, match="body may contain only"):

        @command("touch {txt}")
        def chained(word: str):
            txt = alias = output(Txt)
            return txt

    with pytest.raises(ValueError, match="body may contain only"):

        @command("touch {txt}")
        def destructured(word: str):
            txt, alias = output(Txt)
            return txt

    with pytest.raises(ValueError, match="body may contain only"):

        @command("touch {txt}")
        def nested(word: str):
            if word:
                txt = output(Txt)
            return txt

    with pytest.raises(ValueError, match="final return must contain only"):

        @command("touch {txt}")
        def direct_return(word: str):
            return output(Txt)

    with pytest.raises(ValueError, match="each output must be returned exactly once"):

        @command("touch {txt}")
        def duplicate_return(word: str):
            txt = output(Txt)
            return txt, txt

    with pytest.raises(ValueError, match="declared outputs not returned"):

        @command("touch {txt}")
        def unused(word: str):
            txt = output(Txt)
            log = output(Log)
            return txt

    with pytest.raises(ValueError, match="undeclared outputs returned"):

        @command("touch {txt}")
        def undeclared(word: str):
            txt = output(Txt)
            return other

    with pytest.raises(ValueError, match="exactly one positional NodeType"):

        @command("touch {txt}")
        def wrong_arity(word: str):
            txt = output()
            return txt

    with pytest.raises(ValueError, match="concrete NodeType name"):

        @command("touch {txt}")
        def expression_type(word: str):
            txt = output(Txt())
            return txt

    with pytest.raises(TypeError, match="must be a NodeType"):

        @command("touch {txt}")
        def non_node_type(word: str):
            txt = output(str)
            return txt

    with pytest.raises(ValueError, match="duplicate output declaration"):

        @command("touch {txt}")
        def duplicate_declaration(word: str):
            txt = output(Txt)
            txt = output(Txt)
            return txt

    with pytest.raises(ValueError, match=r"Type\[name\] output syntax was removed"):

        @command("touch {txt}")
        def removed_syntax(word: str):
            return Txt[txt]


def test_output_is_declaration_only():
    """Calling output at runtime explains that it only belongs in declarations."""
    with pytest.raises(RuntimeError, match="declaration-only"):
        output(Txt)


def test_command_decorator_preserves_runtime_shape_and_fingerprint():
    """The lint-clean declaration changes typing syntax, not rule identity or values."""

    @command("echo {word} > {txt}")
    def make_txt(word: str):
        txt = output(Txt)
        return txt

    explicit = Rule(
        "make_txt",
        Inputs(word=str),
        Outputs(txt=Txt),
        "echo {word} > {txt}",
    )
    assert (
        make_txt(P, word="same").provenance_hash
        == explicit(P, word="same").provenance_hash
    )

    @command("tr a-z A-Z < {txt} | tee {log} > {upper}")
    def to_upper(txt: Txt):
        upper = output(Upper)
        log = output(Log)
        return upper, log

    result = to_upper(P, make_txt(P, word="same"))
    assert tuple(result) == (result.upper, result.log)
    assert result.upper.output_name == "upper"
    assert result.log.output_name == "log"


def test_command_factory_preserves_order_and_doc():
    rule = command(
        "tool --input {text} --workdir {workdir}",
        Inputs(text=str),
        Outputs(left=Txt, right=Log),
        Constraints(threads=2),
        name="factory_rule",
        doc="Factory documentation.",
    )
    assert rule.__name__ == "factory_rule"
    assert rule.info == "Factory documentation."
    assert rule.resources["threads"] == 2
    result = rule(P, text="value")
    assert result._fields == ("left", "right")
    assert result.left.node_type is Txt
    assert result.right.node_type is Log


def test_command_factory_and_rule_accept_input_defaults(tmp_path):
    """Both explicit construction paths must expose the same default semantics."""

    factory = command(
        "printf '%s %s' {text} {suffix} > {txt}",
        Inputs(text=str, suffix=str | None),
        Outputs(txt=Txt),
        name="factory_default",
        input_defaults={"text": "factory", "suffix": None},
    )
    direct = Rule(
        "direct_default",
        Inputs(text=str),
        Outputs(txt=Txt),
        "printf %s {text} > {txt}",
        input_defaults={"text": "direct"},
    )
    pipeline = Pipeline(DAG(tmp_path))

    assert factory(pipeline).config == {"text": "factory", "suffix": None}
    assert factory(pipeline) is factory(
        pipeline,
        text="factory",
        suffix=None,
    )
    assert direct(pipeline).config == {"text": "direct"}


def test_rule_rejects_defaults_for_unknown_and_node_inputs():
    """A defaults mapping may name only declared scalar/config inputs."""

    with pytest.raises(TypeError, match="unknown input defaults.*missing"):
        Rule(
            "unknown_default",
            Inputs(text=str),
            Outputs(txt=Txt),
            "touch {txt}",
            input_defaults={"missing": "value"},
        )

    with pytest.raises(TypeError, match="Node input 'source' must not have a default"):
        Rule(
            "node_default",
            Inputs(source=Txt),
            Outputs(txt=Txt),
            "cat {source} > {txt}",
            input_defaults={"source": None},
        )

    with pytest.raises(TypeError, match="'count' expected"):
        Rule(
            "wrong_type_default",
            Inputs(count=int),
            Outputs(txt=Txt),
            "printf %s {count} > {txt}",
            input_defaults={"count": "invalid"},
        )


def test_decorator_input_defaults_constraint_remains_a_resource():
    """The factory keyword must not steal an existing decorator constraint name."""

    @command("printf %s {text} > {txt}", input_defaults=2)
    def make_txt(text: str):
        txt = output(Txt)
        return txt

    assert make_txt.resources["input_defaults"] == 2


def test_registry_construction_api_is_not_exported():
    import necroflow

    assert not hasattr(necroflow, "Rules")
    assert hasattr(necroflow, "Inputs")
    assert hasattr(necroflow, "Outputs")
    assert hasattr(necroflow, "Constraints")


def test_command_decorator_rejects_unannotated_input_without_placeholder():
    """Every signature parameter must be typed even when the command omits it."""
    with pytest.raises(TypeError, match="missing type annotations.*word"):

        @command("touch {txt}")
        def make_txt(word):
            txt = output(Txt)
            return txt


def test_command_decorator_rejects_unannotated_default():
    """A Python default must not hide an input omitted from the rule schema."""
    with pytest.raises(TypeError, match="missing type annotations.*mode"):

        @command("touch {txt}")
        def make_txt(mode="fast"):
            txt = output(Txt)
            return txt


def test_command_decorator_reports_all_unannotated_inputs():
    """One declaration error should identify every parameter requiring a type."""
    with pytest.raises(
        TypeError, match=r"missing type annotations.*\['word', 'mode'\]"
    ):

        @command("touch {txt}")
        def make_txt(word, mode="fast"):
            txt = output(Txt)
            return txt
