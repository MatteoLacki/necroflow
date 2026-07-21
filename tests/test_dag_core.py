"""Tests for dag.py core: path generation, command resolution, hashing,
provenance, Inputs/Outputs validation, NodeType subtyping, pipeline_label."""

from necroflow.rules import Constraints, Inputs, Outputs, Rule
from necroflow import command, output

import pytest
from pathlib import Path
import necroflow.dag as dag_core
from necroflow import NodeType, Pipeline
from necroflow.dag import (
    _accumulated_config,
    resolve_paths,
    resolve_command,
    write_dependencies,
)

# ── fixtures ──────────────────────────────────────────────────────────────────


class Txt(NodeType):
    filename = "out.txt"


class Upper(NodeType):
    filename = "upper.txt"


class Log(NodeType):
    filename = "log.txt"


class SortedTxt(Txt):
    filename = "sorted.txt"


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


# ── path generation ───────────────────────────────────────────────────────────


def test_resolve_paths_structure(tmp_path):
    txt = R_make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    assert txt.path == tmp_path / "make_txt" / txt.fingerprint / "out.txt"


def test_resolve_paths_cooutputs_share_dir(tmp_path):
    txt = R_make_txt(word="hi")
    upper, log = R_to_upper(txt, n=3)
    resolve_paths([txt, upper, log], tmp_path)
    assert upper.path.parent == log.path.parent
    assert upper.path != log.path


def test_resolve_paths_hash_is_16_chars(tmp_path):
    txt = R_make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    assert len(txt.path.parent.name) == 16


def test_resolve_paths_rejects_component_over_name_max(tmp_path, monkeypatch):
    class LongName(NodeType):
        filename = "x" * 11

    r_make_long_name = Rule(
        "make_long_name", Inputs(word=str), Outputs(out=LongName), "echo {word} > {out}"
    )
    node = r_make_long_name(word="hi")
    monkeypatch.setattr(dag_core, "_filesystem_limits", lambda path: (10, 4096))

    with pytest.raises(ValueError, match="NAME_MAX"):
        resolve_paths([node], tmp_path)


def test_resolve_paths_rejects_total_path_over_path_max(tmp_path, monkeypatch):
    node = R_make_txt(word="hi")
    monkeypatch.setattr(dag_core, "_filesystem_limits", lambda path: (255, 20))

    with pytest.raises(ValueError, match="PATH_MAX"):
        resolve_paths([node], tmp_path)


# ── fingerprinting ────────────────────────────────────────────────────────────


def test_fingerprint_stable():
    txt1 = R_make_txt(word="hi")
    txt2 = R_make_txt(word="hi")
    assert txt1.fingerprint == txt2.fingerprint


def test_fingerprint_differs_on_config():
    txt_a = R_make_txt(word="hello")
    txt_b = R_make_txt(word="world")
    assert txt_a.fingerprint != txt_b.fingerprint


def test_fingerprint_differs_on_parent():
    txt_a = R_make_txt(word="hello")
    txt_b = R_make_txt(word="world")
    upper_a, _ = R_to_upper(txt_a, n=1)
    upper_b, _ = R_to_upper(txt_b, n=1)
    assert upper_a.fingerprint != upper_b.fingerprint


def test_fingerprint_changes_on_inputs_type_change():
    """Changing the declared Inputs NodeType must change the fingerprint."""

    class FastqA(NodeType):
        pass

    class FastqB(NodeType):
        pass

    Ra_raw = Rule("raw", Inputs(path=str), Outputs(fastq=FastqA), "touch {fastq}")
    Ra_align = Rule(
        "align", Inputs(fastq=FastqA, ref=str), Outputs(txt=Txt), "touch {txt}"
    )
    Rb_raw = Rule("raw", Inputs(path=str), Outputs(fastq=FastqB), "touch {fastq}")
    Rb_align = Rule(
        "align", Inputs(fastq=FastqB, ref=str), Outputs(txt=Txt), "touch {txt}"
    )

    bam_a = Ra_align(Ra_raw(path="/d/s.fq"), ref="hg38")
    bam_b = Rb_align(Rb_raw(path="/d/s.fq"), ref="hg38")
    assert bam_a.fingerprint != bam_b.fingerprint


def test_fingerprint_changes_on_outputs_type_change():
    """Changing the declared Outputs NodeType must change the fingerprint."""

    class BamA(NodeType):
        filename = "aligned.bam"

    class BamB(NodeType):
        filename = "aligned.bam"

    Ra_align = Rule("align", Inputs(path=str), Outputs(bam=BamA), "touch {bam}")
    Rb_align = Rule("align", Inputs(path=str), Outputs(bam=BamB), "touch {bam}")

    bam_a = Ra_align(path="/d/s.fq")
    bam_b = Rb_align(path="/d/s.fq")
    assert bam_a.fingerprint != bam_b.fingerprint


def test_node_key_unique_for_cooutputs():
    txt = R_make_txt(word="hi")
    upper, log = R_to_upper(txt, n=1)
    assert upper.key != log.key


def test_node_key_contains_rule_and_filename():
    txt = R_make_txt(word="hi")
    assert txt.key.startswith("make_txt/")
    assert txt.key.endswith("/out.txt")


# ── command resolution ────────────────────────────────────────────────────────


def test_resolve_command_input_substitution(tmp_path):
    txt = R_make_txt(word="hi")
    upper, _ = R_to_upper(txt, n=2)
    resolve_paths([txt, upper], tmp_path)
    cmd = resolve_command(upper)
    assert str(txt.path) in cmd


def test_resolve_command_config_substitution(tmp_path):
    txt = R_make_txt(word="hello")
    resolve_paths([txt], tmp_path)
    cmd = resolve_command(txt)
    assert "hello" in cmd


def test_resolve_command_quotes_string_config_for_shell_commands(tmp_path):
    r_filter_txt = Rule(
        "filter_txt",
        Inputs(filter=str),
        Outputs(txt=Txt),
        "tool --filter {filter} > {txt}",
    )
    txt = r_filter_txt(filter="a > b")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool --filter 'a > b' > {txt.path}"


def test_resolve_command_scalar_config_stays_bare_when_shell_safe(tmp_path):
    r_number_txt = Rule(
        "number_txt", Inputs(n=int), Outputs(txt=Txt), "tool -n {n} > {txt}"
    )
    txt = r_number_txt(n=5)

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool -n 5 > {txt.path}"


def test_resolve_command_list_commands_are_not_shell_quoted(tmp_path):
    r_list_filter = Rule(
        "list_filter",
        Inputs(filter=str),
        Outputs(txt=Txt),
        ["tool", "--filter", "{filter}", "--out", "{txt}"],
    )
    txt = r_list_filter(filter="a > b")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == [
        "tool",
        "--filter",
        "a > b",
        "--out",
        str(txt.path),
    ]


def test_resolve_command_output_substitution(tmp_path):
    txt = R_make_txt(word="hi")
    upper, log = R_to_upper(txt, n=1)
    resolve_paths([txt, upper, log], tmp_path)
    cmd = resolve_command(upper)
    assert str(upper.path) in cmd
    assert str(log.path) in cmd


def test_resolve_command_none_for_no_command(tmp_path):
    # node with no command returns None
    txt = R_make_txt(word="hi")
    txt.command = None
    resolve_paths([txt], tmp_path)
    assert resolve_command(txt) is None


def test_resolve_command_direct_constraint_placeholders(tmp_path):
    r_constrained = Rule(
        "constrained",
        Inputs(word=str),
        Outputs(txt=Txt),
        "tool --threads {threads} --ram {ram} --gpu {constraint:gpu} --word {word} > {txt}",
        Constraints(threads=8, ram="4Gi", gpu=2),
    )
    txt = r_constrained(word="hi")

    resolve_paths([txt], tmp_path)

    assert (
        resolve_command(txt)
        == f"tool --threads 8 --ram 4Gi --gpu 2 --word hi > {txt.path}"
    )


def test_resolve_command_threads_defaults_to_one(tmp_path):
    r_default_threads = Rule(
        "default_threads",
        Inputs(word=str),
        Outputs(txt=Txt),
        "tool --threads {threads} > {txt}",
    )
    txt = r_default_threads(word="hi")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool --threads 1 > {txt.path}"


def test_resolve_command_preserves_escaped_shell_braces(tmp_path):
    r_brace = Rule(
        "brace",
        Inputs(word=str),
        Outputs(txt=Txt),
        "printf '%s\n' {{left,right}} {word} > {txt}",
    )
    txt = r_brace(word="hi")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"printf '%s\n' {{left,right}} hi > {txt.path}"


def test_resolve_command_substitutes_union_typed_input(tmp_path):
    # Regression test: resolve_command used to build its {name} substitution dict by
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

    src = R_make_txt(word="hi")
    doc = read_either(src)

    resolve_paths([src, doc], tmp_path)
    cmd = resolve_command(doc)
    assert str(src.path) in cmd
    assert str(doc.path) in cmd


def test_constraint_placeholder_forces_constraint_when_config_name_collides(tmp_path):
    r_colliding_threads = Rule(
        "colliding_threads",
        Inputs(threads=int),
        Outputs(txt=Txt),
        "tool --arg {threads} --scheduler {constraint:threads} > {txt}",
        Constraints(threads=8),
    )
    txt = r_colliding_threads(threads=2)

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool --arg 2 --scheduler 8 > {txt.path}"


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
    txt = R_make_txt(word="hello")
    cfg = _accumulated_config(txt)
    assert cfg["word"] == "hello"


def test_accumulated_config_multi_hop():
    txt = R_make_txt(word="hello")
    upper, _ = R_to_upper(txt, n=5)
    cfg = _accumulated_config(upper)
    assert cfg["word"] == "hello"
    assert cfg["n"] == 5


# ── provenance ────────────────────────────────────────────────────────────────


def test_write_dependencies_creates_file(tmp_path):
    txt = R_make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    write_dependencies(txt)
    assert (txt.path.parent / ".rip" / "dependencies.toml").exists()


def test_write_dependencies_content(tmp_path):
    txt = R_make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    write_dependencies(txt)
    content = (txt.path.parent / ".rip" / "dependencies.toml").read_text()
    assert "make_txt" in content
    assert "hi" in content


# ── Inputs/Outputs validation ─────────────────────────────────────────────────


def test_wrong_nodetype_raises():
    txt = R_make_txt(word="hi")
    upper, _ = R_to_upper(txt, n=1)
    with pytest.raises(TypeError):
        R_to_upper(upper, n=1)  # Upper passed where Txt expected


def test_wrong_config_type_raises():
    txt = R_make_txt(word="hi")
    with pytest.raises(TypeError):
        R_to_upper(txt, n="not_an_int")  # str passed where int expected


def test_missing_positional_input_raises():
    with pytest.raises(TypeError, match="missing required inputs"):
        R_to_upper(n=1)  # txt input omitted


def test_missing_config_input_raises():
    """Rule calls must reject omitted required config inputs immediately.

    This keeps malformed DAGs from being accepted and then failing later
    during command formatting or execution.
    """
    with pytest.raises(TypeError, match="missing required inputs"):
        R_make_txt()  # word config omitted


def test_extra_positional_input_raises():
    """Rule calls must reject undeclared positional node inputs.

    Extra nodes should not silently become parents, because that creates
    dependencies outside the rule's declared input contract.
    """
    txt = R_make_txt(word="hi")
    extra = R_make_txt(word="extra")
    with pytest.raises(TypeError, match="too many positional inputs"):
        R_to_upper(txt, extra, n=1)


def test_subtype_accepted():
    # SortedTxt is a subclass of Txt — to_upper accepts Txt and must accept SortedTxt too
    stxt = R_make_sorted_txt(word="hi")
    result, _ = R_to_upper(stxt, n=1)
    assert result is not None


def test_nodetype_union_accepts_either_member():
    r_use_txt_or_upper = Rule(
        "use_txt_or_upper", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )

    txt = R_make_txt(word="hi")
    upper, _ = R_to_upper(txt, n=1)

    assert r_use_txt_or_upper(txt) is not None
    assert r_use_txt_or_upper(upper) is not None


def test_nodetype_union_accepts_subclass_of_member():
    r_use_txt_or_upper = Rule(
        "use_txt_or_upper", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    stxt = R_make_sorted_txt(word="hi")

    assert r_use_txt_or_upper(stxt) is not None


def test_nodetype_union_rejects_unrelated_type():
    r_use_txt_or_upper = Rule(
        "use_txt_or_upper", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    txt = R_make_txt(word="hi")
    log = r_use_txt_or_upper(txt)

    with pytest.raises(TypeError, match=r"expected Txt \| Upper"):
        r_use_txt_or_upper(log)


def test_mixed_nodetype_union_rejected_at_declaration():
    with pytest.raises(TypeError, match="mixes NodeType and non-NodeType union"):

        @command("touch {log}")
        def bad_union(data: Txt | str):
            log = output(Log)
            return log


def test_config_union_still_supported():
    r_config_union = Rule(
        "config_union",
        Inputs(value=str | int),
        Outputs(txt=Txt),
        "echo {value} > {txt}",
    )

    assert r_config_union(value="hi") is not None
    assert r_config_union(value=3) is not None
    with pytest.raises(TypeError):
        r_config_union(value=object())


def test_fingerprint_changes_for_nodetype_union_contract():
    r_single_consume = Rule(
        "consume", Inputs(data=Txt), Outputs(log=Log), "touch {log}"
    )
    r_union_consume = Rule(
        "consume", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    txt = R_make_txt(word="hi")

    assert r_single_consume(txt).fingerprint != r_union_consume(txt).fingerprint


def test_nodetype_union_fingerprint_order_is_stable():
    r_ab_consume = Rule(
        "consume", Inputs(data=Txt | Upper), Outputs(log=Log), "touch {log}"
    )
    r_ba_consume = Rule(
        "consume", Inputs(data=Upper | Txt), Outputs(log=Log), "touch {log}"
    )
    txt = R_make_txt(word="hi")

    assert r_ab_consume(txt).fingerprint == r_ba_consume(txt).fingerprint


# ── pipeline_label ────────────────────────────────────────────────────────────


def test_pipeline_label_stamped():
    P = Pipeline()
    P.txt = R_make_txt(word="hi")
    assert P.txt.pipeline_label == "txt"


def test_pipeline_label_cooutputs():
    P = Pipeline()
    P.txt = R_make_txt(word="hi")
    P.upper, P.log = R_to_upper(P.txt, n=1)
    assert P.upper.pipeline_label == "upper"
    assert P.log.pipeline_label == "log"


def test_pipeline_duplicate_raises():
    P = Pipeline()
    P.txt = R_make_txt(word="hi")
    with pytest.raises(ValueError):
        P.txt = R_make_txt(word="world")


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


def test_command_list_missing_output_is_allowed():
    r_ok = Rule(
        "bad",
        Inputs(word=str),
        Outputs(txt=Txt, log=Log),
        ["echo {word} > {txt}", "echo done"],
    )
    assert r_ok.outputs.specs == {"txt": Txt, "log": Log}


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
    assert r1_make(word="hi").fingerprint == r2_make(word="hi").fingerprint


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

    with pytest.raises(ValueError, match="Type\[name\] output syntax was removed"):

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
    assert make_txt(word="same").fingerprint == explicit(word="same").fingerprint

    @command("tr a-z A-Z < {txt} | tee {log} > {upper}")
    def to_upper(txt: Txt):
        upper = output(Upper)
        log = output(Log)
        return upper, log

    result = to_upper(make_txt(word="same"))
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
    result = rule(text="value")
    assert result._fields == ("left", "right")
    assert result.left.node_type is Txt
    assert result.right.node_type is Log


def test_registry_construction_api_is_not_exported():
    import necroflow

    assert not hasattr(necroflow, "Rules")
    assert hasattr(necroflow, "Inputs")
    assert hasattr(necroflow, "Outputs")
    assert hasattr(necroflow, "Constraints")


def test_command_unannotated_input_raises():
    """Unannotated input is invisible to the decorator → unknown placeholder → ValueError."""
    with pytest.raises(ValueError, match="unknown placeholders"):

        @command("echo {word} > {txt}")
        def make_txt(word):  # missing annotation — word absent from inputs_specs
            txt = output(Txt)
            return txt
