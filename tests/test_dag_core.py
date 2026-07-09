"""Tests for dag.py core: path generation, command resolution, hashing,
provenance, Inputs/Outputs validation, NodeType subtyping, pipeline_label."""
import pytest
from pathlib import Path
import necroflow.dag as dag_core
from necroflow import NodeType, Inputs, Outputs, Constraints, Rules, Pipeline
from necroflow.dag import (
    _accumulated_config,
    resolve_paths, resolve_command, write_dependencies,
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


R = Rules()
R.register("make_txt",       Inputs(word=str),       Outputs(txt=Txt),                    "echo {word} > {txt}")
R.register("make_sorted_txt",Inputs(word=str),       Outputs(stxt=SortedTxt),             "echo {word} | sort > {stxt}")
R.register("to_upper",       Inputs(txt=Txt, n=int), Outputs(upper=Upper, log=Log),       "tr a-z A-Z < {txt} | head -{n} | tee {log} > {upper}")
R.register("sort_txt",       Inputs(txt=SortedTxt),  Outputs(sorted_txt=SortedTxt),       "sort {txt} > {sorted_txt}")


# ── path generation ───────────────────────────────────────────────────────────

def test_resolve_paths_structure(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    assert txt.path == tmp_path / "make_txt" / txt.fingerprint / "out.txt"


def test_resolve_paths_cooutputs_share_dir(tmp_path):
    txt = R.make_txt(word="hi")
    upper, log = R.to_upper(txt, n=3)
    resolve_paths([txt, upper, log], tmp_path)
    assert upper.path.parent == log.path.parent
    assert upper.path != log.path


def test_resolve_paths_hash_is_16_chars(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    assert len(txt.path.parent.name) == 16


def test_resolve_paths_rejects_component_over_name_max(tmp_path, monkeypatch):
    class LongName(NodeType):
        filename = "x" * 11

    r = Rules()
    r.register("make_long_name", Inputs(word=str), Outputs(out=LongName), "echo {word} > {out}")
    node = r.make_long_name(word="hi")
    monkeypatch.setattr(dag_core, "_filesystem_limits", lambda path: (10, 4096))

    with pytest.raises(ValueError, match="NAME_MAX"):
        resolve_paths([node], tmp_path)


def test_resolve_paths_rejects_total_path_over_path_max(tmp_path, monkeypatch):
    node = R.make_txt(word="hi")
    monkeypatch.setattr(dag_core, "_filesystem_limits", lambda path: (255, 20))

    with pytest.raises(ValueError, match="PATH_MAX"):
        resolve_paths([node], tmp_path)


# ── fingerprinting ────────────────────────────────────────────────────────────

def test_fingerprint_stable():
    txt1 = R.make_txt(word="hi")
    txt2 = R.make_txt(word="hi")
    assert txt1.fingerprint == txt2.fingerprint


def test_fingerprint_differs_on_config():
    txt_a = R.make_txt(word="hello")
    txt_b = R.make_txt(word="world")
    assert txt_a.fingerprint != txt_b.fingerprint


def test_fingerprint_differs_on_parent():
    txt_a = R.make_txt(word="hello")
    txt_b = R.make_txt(word="world")
    upper_a, _ = R.to_upper(txt_a, n=1)
    upper_b, _ = R.to_upper(txt_b, n=1)
    assert upper_a.fingerprint != upper_b.fingerprint


def test_fingerprint_changes_on_inputs_type_change():
    """Changing the declared Inputs NodeType must change the fingerprint."""
    class FastqA(NodeType): pass
    class FastqB(NodeType): pass

    Ra = Rules()
    Ra.register("raw", Inputs(path=str), Outputs(fastq=FastqA), "touch {fastq}")
    Ra.register("align", Inputs(fastq=FastqA, ref=str), Outputs(txt=Txt), "touch {txt}")

    Rb = Rules()
    Rb.register("raw", Inputs(path=str), Outputs(fastq=FastqB), "touch {fastq}")
    Rb.register("align", Inputs(fastq=FastqB, ref=str), Outputs(txt=Txt), "touch {txt}")

    bam_a = Ra.align(Ra.raw(path="/d/s.fq"), ref="hg38")
    bam_b = Rb.align(Rb.raw(path="/d/s.fq"), ref="hg38")
    assert bam_a.fingerprint != bam_b.fingerprint


def test_fingerprint_changes_on_outputs_type_change():
    """Changing the declared Outputs NodeType must change the fingerprint."""
    class BamA(NodeType): filename = "aligned.bam"
    class BamB(NodeType): filename = "aligned.bam"

    Ra = Rules()
    Ra.register("align", Inputs(path=str), Outputs(bam=BamA), "touch {bam}")

    Rb = Rules()
    Rb.register("align", Inputs(path=str), Outputs(bam=BamB), "touch {bam}")

    bam_a = Ra.align(path="/d/s.fq")
    bam_b = Rb.align(path="/d/s.fq")
    assert bam_a.fingerprint != bam_b.fingerprint


def test_node_key_unique_for_cooutputs():
    txt = R.make_txt(word="hi")
    upper, log = R.to_upper(txt, n=1)
    assert upper.key != log.key


def test_node_key_contains_rule_and_filename():
    txt = R.make_txt(word="hi")
    assert txt.key.startswith("make_txt/")
    assert txt.key.endswith("/out.txt")


# ── command resolution ────────────────────────────────────────────────────────

def test_resolve_command_input_substitution(tmp_path):
    txt = R.make_txt(word="hi")
    upper, _ = R.to_upper(txt, n=2)
    resolve_paths([txt, upper], tmp_path)
    cmd = resolve_command(upper)
    assert str(txt.path) in cmd


def test_resolve_command_config_substitution(tmp_path):
    txt = R.make_txt(word="hello")
    resolve_paths([txt], tmp_path)
    cmd = resolve_command(txt)
    assert "hello" in cmd


def test_resolve_command_output_substitution(tmp_path):
    txt = R.make_txt(word="hi")
    upper, log = R.to_upper(txt, n=1)
    resolve_paths([txt, upper, log], tmp_path)
    cmd = resolve_command(upper)
    assert str(upper.path) in cmd
    assert str(log.path) in cmd


def test_resolve_command_none_for_no_command(tmp_path):
    # node with no command returns None
    txt = R.make_txt(word="hi")
    txt.command = None
    resolve_paths([txt], tmp_path)
    assert resolve_command(txt) is None


def test_resolve_command_direct_constraint_placeholders(tmp_path):
    r = Rules()
    r.register(
        "constrained",
        Inputs(word=str),
        Outputs(txt=Txt),
        "tool --threads {threads} --ram {ram} --gpu {constraint:gpu} --word {word} > {txt}",
        Constraints(threads=8, ram="4Gi", gpu=2),
    )
    txt = r.constrained(word="hi")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool --threads 8 --ram 4Gi --gpu 2 --word hi > {txt.path}"


def test_resolve_command_threads_defaults_to_one(tmp_path):
    r = Rules()
    r.register("default_threads", Inputs(word=str), Outputs(txt=Txt), "tool --threads {threads} > {txt}")
    txt = r.default_threads(word="hi")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool --threads 1 > {txt.path}"


def test_resolve_command_preserves_escaped_shell_braces(tmp_path):
    r = Rules()
    r.register("brace", Inputs(word=str), Outputs(txt=Txt), "printf '%s\n' {{left,right}} {word} > {txt}")
    txt = r.brace(word="hi")

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"printf '%s\n' {{left,right}} hi > {txt.path}"


def test_constraint_placeholder_forces_constraint_when_config_name_collides(tmp_path):
    r = Rules()
    r.register(
        "colliding_threads",
        Inputs(threads=int),
        Outputs(txt=Txt),
        "tool --arg {threads} --scheduler {constraint:threads} > {txt}",
        Constraints(threads=8),
    )
    txt = r.colliding_threads(threads=2)

    resolve_paths([txt], tmp_path)

    assert resolve_command(txt) == f"tool --arg 2 --scheduler 8 > {txt.path}"


def test_unknown_constraint_placeholder_is_rejected():
    with pytest.raises(ValueError, match=r"unknown placeholders: \['ram'\]"):
        Rules().register("bad_ram", Inputs(word=str), Outputs(txt=Txt), "tool --ram {ram} > {txt}")

    with pytest.raises(ValueError, match=r"unknown constraint placeholders: \['gpu'\]"):
        Rules().register("bad_gpu", Inputs(word=str), Outputs(txt=Txt), "tool --gpu {constraint:gpu} > {txt}")


# ── accumulated config ────────────────────────────────────────────────────────

def test_accumulated_config_single_node():
    txt = R.make_txt(word="hello")
    cfg = _accumulated_config(txt)
    assert cfg["word"] == "hello"


def test_accumulated_config_multi_hop():
    txt = R.make_txt(word="hello")
    upper, _ = R.to_upper(txt, n=5)
    cfg = _accumulated_config(upper)
    assert cfg["word"] == "hello"
    assert cfg["n"] == 5


# ── provenance ────────────────────────────────────────────────────────────────

def test_write_dependencies_creates_file(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    write_dependencies(txt)
    assert (txt.path.parent / ".rip" / "dependencies.toml").exists()


def test_write_dependencies_content(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    write_dependencies(txt)
    content = (txt.path.parent / ".rip" / "dependencies.toml").read_text()
    assert "make_txt" in content
    assert "hi" in content


# ── Inputs/Outputs validation ─────────────────────────────────────────────────

def test_wrong_nodetype_raises():
    txt = R.make_txt(word="hi")
    upper, _ = R.to_upper(txt, n=1)
    with pytest.raises(TypeError):
        R.to_upper(upper, n=1)   # Upper passed where Txt expected


def test_wrong_config_type_raises():
    txt = R.make_txt(word="hi")
    with pytest.raises(TypeError):
        R.to_upper(txt, n="not_an_int")   # str passed where int expected


def test_missing_positional_input_raises():
    with pytest.raises(TypeError, match="missing required inputs"):
        R.to_upper(n=1)   # txt input omitted


def test_missing_config_input_raises():
    """Rule calls must reject omitted required config inputs immediately.

    This keeps malformed DAGs from being accepted and then failing later
    during command formatting or execution.
    """
    with pytest.raises(TypeError, match="missing required inputs"):
        R.make_txt()   # word config omitted


def test_extra_positional_input_raises():
    """Rule calls must reject undeclared positional node inputs.

    Extra nodes should not silently become parents, because that creates
    dependencies outside the rule's declared input contract.
    """
    txt = R.make_txt(word="hi")
    extra = R.make_txt(word="extra")
    with pytest.raises(TypeError, match="too many positional inputs"):
        R.to_upper(txt, extra, n=1)


def test_subtype_accepted():
    # SortedTxt is a subclass of Txt — to_upper accepts Txt and must accept SortedTxt too
    stxt = R.make_sorted_txt(word="hi")
    result, _ = R.to_upper(stxt, n=1)
    assert result is not None


# ── pipeline_label ────────────────────────────────────────────────────────────

def test_pipeline_label_stamped():
    P = Pipeline()
    P.txt = R.make_txt(word="hi")
    assert P.txt.pipeline_label == "txt"


def test_pipeline_label_cooutputs():
    P = Pipeline()
    P.txt = R.make_txt(word="hi")
    P.upper, P.log = R.to_upper(P.txt, n=1)
    assert P.upper.pipeline_label == "upper"
    assert P.log.pipeline_label == "log"


def test_pipeline_duplicate_raises():
    P = Pipeline()
    P.txt = R.make_txt(word="hi")
    with pytest.raises(ValueError):
        P.txt = R.make_txt(word="world")


# ── command placeholder validation ────────────────────────────────────────────

def test_register_unknown_placeholder_raises():
    r = Rules()
    with pytest.raises(ValueError, match="unknown placeholders"):
        r.register("bad", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt} {typo}")


def test_register_missing_output_raises():
    r = Rules()
    with pytest.raises(ValueError, match="outputs not referenced in command"):
        r.register("bad", Inputs(word=str), Outputs(txt=Txt, log=Log), "echo {word} > {txt}")


def test_register_list_command_missing_output_raises():
    r = Rules()
    with pytest.raises(ValueError, match="outputs not referenced in command"):
        r.register("bad", Inputs(word=str), Outputs(txt=Txt, log=Log), ["echo {word} > {txt}", "echo done"])


def test_register_unreferenced_input_is_allowed():
    r = Rules()
    r.register("ok", Inputs(word=str), Outputs(txt=Txt), "touch {txt}")
    assert r.ok is not None


def test_register_valid_command_ok():
    r = Rules()
    r.register("good", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}")
    assert r.good is not None


def test_register_repeat_metadata():
    r = Rules()
    r.register("repeat_rule", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=3)
    assert r.repeat_rule.repeat == 3
    assert "repeat" not in r.repeat_rule.resources


def test_register_repeat_must_be_positive_int():
    r = Rules()
    with pytest.raises(ValueError, match="repeat must be a positive integer"):
        r.register("bad_repeat", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=0)
    with pytest.raises(ValueError, match="repeat must be a positive integer"):
        r.register("bad_repeat_bool", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=True)


def test_repeat_does_not_affect_fingerprint():
    r1 = Rules()
    r1.register("make", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=1)
    r2 = Rules()
    r2.register("make", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}", repeat=3)
    assert r1.make(word="hi").fingerprint == r2.make(word="hi").fingerprint


# ── body return style ─────────────────────────────────────────────────────────

def test_command_decorator_body_return_single():
    r = Rules()

    @r.command("echo {word} > {txt}")
    def make_txt(word: str):
        return Txt[txt]

    assert r.make_txt.outputs.specs == {"txt": Txt}


def test_command_decorator_accepts_repeat_and_constraints():
    r = Rules()

    @r.command("echo {word} > {txt}", threads=2, repeat=4)
    def make_txt(word: str):
        return Txt[txt]

    assert r.make_txt.repeat == 4
    assert r.make_txt.resources == {"threads": 2}


def test_command_decorator_body_return_multi():
    r = Rules()

    @r.command("tr a-z A-Z < {txt} | tee {log} > {upper}")
    def to_upper(txt: Txt):
        return Upper[upper], Log[log]

    assert r.to_upper.outputs.specs == {"upper": Upper, "log": Log}


def test_command_decorator_body_return_ignores_arrow_annotation():
    r = Rules()

    @r.command("echo {word} > {txt}")
    def make_txt2(word: str) -> Upper:  # -> annotation should be ignored
        return Txt[txt]

    assert r.make_txt2.outputs.specs == {"txt": Txt}


def test_command_decorator_annotation_fallback():
    """-> annotation fallback still works when no return statement in body."""
    r = Rules()
    r.register("fallback", Inputs(word=str), Outputs(txt=Txt), "echo {word} > {txt}")
    assert r.fallback.outputs.specs == {"txt": Txt}


def test_rule_decorator_accepts_repeat_and_constraints():
    r = Rules()

    @r.rule(threads=2, repeat=5)
    def make_txt(word: str):
        command = "echo {word} > {txt}"
        return Txt[txt]

    assert r.make_txt.repeat == 5
    assert r.make_txt.resources == {"threads": 2}


def test_command_unannotated_input_raises():
    """Unannotated input is invisible to the decorator → unknown placeholder → ValueError."""
    r = Rules()
    with pytest.raises(ValueError, match="unknown placeholders"):
        @r.command("echo {word} > {txt}")
        def make_txt(word):   # missing annotation — word absent from inputs_specs
            return Txt[txt]
