"""Tests for dag.py core: path generation, command resolution, hashing,
provenance, Inputs/Outputs validation, NodeType subtyping, pipeline_label."""
import pytest
from pathlib import Path
from necroflow import NodeType, Inputs, Outputs, Rules, Pipeline
from necroflow.dag import (
    _folder_hash, _node_key, _accumulated_config,
    resolve_paths, resolve_command, write_dependencies, check_cache,
)


# ── fixtures ──────────────────────────────────────────────────────────────────

class Txt(NodeType):
    name = "out.txt"

class Upper(NodeType):
    name = "upper.txt"

class Log(NodeType):
    name = "log.txt"

class SortedTxt(Txt):
    name = "sorted.txt"


R = Rules()
R.register("make_txt",       Inputs(word=str),       Outputs(txt=Txt),                    "echo {word} > {txt}")
R.register("make_sorted_txt",Inputs(word=str),       Outputs(stxt=SortedTxt),             "echo {word} | sort > {stxt}")
R.register("to_upper",       Inputs(txt=Txt, n=int), Outputs(upper=Upper, log=Log),       "tr a-z A-Z < {txt} | head -{n} | tee {log} > {upper}")
R.register("sort_txt",       Inputs(txt=SortedTxt),  Outputs(sorted_txt=SortedTxt),       "sort {txt} > {sorted_txt}")


# ── path generation ───────────────────────────────────────────────────────────

def test_resolve_paths_structure(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    assert txt.path == tmp_path / "make_txt" / _folder_hash(txt) / "out.txt"


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


# ── hashing ───────────────────────────────────────────────────────────────────

def test_folder_hash_stable():
    txt1 = R.make_txt(word="hi")
    txt2 = R.make_txt(word="hi")
    assert _folder_hash(txt1) == _folder_hash(txt2)


def test_folder_hash_differs_on_config():
    txt_a = R.make_txt(word="hello")
    txt_b = R.make_txt(word="world")
    assert _folder_hash(txt_a) != _folder_hash(txt_b)


def test_folder_hash_differs_on_parent():
    txt_a = R.make_txt(word="hello")
    txt_b = R.make_txt(word="world")
    upper_a, _ = R.to_upper(txt_a, n=1)
    upper_b, _ = R.to_upper(txt_b, n=1)
    assert _folder_hash(upper_a) != _folder_hash(upper_b)


def test_node_key_unique_for_cooutputs():
    txt = R.make_txt(word="hi")
    upper, log = R.to_upper(txt, n=1)
    assert _node_key(upper) != _node_key(log)


def test_node_key_contains_rule_and_filename():
    txt = R.make_txt(word="hi")
    key = _node_key(txt)
    assert key.startswith("make_txt/")
    assert key.endswith("/out.txt")


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
    assert (txt.path.parent / "dependencies.toml").exists()


def test_write_dependencies_content(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    write_dependencies(txt)
    content = (txt.path.parent / "dependencies.toml").read_text()
    assert "make_txt" in content
    assert "hi" in content


def test_check_cache_true(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    write_dependencies(txt)
    assert check_cache(txt) is True


def test_check_cache_false_missing_deps(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    txt.path.parent.mkdir(parents=True, exist_ok=True)
    txt.path.touch()
    assert check_cache(txt) is False


def test_check_cache_false_missing_output(tmp_path):
    txt = R.make_txt(word="hi")
    resolve_paths([txt], tmp_path)
    assert check_cache(txt) is False


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
