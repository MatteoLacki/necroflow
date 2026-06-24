"""Tests for iter_configs() / __grid expansion."""
import pytest
import tomlkit
from necroflow.grid import iter_configs


def parse(toml_str: str):
    return tomlkit.parse(toml_str)


# ── no-grid passthrough ───────────────────────────────────────────────────────

def test_no_grid_yields_one():
    doc = parse('word = "hello"\nn = 3\n')
    results = list(iter_configs(doc, base_stem="exp"))
    assert len(results) == 1


def test_no_grid_label_is_base_stem():
    doc = parse('word = "hello"\n')
    (label, cfg), = iter_configs(doc, base_stem="myexp")
    assert label == "myexp"


def test_no_grid_plain_dict():
    doc = parse('word = "hello"\nn = 3\n')
    (_, cfg), = iter_configs(doc, base_stem="exp")
    assert cfg == {"word": "hello", "n": 3}


# ── 1-D grid ─────────────────────────────────────────────────────────────────

def test_1d_grid_count():
    doc = parse('word__grid = ["a", "b", "c"]\n')
    results = list(iter_configs(doc, base_stem="exp"))
    assert len(results) == 3


def test_1d_grid_values():
    doc = parse('word__grid = ["hello", "world"]\n')
    results = list(iter_configs(doc, base_stem="exp"))
    words = [cfg["word"] for _, cfg in results]
    assert set(words) == {"hello", "world"}


def test_1d_grid_label_contains_value():
    doc = parse('word__grid = ["alpha", "beta"]\n')
    labels = [label for label, _ in iter_configs(doc, base_stem="exp")]
    assert any("alpha" in l for l in labels)
    assert any("beta" in l for l in labels)


# ── 2-D grid (Cartesian product) ──────────────────────────────────────────────

def test_2d_grid_count():
    doc = parse('word__grid = ["a", "b"]\nn__grid = [1, 2, 3]\n')
    results = list(iter_configs(doc, base_stem="exp"))
    assert len(results) == 6


def test_2d_grid_all_combinations():
    doc = parse('word__grid = ["x", "y"]\nn__grid = [10, 20]\n')
    pairs = [(cfg["word"], cfg["n"]) for _, cfg in iter_configs(doc, base_stem="exp")]
    assert set(pairs) == {("x", 10), ("x", 20), ("y", 10), ("y", 20)}


def test_2d_grid_unique_labels():
    doc = parse('word__grid = ["x", "y"]\nn__grid = [10, 20]\n')
    labels = [label for label, _ in iter_configs(doc, base_stem="exp")]
    assert len(labels) == len(set(labels))


# ── plain types in output ─────────────────────────────────────────────────────

def test_plain_types_returned():
    doc = parse('word__grid = ["hello"]\nn__grid = [5]\n')
    (_, cfg), = iter_configs(doc, base_stem="exp")
    # tomlkit proxies are subclasses of built-in types; isinstance checks what factory code uses
    assert isinstance(cfg["word"], str)
    assert isinstance(cfg["n"], int)


# ── base_stem in label ────────────────────────────────────────────────────────

def test_base_stem_in_label():
    doc = parse('word__grid = ["a", "b"]\n')
    labels = [label for label, _ in iter_configs(doc, base_stem="mystem")]
    assert all(label.startswith("mystem") for label in labels)
