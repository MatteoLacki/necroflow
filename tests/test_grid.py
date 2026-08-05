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
    ((label, cfg),) = iter_configs(doc, base_stem="myexp")
    assert label == "myexp"


def test_no_grid_plain_dict():
    doc = parse('word = "hello"\nn = 3\n')
    ((_, cfg),) = iter_configs(doc, base_stem="exp")
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
    ((_, cfg),) = iter_configs(doc, base_stem="exp")
    # tomlkit proxies are subclasses of built-in types; isinstance checks what factory code uses
    assert isinstance(cfg["word"], str)
    assert isinstance(cfg["n"], int)


# ── base_stem in label ────────────────────────────────────────────────────────


def test_base_stem_in_label():
    doc = parse('word__grid = ["a", "b"]\n')
    labels = [label for label, _ in iter_configs(doc, base_stem="mystem")]
    assert all(label.startswith("mystem") for label in labels)


def test_nested_grids_expand_in_deterministic_cartesian_order():
    """Nested grid paths form stable labels and preserve declaration order."""
    doc = parse(
        "[model]\n"
        "width__grid = [64, 128]\n"
        "[model.optimizer]\n"
        "lr__grid = [0.1, 0.01]\n"
    )

    results = list(iter_configs(doc, base_stem="job"))

    assert [label for label, _ in results] == [
        "job__model_width+64__model_optimizer_lr+0p1",
        "job__model_width+64__model_optimizer_lr+0p01",
        "job__model_width+128__model_optimizer_lr+0p1",
        "job__model_width+128__model_optimizer_lr+0p01",
    ]
    assert [config["model"]["optimizer"]["lr"] for _, config in results] == [
        0.1,
        0.01,
        0.1,
        0.01,
    ]


def test_table_grid_inner_dimensions_extend_explicit_labels():
    """An explicit table label stays unique when that table contains a grid."""
    doc = parse(
        "[[model__grid]]\n"
        '__label = "family"\n'
        'name = "alpha"\n'
        "width__grid = [64, 128]\n"
    )

    results = list(iter_configs(doc, base_stem="job"))

    assert [label for label, _ in results] == [
        "job__model+family__width+64",
        "job__model+family__width+128",
    ]
    assert [config for _, config in results] == [
        {"model": {"name": "alpha", "width": 64}},
        {"model": {"name": "alpha", "width": 128}},
    ]


def test_empty_grid_is_rejected_with_its_config_path():
    """An empty grid cannot produce a concrete config and fails descriptively."""
    doc = parse("[model]\nwidth__grid = []\n")

    with pytest.raises(
        ValueError, match="model.width__grid must contain at least one value"
    ):
        list(iter_configs(doc))


@pytest.mark.parametrize(
    ("source", "expected_labels"),
    [
        (
            "[[model__grid]]\n"
            '__label = "small"\n'
            "width = 64\n"
            "[[model__grid]]\n"
            '__label = "large"\n'
            "width = 128\n",
            ["job__model+small", "job__model+large"],
        ),
        (
            "[[model__grid]]\n"
            'name = "alpha"\n'
            "width = 64\n"
            "[[model__grid]]\n"
            'name = "beta"\n'
            "width = 128\n",
            ["job__model+alpha", "job__model+beta"],
        ),
        (
            "[[model__grid]]\n" "width = 64\n" "[[model__grid]]\n" "width = 128\n",
            ["job__model+0", "job__model+1"],
        ),
    ],
)
def test_table_grids_receive_stable_human_readable_labels(source, expected_labels):
    """Table variants prefer explicit, unique-string, then numeric labels."""
    results = list(iter_configs(parse(source), base_stem="job"))

    assert [label for label, _ in results] == expected_labels
    assert all("__label" not in config["model"] for _, config in results)


def test_table_grid_labels_track_their_values_across_axes():
    """A label names a slot in the grid, so it must follow that slot's value.

    Labels are indexed by position rather than by the identity of the table
    occupying it, which is only correct if every combination in the cartesian
    product pairs a label with the table it was derived from.
    """
    doc = parse(
        "depth__grid = [1, 2]\n"
        "[[model__grid]]\n"
        '__label = "small"\n'
        'weights = "s.pt"\n'
        "[[model__grid]]\n"
        '__label = "large"\n'
        'weights = "l.pt"\n'
    )

    results = list(iter_configs(doc, base_stem="job"))

    assert len(results) == 4
    expected = {"small": "s.pt", "large": "l.pt"}
    for label, config in results:
        assert config["model"]["weights"] == expected[label.rsplit("model+", 1)[1]]


def test_partially_labelled_table_grid_falls_back_to_position():
    """An unlabelled variant among labelled ones keeps its positional label."""
    doc = parse(
        "[[model__grid]]\n"
        '__label = "small"\n'
        "width = 64\n"
        "[[model__grid]]\n"
        "width = 128\n"
    )

    results = list(iter_configs(doc, base_stem="job"))

    assert [label for label, _ in results] == ["job__model+small", "job__model+1"]
    assert all("__label" not in config["model"] for _, config in results)


def test_custom_grid_suffix_and_label_options_are_honored():
    """Callers can select a suffix and compact nested parameter labels."""
    doc = parse("[model]\nwidth__choice = [64, 128]\n")

    results = list(
        iter_configs(
            doc,
            grid_suffixes=("__choice",),
            base_stem="job",
            short_names=True,
            equal_sign="=",
        )
    )

    assert [label for label, _ in results] == ["job__width=64", "job__width=128"]


def test_short_names_omits_key_for_unambiguous_string_values():
    """A single string-valued grid dimension gets a bare value, no key= prefix."""
    doc = parse('word__grid = ["alpha", "beta"]\n')
    results = list(iter_configs(doc, base_stem="exp", short_names=True))
    assert [label for label, _ in results] == ["exp__alpha", "exp__beta"]


def test_short_names_rejects_colliding_leaf_names():
    """Distinct nested parameters that shorten to the same leaf name must not silently collide."""
    doc = parse("[model]\nwidth__grid = [64, 128]\n\n[optim]\nwidth__grid = [1, 2]\n")
    with pytest.raises(ValueError, match="short_names=True"):
        list(iter_configs(doc, base_stem="exp", short_names=True))


def test_grid_expansion_does_not_mutate_the_parsed_job_document():
    """One parsed job document can be expanded repeatedly with identical results."""
    doc = parse('word__grid = ["first", "second"]\n')
    original = tomlkit.dumps(doc)

    first = list(iter_configs(doc, base_stem="job"))
    second = list(iter_configs(doc, base_stem="job"))

    assert first == second
    assert tomlkit.dumps(doc) == original


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("width__grid = 64\n", "width__grid must be a list or array of tables"),
        (
            "[[model__grid]]\nwidth__grid = 64\n",
            "width__grid must be a list",
        ),
    ],
)
def test_grid_values_must_be_lists_or_arrays_of_tables(source, message):
    """Malformed grid declarations identify the invalid config path."""
    with pytest.raises(TypeError, match=message):
        list(iter_configs(parse(source)))


def test_long_grid_labels_are_bounded_and_collision_resistant():
    """Generated labels fit filesystem components without collapsing variants."""
    first = "a" * 300
    second = "a" * 299 + "b"
    doc = tomlkit.document()
    doc["value__grid"] = [first, second]

    labels = [label for label, _ in iter_configs(doc, base_stem="job")]

    assert len(labels) == len(set(labels))
    assert all(len(label.encode("utf-8")) <= 250 for label in labels)
