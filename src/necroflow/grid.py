"""TOML __grid expansion — vendored from snakemakeconfigs.toml_patcher.

Source: github.com/MatteoLacki/snakemakeconfigs
Added at the bottom: iter_configs(), _to_plain_dict().
"""

from __future__ import annotations

import copy
import difflib
import hashlib
import os
import re
from itertools import product
from typing import Any, Iterator

import tomlkit
from tomlkit.items import AoT

# ── patch / grid extraction ───────────────────────────────────────────────────


def apply_patch(base_doc, patch_doc, grid_suffixes):
    grid_params = {}

    def merge(target, updates, path=""):
        for key, value in updates.items():
            for suffix in grid_suffixes:
                if key.endswith(suffix):
                    actual_key = key[: -len(suffix)]
                    actual_path = f"{path}.{actual_key}" if path else actual_key
                    if isinstance(value, AoT):
                        all_variants = []
                        for elem in value:
                            all_variants.extend(
                                _expand_aot_element(elem, grid_suffixes)
                            )
                        grid_params[actual_path] = all_variants
                        target[actual_key] = all_variants[0]
                    elif isinstance(value, list):
                        grid_params[actual_path] = value
                        target[actual_key] = value[0]
                    else:
                        raise TypeError(
                            f"{actual_path}{suffix} must be a list or array of tables"
                        )
                    break
            else:
                if isinstance(value, dict):
                    if key not in target:
                        target[key] = tomlkit.table()
                    next_path = f"{path}.{key}" if path else key
                    merge(target[key], value, next_path)
                else:
                    target[key] = value

    result = tomlkit.parse(tomlkit.dumps(base_doc))
    merge(result, patch_doc)
    return result, grid_params


def _aot_elem_to_plain_table(elem):
    t = tomlkit.table()
    for k, v in elem.items():
        t[k] = _aot_elem_to_plain_table(v) if isinstance(v, dict) else copy.deepcopy(v)
    return t


def _expand_aot_element(elem_table, grid_suffixes):
    local_grids = {}

    def walk_elem(tbl, path=""):
        for key in list(tbl.keys()):
            value = tbl[key]
            for suffix in grid_suffixes:
                if key.endswith(suffix):
                    actual_key = key[: -len(suffix)]
                    actual_path = f"{path}.{actual_key}" if path else actual_key
                    if not isinstance(value, list):
                        raise TypeError(f"{actual_path}{suffix} must be a list")
                    local_grids[actual_path] = value
                    tbl[actual_key] = value[0]
                    del tbl[key]
                    break
            else:
                if isinstance(value, dict):
                    walk_elem(value, f"{path}.{key}" if path else key)

    base = _aot_elem_to_plain_table(elem_table)
    explicit_label = None
    if "__label" in base:
        explicit_label = str(base["__label"])
        del base["__label"]

    walk_elem(base)

    if not local_grids:
        results = [base]
    else:
        names = list(local_grids)
        values = [local_grids[n] for n in names]
        results = []
        for combo in product(*values):
            variant = _aot_elem_to_plain_table(base)
            for k, v in zip(names, combo):
                set_nested_value(variant, k, v)
            results.append(variant)

    if explicit_label is not None:
        for variant in results:
            variant["__label"] = explicit_label
    return results


def extract_grids_from_doc(doc, grid_suffixes):
    grid_params = {}

    def walk(table, path=""):
        for key in list(table.keys()):
            value = table[key]
            for suffix in grid_suffixes:
                if key.endswith(suffix):
                    actual_key = key[: -len(suffix)]
                    actual_path = f"{path}.{actual_key}" if path else actual_key
                    if isinstance(value, AoT):
                        all_variants = []
                        for elem in value:
                            all_variants.extend(
                                _expand_aot_element(elem, grid_suffixes)
                            )
                        grid_params[actual_path] = all_variants
                        table[actual_key] = all_variants[0]
                        del table[key]
                    elif isinstance(value, list):
                        grid_params[actual_path] = value
                        table[actual_key] = value[0]
                        del table[key]
                    else:
                        raise TypeError(
                            f"{actual_path}{suffix} must be a list or array of tables"
                        )
                    break
            else:
                if isinstance(value, dict):
                    next_path = f"{path}.{key}" if path else key
                    walk(value, next_path)

    result = tomlkit.parse(tomlkit.dumps(doc))
    walk(result)
    return result, grid_params


# ── nested helpers ────────────────────────────────────────────────────────────


def set_nested_value(doc, path, value):
    parts = path.split(".")
    current = doc
    for part in parts[:-1]:
        if part not in current:
            current[part] = tomlkit.table()
        current = current[part]
    current[parts[-1]] = value


def get_nested_value(doc, path):
    current = doc
    for part in path.split("."):
        if part not in current:
            return None
        current = current[part]
    return current


# ── filename helpers ──────────────────────────────────────────────────────────


def sanitize_for_filename(s):
    s = str(s)
    for old, new in {
        "[": "",
        "]": "",
        " ": "",
        ",": "-",
        ".": "p",
        "/": "_",
        "\\": "_",
        ":": "_",
        "*": "star",
        "?": "",
        '"': "",
        "<": "",
        ">": "",
        "|": "_",
    }.items():
        s = s.replace(old, new)
    return s


def _strip_common_affixes(labels):
    prefix = os.path.commonprefix(labels)
    suffix = os.path.commonprefix([s[::-1] for s in labels])[::-1]
    p, s = len(prefix), len(suffix)
    if p + s == 0:
        return labels
    if any(p + s >= len(lab) for lab in labels):
        s = 0
        if any(p >= len(lab) for lab in labels):
            return labels
    return [lab[p : len(lab) - s if s else None].strip("_") or lab for lab in labels]


def _find_auto_label(vals):
    common_keys = set(vals[0].keys())
    for v in vals[1:]:
        common_keys &= set(v.keys())
    for key in sorted(common_keys):
        values = [v[key] for v in vals]
        if all(isinstance(val, str) for val in values) and len(set(values)) == len(
            vals
        ):
            return {id(v): sanitize_for_filename(v[key]) for v in vals}
    return None


def diff_strings(str_a, str_b):
    tokens_a = re.findall(r"\w+", str(str_a))
    tokens_b = re.findall(r"\w+", str(str_b))
    matcher = difflib.SequenceMatcher(None, tokens_a, tokens_b)
    new_tokens = []
    for tag, _, _, j1, j2 in matcher.get_opcodes():
        if tag in ("insert", "replace"):
            new_tokens.extend(tokens_b[j1:j2])
    return "_".join(new_tokens)


def value_to_string(value, base_value=None):
    if (
        base_value is not None
        and isinstance(value, str)
        and isinstance(base_value, str)
    ):
        diff = diff_strings(base_value, value)
        if diff:
            return sanitize_for_filename(diff)
    if isinstance(value, float):
        return str(value).replace(".", "p").replace("-", "neg")
    if isinstance(value, bool):
        return "true" if value else "false"
    return sanitize_for_filename(str(value))


def shorten_param_name(name):
    return name.split(".")[-1]


def truncate_to_bytes(s, max_bytes):
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def make_config_name(
    params,
    base_stem,
    base_values,
    short_names=False,
    equal_sign="=",
    grid_indices=None,
    value_only_keys=None,
):
    parts = []
    for key, value in params.items():
        name = shorten_param_name(key) if short_names else key.replace(".", "_")
        if grid_indices and key in grid_indices:
            val_str = str(grid_indices[key][id(value)])
        else:
            base_value = base_values.get(key)
            val_str = value_to_string(value, base_value)
        if value_only_keys and key in value_only_keys:
            parts.append(val_str)
        else:
            parts.append(f"{name}{equal_sign}{val_str}")

    param_str = "__".join(parts)
    base_name = f"{base_stem}__{param_str}"

    if len(base_name.encode("utf-8")) > 250:
        h = hashlib.md5(param_str.encode()).hexdigest()[:8]
        truncated = truncate_to_bytes(base_name, 241)
        base_name = f"{truncated}_{h}"

    return f"{base_name}.toml"


# ── grid index helpers ────────────────────────────────────────────────────────


def _build_grid_indices(grid_params):
    result = {}
    for name, vals in grid_params.items():
        if not (vals and isinstance(vals[0], dict)):
            continue
        if any("__label" in v for v in vals):
            result[name] = {
                id(v): sanitize_for_filename(str(v["__label"]))
                for v in vals
                if "__label" in v
            }
            for i, v in enumerate(vals):
                if id(v) not in result[name]:
                    result[name][id(v)] = str(i)
        else:
            auto = _find_auto_label(vals)
            result[name] = (
                auto
                if auto is not None
                else {id(v): str(i) for i, v in enumerate(vals)}
            )
    return result


def _compute_value_only_keys(
    grid_params, grid_indices, base_scalar_values, short_names
):
    if not short_names:
        return set()
    candidate_label_sets = {}
    for name, vals in grid_params.items():
        if name in grid_indices:
            labels = set(grid_indices[name].values())
            if not all(s.isdigit() for s in labels):
                candidate_label_sets[name] = labels
        elif vals and all(isinstance(v, str) for v in vals):
            base_val = base_scalar_values.get(name)
            candidate_label_sets[name] = {value_to_string(v, base_val) for v in vals}
    value_only = set()
    for name, label_set in candidate_label_sets.items():
        other_values = set()
        for other_name, other_set in candidate_label_sets.items():
            if other_name != name:
                other_values |= other_set
        if not (label_set & other_values):
            value_only.add(name)
    return value_only


# ── necroflow addition ────────────────────────────────────────────────────────


def iter_configs(
    doc,
    grid_suffixes: tuple[str, ...] = ("__grid",),
    base_stem: str = "config",
    short_names: bool = False,
    equal_sign: str = "+",
) -> Iterator[tuple[str, dict]]:
    """Yield (label, config_dict) pairs from a TOML doc with __grid dimensions.

    label matches the filename snakemakeconfigs would produce, without the .toml
    extension — e.g. 'experiment__layers+128__lr+0p01'.
    config_dict is a plain Python dict (tomlkit types stripped).

    If the doc has no __grid keys, yields a single (base_stem, full_config) pair.
    """
    result_doc, grid_params = extract_grids_from_doc(doc, tuple(grid_suffixes))

    if not grid_params:
        yield base_stem, _to_plain_dict(result_doc)
        return

    base_scalar_values = {
        name: get_nested_value(result_doc, name) for name in grid_params
    }
    grid_indices = _build_grid_indices(grid_params)
    value_only_keys = _compute_value_only_keys(
        grid_params, grid_indices, base_scalar_values, short_names
    )

    for vals in grid_params.values():
        for v in vals:
            if isinstance(v, dict) and "__label" in v:
                del v["__label"]

    param_names = list(grid_params)
    param_values = [grid_params[n] for n in param_names]

    for combo in product(*param_values):
        variant = tomlkit.parse(tomlkit.dumps(result_doc))
        params = dict(zip(param_names, combo))
        for k, v in params.items():
            set_nested_value(variant, k, v)
        filename = make_config_name(
            params,
            base_stem,
            base_scalar_values,
            grid_indices=grid_indices,
            value_only_keys=value_only_keys,
            short_names=short_names,
            equal_sign=equal_sign,
        )
        yield filename[:-5], _to_plain_dict(variant)  # strip .toml


def _to_plain_dict(doc: Any) -> Any:
    """Recursively convert a tomlkit document to plain Python types."""
    if isinstance(doc, dict):
        return {k: _to_plain_dict(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_to_plain_dict(v) for v in doc]
    return doc
