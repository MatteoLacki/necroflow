---
name: compact-cols
description: Compact Python class declarations into aligned tables while preserving inheritance, docstrings, and Black formatting guards. Use when asked to compact or align repetitive node classes.
---

# Compact class columns

Apply this convention when formatting repetitive Python class declarations.
The rules come from the module docstring of ionmaidentools' `pipelines.py`
(lines 10–20).

## Rules

- Keep prerequisite base classes first, all compact classes in one tabular section,
  and the remaining documented classes in a separate section. Define bases before
  subclasses within each section.
- Write attribute-only and pass-only classes on one line. Group matching fields
  together; align each field's start column across its group. Separate assignments
  with semicolons and keep adjacent table rows contiguous.
- Preserve class docstrings and use multiline declarations for documented classes.
  Do not leave blank lines between a docstring and the following code in its body.
- Protect declaration sections with `# fmt: off` / `# fmt: on` so Black preserves
  alignment and docstring spacing.

Keep class names, bases, attributes, values, and docstrings unchanged. Move constants
before classes that use them; move registries after the classes they reference.
Do not reorder definitions across dependencies or import-time side effects.

## Example

Excerpt from `pipelines.py`, with prerequisite imports included and Node bases
qualified as `nf.Node`. The three compact rows retain the source table's columns.

```python
import necroflow as nf

TIMSTOFU = "git/timstofu/src/timstofu"

# fmt: off
# --- base node types ---
class Toml(nf.Node):
    """A TOML file."""


class PythonModule(nf.Node):
    """The module a Python tool's launcher imports, at `source`: an input of the steps
    running the tool, so that editing it reruns them. Edits deeper in its package are not
    tracked. One subclass per tool, so a step accepts only its own tool's module."""
    source: str


# --- tabular node types ---
class FindArgmaxesModule(PythonModule):                  filename = "find_argmaxes.py";                   source = f"{TIMSTOFU}/scale_estimation/find_argmaxes.py"
class ExtractSampleTensorsModule(PythonModule):          filename = "extract_sample_tensors.py";          source = f"{TIMSTOFU}/scale_estimation/extract_sample_tensors.py"
class FitScaleEstimatesModule(PythonModule):             filename = "fit_scale_estimates.py";             source = f"{TIMSTOFU}/scale_estimation/fit_scale_estimates.py"


# --- documented node types ---
class PipelineConfig(Toml):
    """The job's complete effective config, as TOML."""
    filename = "pipeline_config.toml"
# fmt: on
```

## Verification

Compare each class's AST before and after formatting, ignoring source locations.
When regrouping, check that bases and referenced constants are defined first.
Compile the result and confirm Black preserves each guarded block. Keep blank
lines separating definitions; omit blanks between a docstring and subsequent
code inside the same body.
