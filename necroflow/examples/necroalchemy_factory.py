"""Pipeline factory for the necroalchemy CLI example.

The necroflow CLI adds this file's directory to sys.path, so necroalchemy
can be imported directly.

Usage (from the necroflow/ project root):

    necroflow \\
        --pipeline examples/necroalchemy_factory.py:factory \\
        --config   examples/necroalchemy_grid.toml \\
        --outdir   /tmp/necroalchemy_cli \\
        --link-outputs

After the run, /tmp/necroalchemy_cli/ contains:
  - The hash-addressed output tree  (rule/hash/file)
  - One symlinked subfolder per grid combo, e.g.:
      necroalchemy_grid__word+necroflow__n+2/
      necroalchemy_grid__word+necroflow__n+5/
      ...
  - A manifest.toml inside each subfolder listing sink output paths.

Multiple --config flags are accepted; each expands independently and all
pipelines share the same DAG (upstream nodes common across configs run once).
"""
from necroalchemy import alchemy_pipeline


def factory(cfg: dict):
    """Build one alchemy pipeline from a plain config dict."""
    return alchemy_pipeline(cfg["word"], n=cfg["n"])
