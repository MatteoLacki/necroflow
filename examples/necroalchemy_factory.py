"""Pipeline factory for the necroalchemy CLI example.

The necroflow CLI adds this file's directory to sys.path, so necroalchemy
can be imported directly.

Usage (from the necroflow/ project root):

    necroflow \
        --nodes-dir   /tmp/necroalchemy_nodes \
        --results-dir /tmp/necroalchemy_results \
        examples/necroalchemy_grid.toml

After the run, /tmp/necroalchemy_nodes/ contains the hash-addressed node output
tree (rule/hash/file). /tmp/necroalchemy_results/ contains one symlinked
subfolder per grid combo, e.g.:
    necroalchemy_grid__word+necroflow__n+2/
    necroalchemy_grid__word+necroflow__n+5/
    ...
Each result subfolder has a manifest.toml listing sink output paths, keyed by
Pipeline attribute name (e.g. summary, audit).

Multiple job TOML files are accepted; each expands independently and all
pipelines share the same DAG (upstream nodes common across configs run once).
"""

from necroalchemy import alchemy_pipeline


def factory(P, cfg: dict) -> None:
    """Build one alchemy pipeline from a plain config dict."""
    alchemy_pipeline(P, cfg["word"], n=cfg["n"])
