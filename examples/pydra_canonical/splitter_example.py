"""Run the splitter examples from the Pydra SciPy 2020 paper using Pydra 1.0a11.

Figures 4-7 show paired/product mapping and grouping. Page 137 applies mapping
to a list of Taylor-series indices produced by an upstream task.
"""

import argparse
import json
import math
from pathlib import Path

from pydra.compose import python, workflow
from pydra.engine.submitter import Submitter


@python.define
def add(x: int, y: int) -> int:
    """One concrete call receives two scalar integers."""
    return x + y


@python.define
def powers(n_max: int) -> list[int]:
    """Return the index list as a task output, as in the paper."""
    return list(range(n_max + 1))


@python.define
def term(x: float, n: int) -> float:
    """Compute one sine Taylor-series term."""
    return (-1) ** n * x ** (2 * n + 1) / math.factorial(2 * n + 1)


@python.define
def sum_terms(terms: list[float]) -> float:
    """Perform the actual reduction after the combiner has collected terms."""
    return sum(terms)


@workflow.define(outputs=["indices", "terms", "approximation"])
def sine_series(x: float, n_max: int) -> tuple[list[int], list[float], float]:
    """Split a runtime task output, collect its terms, then sum them."""
    indices = workflow.add(powers(n_max=n_max)).out
    terms = workflow.add(term(x=x).split(n=indices).combine("n")).out
    approximation = workflow.add(sum_terms(terms=terms)).out
    return indices, terms, approximation


def run_demo(cache_dir: Path, worker: str = "cf", n_procs: int = 2) -> dict:
    options = {"n_procs": n_procs} if worker == "cf" else {}
    with Submitter(
        worker=worker, cache_root=cache_dir.expanduser().resolve(), **options
    ) as submitter:
        # [] means Cartesian product: for x in X: for y in Y: add(x, y).
        outer = submitter(
            add().split(["x", "y"], x=[1, 2], y=[10, 100]).combine(["x", "y"]),
            raise_errors=True,
        ).outputs.out
        # () means paired iteration: for x, y in zip(X, Y, strict=True): add(x, y).
        paired = submitter(
            add().split(("x", "y"), x=[1, 2], y=[10, 100]).combine(["x", "y"]),
            raise_errors=True,
        ).outputs.out
        # Group the y dimension for each x; combine groups, it does not sum.
        grouped = submitter(
            add().split(["x", "y"], x=[1, 2], y=[10, 100]).combine("y"),
            raise_errors=True,
        ).outputs.out
        sine = submitter(sine_series(x=math.pi / 2, n_max=2), raise_errors=True).outputs
    return {
        "outer_product": outer,
        "paired": paired,
        "grouped_by_x": grouped,
        "sine": {
            "indices": sine.indices,
            "terms": sine.terms,
            "approximation": sine.approximation,
            "reference": 1.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("build/pydra-splitters/cache")
    )
    parser.add_argument("--worker", choices=("debug", "cf"), default="cf")
    parser.add_argument("--n-procs", type=int, default=2)
    args = parser.parse_args()
    if args.n_procs < 1:
        parser.error("--n-procs must be positive")
    print(json.dumps(run_demo(args.cache_dir, args.worker, args.n_procs), indent=2))


if __name__ == "__main__":
    main()
