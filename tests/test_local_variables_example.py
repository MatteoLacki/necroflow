"""Regression coverage for the local-variable pipeline example."""

import importlib.util
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "local_variables.py"


def _load_example():
    spec = importlib.util.spec_from_file_location("local_variables_example", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_local_variables_can_be_rebound_while_only_the_result_is_labelled(tmp_path):
    """Required labelled outputs execute every unlabelled local ancestor.

    A local Python name may be rebound after each rule call without consuming
    Pipeline labels. The final label remains enough to request and execute the
    complete chain.
    """
    example = _load_example()
    dag, pipeline = example.build(tmp_path)

    assert pipeline.labels == ("result",)
    assert pipeline.nodes == [pipeline.result]
    assert len(dag.nodes) == 3

    dag.run()

    assert pipeline.result.path.read_text() == "RESULT: HELLO\n"
