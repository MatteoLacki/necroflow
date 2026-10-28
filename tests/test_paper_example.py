"""Checks for the canonical paper example without external bioinformatics tools."""

import importlib.util
from pathlib import Path

from necroflow import DAG, Pipeline

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "simple_dag.py"


def _load_example():
    spec = importlib.util.spec_from_file_location("paper_simple_dag", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_paper_example_workflows_have_consistent_contracts():
    example = _load_example()
    config = example.example_config()

    extended = Pipeline(DAG("/tmp/necroflow-paper-example"))
    example.extended_pipeline(extended, config)
    assert type(extended.counts) is example.Counts
    assert type(extended.vcf) is example.Vcf
    assert type(extended.annotated_vcf) is example.AnnotatedVcf
    assert type(extended.align_log) is example.Log

    dag, quant, variants = example.shared_dag()
    assert quant.sorted_bam.relative_path == variants.sorted_bam.relative_path
    assert type(quant.counts) is example.Counts
    assert type(variants.vcf) is example.Vcf
    assert len(dag.nodes) < len(quant.nodes) + len(variants.nodes)
