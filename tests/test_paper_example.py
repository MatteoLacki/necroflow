"""Checks for the canonical paper example without external bioinformatics tools."""

import importlib.util
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "simple_dag.py"


def _load_example():
    spec = importlib.util.spec_from_file_location("paper_simple_dag", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_paper_example_factories_have_consistent_contracts():
    example = _load_example()
    config = example.example_config()

    extended = example.extended_pipeline(config)
    assert extended.counts.node_type is example.Counts
    assert extended.vcf.node_type is example.Vcf
    assert extended.annotated_vcf.node_type is example.AnnotatedVcf
    assert extended.align_log.node_type is example.Log

    dag, quant, variants = example.shared_dag()
    assert quant.sorted_bam.key == variants.sorted_bam.key
    assert quant.counts.node_type is example.Counts
    assert variants.vcf.node_type is example.Vcf
    assert len(dag.nodes) < len(quant.nodes) + len(variants.nodes)
