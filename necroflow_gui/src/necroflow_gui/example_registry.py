from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from necroflow import Constraints, DAG, Inputs, Outputs, Pipeline, Rules, node_types

from necroflow_gui.registry import PipelineConfig, PipelineSpec


Fastq, Bam, Log, Counts, QcReport, Vcf, AnnotatedVcf, MergedVcf = node_types(
    "fastq=reads.fastq.gz"
    " bam=aligned.bam"
    " log=align.log"
    " counts=counts.txt"
    " qc_report=qc.txt"
    " vcf=variants.vcf.gz"
    " annotated_vcf=annotated.vcf.gz"
    " merged_vcf=merged.vcf.gz"
)


class SortedBam(Bam):
    name = "sorted.bam"


R = Rules()

R.register("raw_fastq", Inputs(path=str), Outputs(fastq=Fastq), "touch {fastq}")

R.register(
    "align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam, log=Log),
    "touch {bam} {log}",
    Constraints(threads=4),
)

R.register(
    "sort_bam",
    Inputs(bam=Bam),
    Outputs(sorted_bam=SortedBam),
    "touch {sorted_bam}",
)

R.register(
    "quantify",
    Inputs(bam=SortedBam, gene_model=str),
    Outputs(counts=Counts, qcreport=QcReport),
    "touch {counts} {qcreport}",
)

R.register(
    "call_variants",
    Inputs(bam=SortedBam, caller=str),
    Outputs(vcf=Vcf),
    "touch {vcf}",
)

R.register(
    "annotate",
    Inputs(vcf=Vcf, db=str),
    Outputs(annotated_vcf=AnnotatedVcf),
    "touch {annotated_vcf}",
)

R.register(
    "merge_annotations",
    Inputs(snp_ann=AnnotatedVcf, indel_ann=AnnotatedVcf),
    Outputs(merged_vcf=MergedVcf),
    "touch {merged_vcf}",
)


def basic_pipeline(config, rules):
    pipeline = Pipeline()
    pipeline.fastq = rules.raw_fastq(path=config.path)
    pipeline.bam, pipeline.align_log = rules.align(pipeline.fastq, ref=config.ref)
    pipeline.sorted_bam = rules.sort_bam(pipeline.bam)
    pipeline.counts, pipeline.qc = rules.quantify(
        pipeline.sorted_bam, gene_model=config.gene_model
    )
    return pipeline


def diamond_pipeline(config, rules):
    pipeline = Pipeline()
    pipeline.fastq = rules.raw_fastq(path=config.path)
    pipeline.bam, pipeline.align_log = rules.align(pipeline.fastq, ref=config.ref)
    pipeline.sorted_bam = rules.sort_bam(pipeline.bam)
    pipeline.snp_vcf = rules.call_variants(
        pipeline.sorted_bam, caller="haplotypecaller"
    )
    pipeline.indel_vcf = rules.call_variants(pipeline.sorted_bam, caller="mutect2")
    pipeline.snp_ann = rules.annotate(pipeline.snp_vcf, db="dbsnp")
    pipeline.indel_ann = rules.annotate(pipeline.indel_vcf, db="clinvar")
    pipeline.merged = rules.merge_annotations(pipeline.snp_ann, pipeline.indel_ann)
    return pipeline


CONFIGS = (
    PipelineConfig(
        id="sample1",
        label="Sample 1",
        values=SimpleNamespace(
            path="/tmp/necroflow_gui/sample1.fastq.gz",
            ref="hg38",
            gene_model="gencode_v44",
        ),
    ),
    PipelineConfig(
        id="sample2",
        label="Sample 2",
        values=SimpleNamespace(
            path="/tmp/necroflow_gui/sample2.fastq.gz",
            ref="hg38",
            gene_model="gencode_v44",
        ),
    ),
)


def _load_necroalchemy_module():
    path = Path(__file__).resolve().parents[3] / "necroflow" / "examples" / "necroalchemy.py"
    spec = importlib.util.spec_from_file_location("necroflow_examples_necroalchemy", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load necroalchemy example from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


NECROALCHEMY = _load_necroalchemy_module()


def necroalchemy_pipeline(config, rules):
    return NECROALCHEMY.alchemy_pipeline(config.word, n=config.n)


NECROALCHEMY_CONFIGS = (
    PipelineConfig(
        id="hello",
        label="hello x2",
        values=SimpleNamespace(word="hello", n=2),
    ),
    PipelineConfig(
        id="necroflow",
        label="necroflow x3",
        values=SimpleNamespace(word="necroflow", n=3),
    ),
    PipelineConfig(
        id="python",
        label="python x3",
        values=SimpleNamespace(word="python", n=3),
    ),
)


PIPELINES = (
    PipelineSpec(
        id="basic",
        label="Basic RNA pipeline",
        rules=R,
        build=basic_pipeline,
        configs=CONFIGS,
        outdir=Path("/tmp/necroflow_gui/results/basic"),
    ),
    PipelineSpec(
        id="diamond",
        label="Diamond variant pipeline",
        rules=R,
        build=diamond_pipeline,
        configs=CONFIGS,
        outdir=Path("/tmp/necroflow_gui/results/diamond"),
    ),
    PipelineSpec(
        id="necroalchemy",
        label="Necroalchemy text pipeline",
        rules=NECROALCHEMY.R,
        build=necroalchemy_pipeline,
        configs=NECROALCHEMY_CONFIGS,
        outdir=Path("/tmp/necroflow_gui/results/necroalchemy"),
    ),
)
