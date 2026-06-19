import time
import pytest
from pathlib import Path

from necroflow import (
    Rules, Inputs, Outputs, Pipeline, DAG, node_types, NodeState, classify_nodes,
)
from necroflow.dag import _folder_hash, _node_key


Fastq, Bam, Log = node_types("fastq bam log")

R = Rules()
R.register("raw_fastq", Inputs(path=str), Outputs(fastq=Fastq), "touch {fastq}")
R.register(
    "align",
    Inputs(fastq=Fastq, ref=str),
    Outputs(bam=Bam, log=Log),
    "touch {bam} && touch {log}",
)
R.register("sort_bam", Inputs(bam=Bam), Outputs(bam=Bam), "touch {bam}")


def make_pipeline(path="/data/s.fastq", ref="hg38"):
    P = Pipeline()
    P.fastq = R.raw_fastq(path=path)
    P.bam, P.log = R.align(P.fastq, ref=ref)
    P.sorted = R.sort_bam(P.bam)
    return P


# --- _node_key / _folder_hash ---

def test_cooutputs_distinct_node_keys():
    P = make_pipeline()
    assert _node_key(P.bam) != _node_key(P.log)


def test_cooutputs_share_folder_hash():
    P = make_pipeline()
    assert _folder_hash(P.bam) == _folder_hash(P.log)


def test_command_change_changes_folder_hash():
    R2 = Rules()
    R2.register("raw_fastq", Inputs(path=str), Outputs(fastq=Fastq), "touch {fastq}")
    R2.register("align", Inputs(fastq=Fastq, ref=str), Outputs(bam=Bam),
                "bwa mem {ref} {fastq} > {bam}")  # different command
    R2.register("sort_bam", Inputs(bam=Bam), Outputs(bam=Bam), "touch {bam}")

    P1 = make_pipeline()  # uses original R with "touch {bam}"
    P2 = Pipeline()
    P2.fastq = R2.raw_fastq(path="/data/s.fastq")
    P2.bam = R2.align(P2.fastq, ref="hg38")
    P2.sorted = R2.sort_bam(P2.bam)

    assert _folder_hash(P1.bam) != _folder_hash(P2.bam)


def test_dag_contains_all_cooutputs(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P)
    names = [(n.rule.__name__, n.output_name) for n in dag.nodes]
    assert ("align", "bam") in names
    assert ("align", "log") in names


# --- classify_nodes states ---

def test_missing(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.resolve_paths(tmp_path)
    classify_nodes(dag.nodes, dag.required_nodes)
    states = {n.output_name: n.state for n in dag.nodes}
    assert states["fastq"] == NodeState.MISSING
    # both align[bam] and sort_bam[bam] have output_name="bam"; check by rule name instead
    missing_rules = {n.rule.__name__ for n in dag.nodes if n.state == NodeState.MISSING}
    assert missing_rules == {"raw_fastq", "align", "sort_bam"}
    assert states["log"] is None  # outside required subgraph, no output yet


def test_up_to_date_after_run(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.execute()
    classify_nodes(dag.nodes, dag.required_nodes)
    for n in dag.required_nodes:
        assert n.state == NodeState.UP_TO_DATE


def test_stale_direct(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.execute()
    dag.resolve_paths(tmp_path)

    raw_node = next(n for n in dag.nodes if n.rule.__name__ == "raw_fastq")
    time.sleep(0.05)
    raw_node.path.touch()

    classify_nodes(dag.nodes, dag.required_nodes)
    align_bam = next(n for n in dag.nodes if n.rule.__name__ == "align" and n.output_name == "bam")
    assert align_bam.state == NodeState.STALE


def test_stale_transitive(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.execute()
    dag.resolve_paths(tmp_path)

    raw_node = next(n for n in dag.nodes if n.rule.__name__ == "raw_fastq")
    time.sleep(0.05)
    raw_node.path.touch()

    classify_nodes(dag.nodes, dag.required_nodes)
    sort_node = next(n for n in dag.nodes if n.rule.__name__ == "sort_bam")
    assert sort_node.state == NodeState.STALE


def test_orphan(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.execute()

    # rebuild dag requesting only raw_fastq — align/sort outputs become orphans
    P2 = make_pipeline()
    dag2 = DAG(outdir=tmp_path)
    dag2.add(P2, request=[P2.fastq])
    dag2.resolve_paths(tmp_path)
    classify_nodes(dag2.nodes, dag2.required_nodes)

    orphans = [n for n in dag2.nodes if n.state == NodeState.ORPHAN]
    orphan_rules = {n.rule.__name__ for n in orphans}
    assert "align" in orphan_rules
    assert "sort_bam" in orphan_rules


def test_reruns_stale_nodes(tmp_path):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.execute()
    dag.resolve_paths(tmp_path)

    raw_node = next(n for n in dag.nodes if n.rule.__name__ == "raw_fastq")
    time.sleep(0.05)
    raw_node.path.touch()

    dag.execute()

    classify_nodes(dag.nodes, dag.required_nodes)
    for n in dag.required_nodes:
        assert n.state == NodeState.UP_TO_DATE


def test_skips_up_to_date_on_rerun(tmp_path, capsys):
    P = make_pipeline()
    dag = DAG(outdir=tmp_path)
    dag.add(P, request=[P.sorted])
    dag.execute()

    # second run: record mtimes, re-execute, confirm paths unchanged
    dag.resolve_paths(tmp_path)
    mtimes_before = {n.output_name: n.path.stat().st_mtime for n in dag.required_nodes}
    time.sleep(0.05)
    dag.execute()
    mtimes_after = {n.output_name: n.path.stat().st_mtime for n in dag.required_nodes}
    assert mtimes_before == mtimes_after
