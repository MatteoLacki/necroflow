"""Compare scientific outputs, ignoring paths, timestamps and HTML packaging."""

import csv
import json
import math
from pathlib import Path
import zipfile

from containers import ROOT
from prepare import SAMPLES


def compare_quant(first, second):
    def read(path):
        with path.open() as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        result = {row["Name"]: row for row in rows}
        if not result or len(result) != len(rows):
            raise AssertionError(f"Empty or duplicate transcript rows: {path}")
        return result

    left, right = read(first), read(second)
    assert left.keys() == right.keys(), "Transcript identities differ"
    for name, row in left.items():
        assert (
            row["Length"] == right[name]["Length"]
        ), f"Transcript length differs: {name}"
        for field in ("EffectiveLength", "TPM", "NumReads"):
            assert math.isclose(
                float(row[field]), float(right[name][field]), rel_tol=1e-4, abs_tol=1e-6
            ), f"Salmon {name}/{field}: {row[field]} != {right[name][field]}"
    return len(left)


def fastqc_data(folder):
    result = {}
    for path in sorted(folder.rglob("*_fastqc.zip")):
        with zipfile.ZipFile(path) as archive:
            name = next(n for n in archive.namelist() if n.endswith("/fastqc_data.txt"))
            if path.name in result:
                raise AssertionError(f"Duplicate FastQC report: {path.name}")
            result[path.name] = archive.read(name).decode()
    assert result, f"No FastQC archives: {folder}"
    return result


def coverage(path):
    data = json.loads(path.read_text())["report_saved_raw_data"]
    result = {}
    for tool in ("fastqc", "salmon"):
        matching = [
            value for key, value in data.items() if key in (tool, f"multiqc_{tool}")
        ]
        assert len(matching) == 1, f"Missing or ambiguous {tool} module in {path}"
        result[tool] = sorted(matching[0])
    return result


def main():
    necro = ROOT / "results" / "necroflow"
    baseline = ROOT / "results" / "nextflow"
    summary = {"samples": {}}
    for sample in SAMPLES:
        candidates = list((baseline / "quant" / sample).rglob("quant.sf"))
        assert (
            len(candidates) == 1
        ), f"Expected one Nextflow quantification for {sample}"
        count = compare_quant(necro / "quant" / sample / "quant.sf", candidates[0])
        assert fastqc_data(necro / "fastqc" / sample) == fastqc_data(
            baseline / "fastqc" / sample
        ), f"FastQC metrics differ: {sample}"
        summary["samples"][sample] = {"transcripts": count, "fastqc": "identical"}
    with (ROOT / "reports" / "nextflow-trace.tsv").open() as stream:
        tasks = list(csv.DictReader(stream, delimiter="\t"))
    multiqc_tasks = [
        task for task in tasks if task["name"].split(":")[-1].startswith("MULTIQC")
    ]
    assert len(multiqc_tasks) == 1
    baseline_json = (
        Path(multiqc_tasks[0]["workdir"]) / "multiqc_report_data" / "multiqc_data.json"
    )
    observed = coverage(necro / "multiqc" / "multiqc_report_data" / "multiqc_data.json")
    assert observed == coverage(baseline_json), "MultiQC module/sample coverage differs"
    assert len(observed["fastqc"]) == 4 and len(observed["salmon"]) == 2
    summary["multiqc"] = observed
    summary["status"] = "passed"
    (ROOT / "reports" / "comparison.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
