#!/bin/bash
# Run the two-pass Sage recalibration example against one job folder.
#
# Usage: ./run.sh jobs/q99536_example          (bundled example)
#        ./run.sh jobs/<your_job_name>          (your own data -- see README.md)
set -euo pipefail

usage() {
    echo "Usage: $0 jobs/<job_name>"
    echo "  e.g.: $0 jobs/q99536_example"
    exit 1
}

[ $# -eq 1 ] || usage
JOB_DIR="$1"
JOB_TOML="$JOB_DIR/job.toml"

if [ ! -f "$JOB_TOML" ]; then
    echo "error: $JOB_TOML not found (expected fasta.fasta, spectra.mzml, job.toml in $JOB_DIR)"
    exit 1
fi

docker compose run -u "$(id -u):$(id -g)" --rm sage_recal \
    "$JOB_TOML" --outdir "$JOB_DIR/outputs"
