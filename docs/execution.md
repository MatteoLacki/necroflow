# Execution, Scheduling, and Cleanup

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Release Checklist](release.md)

## Parallelism and scheduling

`execute()` runs nodes in parallel subject to resource caps. By default the thread cap is all available CPUs. Declare per-job requirements as `@command` keyword arguments; set global caps via `resource_caps` (Python API) or CLI flags.

```python
@command("bwa mem {ref} {fastq} > {bam}", threads=4, ram="8Gi")
def align(fastq: Fastq, ref: str):
    """Align reads with BWA-MEM."""
    bam = output(Bam)
    return bam
dag.execute(resource_caps={"threads": 16, "ram": parse_resource("64Gi")})
```

Resource values accept SI (`K M G T P` = powers of 1000) and binary (`Ki Mi Gi Ti Pi` = powers of 1024) suffixes — e.g. `"8Gi"` is 8 GiB, `"8G"` is 8 GB. A job whose requirement exceeds the cap still runs solo when nothing else is running.

Rule constraints are also available to command templates. Direct placeholders such as `{threads}` and `{ram}` resolve to the declared constraint value; `{threads}` defaults to `1` when omitted. Use `{constraint:name}` to force lookup from constraints when a config input has the same name:

```python
@command("tool --threads {threads} --memory {ram} --gpu {constraint:gpu} -i {inp} -o {out}",
           threads=8, ram="32Gi", gpu=1)
def run_tool(inp: Input):
    out = output(Output)
    return out
```

`@command(..., repeat=N)` accepts Snakemake-style repeat compatibility metadata. Necroflow stores it as `rule.repeat` and validates that it is a positive integer, but it is currently metadata only: it does not make the executor run the command multiple times and it is not part of scheduling resources or output fingerprints.

## Shell selection and brace expansion

String commands use Python's default `shell=True` behavior unless a shell path is provided. Choose a shell explicitly when the command depends on shell-specific behavior such as Bash brace expansion. Literal shell braces must be doubled in rule templates so Python formatting leaves them intact:

```python
@command("printf '%s\n' {{left,right}} > {out}")
def make_out():
    out = output(Out)
    return out
execute(P, "nodes", shellpath="/bin/bash")
```

The equivalent CLI flag is:

```bash
necroflow --shellpath /bin/bash job.toml
```

An explicit `shellpath` is included in fingerprints for string commands and recorded in provenance. List-form commands and built-in materializers do not use a shell and are not affected.

By default the scheduler prioritises nodes from the **smallest connected component** of remaining work — this tends to finish whole samples before starting new ones, keeping memory pressure low.
The CLI accepts `--scheduler connected-components` (default), `--scheduler fifo`, or a local Python callable such as `--scheduler schedulers.py:my_scheduler`.

```python
from necroflow import fifo_scheduler

dag.execute(resource_caps={"threads": 16}, scheduler=fifo_scheduler)  # topological order instead
```

Custom schedulers:

```python
def my_scheduler(ready, remaining, available_resources):
    # Contains remaining capped capacity, e.g. {"threads": 12}.
    return sorted(ready, key=lambda n: n.rule.constraints.get("threads", 1), reverse=True)

dag.execute(scheduler=my_scheduler)
```

## Failure handling

```python
dag.execute(keep_going=True)   # continue independent branches past failures
```

With `keep_going=False` (default) the first failure raises immediately. With `keep_going=True` independent branches keep running and all failures are collected into an `ExceptionGroup` at the end.

After each successful job, necroflow verifies that the declared output file exists. A command that exits 0 but writes no output is treated as a failure.

Run state is persisted to a plain-text `state` file inside each node's `.rip/` directory between invocations. A node whose output exists on disk but whose previous run was interrupted by a signal or left in an unknown state is automatically re-executed next time.

Each job's stdout/stderr is captured to the node store at `{rule}/{hash}/.rip/job.log`. On failure the log is printed to the terminal.


## Execution reports

Each successful rule call writes node-local runtime metadata to the rule-call
metadata directory:

```text
nodes/<rule>/<hash>/.rip/run.toml
```

The file records the last successful execution of that cached node directory:
start/end timestamps, wall-clock duration, exit code, and total output size. The
size is measured for the rule-call output directory, excluding `.rip` metadata,
so side files written under `{workdir}` are counted with declared outputs.

CLI runs also write a per-job execution summary next to the manifest:

```text
results/<job-label>/execution.toml
```

That summary lists every requested node and ancestor for that job. Executed nodes
include `duration_seconds`, `started_at`, `finished_at`, `exit_code`, and
`output_size_bytes`; cached nodes are marked `cached = true` and include the
current measured output size when the cached output directory exists. With
`--keep-going`, failed attempted nodes are included with `state = "failed"` or
`state = "interrupted"` and the captured error/exit code.

`--autoclean` may delete intermediate node directories and their `.rip/run.toml`
files after downstream work completes. The per-job `execution.toml` is written
under `results/`, so it remains available as the durable summary for that CLI
invocation even when intermediate cached folders were removed.

## Cleaning orphan outputs

Outputs that existed from a previous run but are no longer in the required subgraph are classified as `ORPHAN`. Pass `autoclean=True` to delete them. Intermediate rule-call directories are removed as whole directories once all downstream work is complete, so side files written under `{workdir}` are cleaned together with the declared outputs:

```python
dag.execute(autoclean=True)
```

Or via CLI:

```bash
necroflow --nodes-dir nodes --results-dir results --autoclean job.toml
```

[Previous: Generated Config Files](generated-config-files.md) | [README](../README.md) | [Next: Release Checklist](release.md)
