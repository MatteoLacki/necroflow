# Why Necroflow?

You write the logic in Python. The framework takes care of the cache, the DAG, and the
genealogy of paths.

## A pipeline is an ordinary Python script

```python
@command("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
def process_text(raw_text: RawText, tool_config: ToolConfig):
    processed_text = output(ProcessedText)
    return processed_text

def my_pipeline(P: Pipeline, config: dict) -> None:
    P.raw = import_text(P, path=config["input"])
    P.processed = process_text(P, P.raw, P.tool_config)
    P.summary = summarize(P, P.processed)
```

- Results are ordinary variables, rules are ordinary functions. Adding a step = adding a call
  plus a small local change in how the variables are used — no rewriting of file names.
- Full Python while the graph is being built: loops, conditionals, functions, pytest tests.
  Subpipelines (`P.subpipeline`) are plain factories, and shared prefixes are computed once.
- It reads like normal procedural code. That matters during review: changes stay local to the
  call sites, so it is easy to check whether an AI agent understood the intent — or to write
  the logic yourself and hand the rule implementations to somebody else. A pipeline is a kind
  of C++ header stating the intent of how things should work.

## The framework generates the paths

- A result lands in `nodes/{rule}/{provenance_hash}/{file}`. The hash (fingerprint v4) covers
  the recipe structure, the config, the shell, and the full parent lineage.
- Identical work converges on one directory — across independent pipelines in the same DAG too.
  You design no file-name taxonomy and no wildcards.
- Next to the result sits `.rip/`: `dependencies.toml` (lineage + SHA-256 of consumed parents),
  `graph.tgf` (ancestor DAG), `job.log`, `state`, `run.toml` (timings, sizes). The genealogy of
  paths travels with the file instead of sitting in a central database.
- Whatever you want to look at you get as copies under `results/<job>/` with readable labels,
  plus `manifest.toml` (visible path, origin node, content hash). Copies use reflink/CoW
  wherever the filesystem allows it.

## We cache by content, not by time

- Rebuilding a parent to identical bytes does **not** invalidate consumers: we compare SHA-256,
  and mtime only invalidates the fast path.
- State lives in text files, no database. A dangling `running` after a crash forces a rerun.
- `necroflow explain job.toml` says what would run and why (per node); `doctor` runs preflight
  checks with stable `NF_*` codes; `gc` cleans the node store; `graph --json` / `outputs --json`
  / `provenance --json` are there for tools and agents.

## Typed results

- A `NodeType` is a file type. You decide on the hierarchy; subtypes and unions (alternatives)
  of types act as format contracts, and `filename = None` gives an abstract, input-only contract.
- The framework checks composition while the pipeline is being built — the error arrives before
  anything expensive starts.

## Running it

- A single `job.toml` describes the run; `__grid` expands parameter grids (tuning, many datasets)
  into separate jobs with deterministic labels.
- A local, parallel executor with resource caps (`threads`, `ram`, custom ones), a scheduler
  protocol (FIFO by default), `--dry-run`, `--keep-going`, `repeat=N`, and `autoclean` for
  intermediate results.
- A `RuleCall` is atomic: all co-outputs of one call are cached and executed together. Exit 0
  with a declared file missing is a failure, not a success.

## How does this relate to Nextflow (and Snakemake)?

A different weight class and, more importantly, a different abstraction boundary:

- One machine. We do not coordinate a cluster — not yet.
- Containers are orthogonal: Necroflow does not use them in any particular way. You can seal the
  whole project into an image, or base individual rules on `docker run` and combine Necroflow's
  convenience with the reproducibility of an externally configured environment.
- Python instead of Groovy or a wildcard DSL: simpler for developers, for data scientists, and
  for AI. Tests and multi-dataset pipelines are written with ordinary means.
- Snakemake is the closest reference point and it is mature. The difference is not that things
  cannot be done there — it is about who maintains the path taxonomy and the wildcard
  constraints as variants pile up.
- Prefect shows that dynamic orchestration in Python works, but generic tasks give you neither
  typed result files nor paths derived from lineage.
- A small code base: ~5.6k lines in `src/`.

Downsides: this is new, so you automatically become an early adopter; there is no HPC and no
cloud; there is no plugin ecosystem.

## Plans for the near future

- A publication — currently being written.
- Resuscitating the Textual-based process manager. Right now rule stdout goes to `.rip/job.log`
  (because several rules run at once); `tail` works, but a window manager is not the same thing.
- Scouting SLURM — there is a supercomputer near where I live, so I will check how it relates to
  what already exists.
