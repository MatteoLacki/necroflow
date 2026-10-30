# Necroflow and Pydra

Reviewed 2026-10-19. Necroflow evidence is this working tree, based on commit
`910967d`, including its local changes. The overview compares Necroflow, Snakemake, Nextflow, and **Pydra 1.0a11**.
A focused table then compares Necroflow with the function-defined Pydra API used
by the runnable example.
A separate historical matrix covers **Pydra 0.25**, the latest non-prerelease on
PyPI when checked. The two APIs are distinguished below; the 0.25 matrix is not
a blanket claim about 1.0.
See the upstream [0.25 release](https://pypi.org/project/pydra/0.25/) and
[1.0a11 release](https://pypi.org/project/pydra/1.0a11/).

Both are Python scientific workflow engines with typed dependencies, persistent
hashed caches, reusable workflows, and parallel execution. Pydra is a much closer
comparison than an engine that only checks whether an output path exists.
Necroflow emphasizes typed file products, eager lineage construction, local
resource scheduling, and inspectable metadata. Pydra emphasizes typed Python
tasks, nested dataflows, split/combine semantics, and distributed workers.

## Overview: Necroflow, Snakemake, Nextflow, Pydra

Adapted from the 20-axis table in [COMPARISON.md](COMPARISON.md), retaining its
Snakemake/Nextflow cells and replacing Luigi, CWL/WDL, and Prefect/Airflow with
**Pydra 1.0a11**. Necroflow's authoring, cache, staleness, and environment cells
are updated to the current code, including Docker and the rebuild policy.

| Axis | Necroflow | Snakemake | Nextflow | Pydra 1.0a11 |
| --- | --- | --- | --- | --- |
| **Primary niche** | Small, Python-native, typed, lineage-addressed pipelines that run locally and remain inspectable as ordinary Python objects. | Mature, file-pattern-based bioinformatics workflows with strong ecosystem support and scalable executors. | Production-scale dataflow workflows with process/channel composition, JVM runtime, and strong HPC/cloud portability. | Typed Python/file dataflows with nested workflows, split/combine mapping, persistent caches, and local or HPC workers. |
| **Authoring model** | Plain Python rules create/intern typed Nodes in a shared DAG; @workflow supplies Pipeline context. P.name = node labels a product. Calls, local variables, and branches use ordinary Python. | Snakefile DSL: rules declare `input`, `output`, `params`, `shell`; dependencies are inferred by matching filenames and wildcards. | Groovy-like Nextflow DSL on the JVM; processes communicate through channels and workflows compose processes. | Python functions decorated with @python.define, @shell.define, and @workflow.define. workflow.add(task) returns output fields; local variables wire tasks procedurally. |
| **Configuration surface** | One job TOML holds the workflow, requested outputs, and all user parameters. Output paths and provenance are derived from lineage rather than extra config files. | Usually a config file plus Snakefile targets, wildcard conventions, and path templates. Large projects may also use profiles, modules, sample sheets, and rule-specific parameter files. | Configuration is powerful but often split across `nextflow.config`, profiles, params files, launch flags, and channel/sample metadata. | Typed workflow constructor arguments plus Submitter worker/cache settings. A job TOML, user request selector, and publication layout like this example's CLI are application code. |
| **Rule and value model** | `Rules` are reusable typed transformations; calling a rule creates concrete `Node` values for one pipeline context. The same rule can be reused across many workflows without subclassing or copying path logic. | Rules are reusable within a Snakefile or module system, but rule reuse is coupled to file-pattern conventions, wildcards, and target naming. | Processes are reusable workflow components, but reuse requires compatible channel shapes and tuple/value contracts. | Reusable typed task definitions exchange Python values and typed files. A workflow is itself a reusable task; file products are not required for every intermediate. |
| **Simple branch** | `if config.call: P.result = r.call(P.bam)` is normal Python. The branch changes the downstream rule fingerprint while shared upstream nodes are reused. | Branches are usually encoded by target expansion, conditional input functions, config flags, or separate rules. Powerful, but control flow is split between DSL sections and wildcard resolution. | Branches are expressed through channel operators and workflow conditionals. Scales well for fan-out patterns but requires reasoning about channel shape and process invocation order. | Ordinary Python if/loops on constructor inputs determine which tasks are added. Lazy outputs cannot immediately drive Python if; nested workflows can defer construction until upstream values are available. |
| **Adding a step** | Add a rule, then assign `P.sorted = r.sort(P.bam)`. The new output path is derived automatically from the rule, command, config, parent fingerprints, and declared I/O types. | Add a rule and adjust downstream filename patterns. A wrong wildcard, target name, or path convention can surface at DAG construction time rather than at the edit location. | Add a process and wire channels. The edit is explicit but often requires channel reshaping, tuples, joins, or process input/output declarations. | Define a task, then bind out = workflow.add(Step(value=previous)).out. Cache directories are derived by the engine; the task or shell specification chooses output filenames. |
| **Long pipelines** | Long workflows remain a sequence of local assignments: `P.bam = r.align(P.fastq)`, `P.sorted = r.sort(P.bam)`. Each line names a reusable intermediate; adding a downstream branch does not require renaming upstream paths. | Long workflows often accumulate many interacting filename patterns, wildcards, rule-order assumptions, and target expansions. Compact once stable, but exploratory refactors can require coordinated edits across rules. | Long workflows scale well operationally, but the author must keep channel contents, tuple structure, and process contracts aligned across many transformations. | Local variables and nested workflow functions keep wiring procedural. Explicit workflow.add calls and output-field selection remain; declared workflow returns select returned fields, not execution targets. |
| **Typed I/O** | `NodeType` subclasses are checked at rule-call time with subtype polymorphism; scalar config kwargs are checked with Python types. | No. Rule I/O is primarily file paths and wildcards; semantic types such as `Bam` vs `Fastq` are naming conventions. | Common DSL2 pipelines still pass paths, values, tuples, and maps rather than Python-like domain types. | Python annotations and fileformats contracts validate task values and lazy connections. Supports ordinary Python types and domain file types; Any or generic contracts provide weaker guarantees. |
| **Early wiring-error feedback** | Calling `r.quantify(fastq)` when the rule expects `SortedBam` raises immediately in Python, before execution or scheduler submission. | Many errors are caught by DAG construction, but semantic file-type mismatches depend on filenames, wildcard constraints, or custom validation. | Channel shape and missing output errors are caught by the runtime; semantic mismatches are usually convention- or schema-driven. | Annotated values and connections are checked during task/workflow construction, before component jobs execute. The decorated constructor can run when submission constructs the graph. |
| **Cache identity** | Outputs live at nodes/rule/provenance_hash/filename. Recipe structure, declared I/O, command/config, selected shell, and parent lineage determine identity; scheduling constraints and repeat do not. | Optional between-workflow caching hashes steps, parameters, software stacks, and raw inputs for selected cacheable rules. | Task executions are stored in a task cache and work directory; `-resume` checks task hash plus presence of outputs. | Task definitions and inputs determine checksum cache directories. Typed File/FileSet data enters hashing; plain string paths do not provide equivalent content tracking. |
| **Staleness model** | Mtime-gated content hashes (BLAKE3 default) and consumed-parent metadata classify cache state. Rebuilds cascade by default; --reuse-unchanged permits reuse of identical rebuilt inputs. Failed/interrupted/running states force reruns. | Mtime and metadata/rerun triggers for code, input, params, and software environment; summary and list-change commands expose planned reruns. | Task hash includes task name, container/environment, inputs, script, globals, and other metadata; default file cache mode considers path, mtime, and size. | Reuses successful checksum results; typed file content changes select new work. File hashes use an mtime-keyed optimization. rerun and propagate_rerun control forced rebuilds; no Necroflow consumed-parent/state classifier. |
| **Provenance near outputs** | Each output directory has `.rip/dependencies.toml`, per-output content hashes, `job.log`, `state`, and an ancestor graph. | Snakemake tracks metadata, reports, summaries, and archives; reports combine results with parameters, code, and software. | Nextflow stores task metadata in `.nextflow/cache`, work dirs, logs, trace/report files, and platform metadata; provenance is not a per-output TOML sidecar. | Task/workflow cache directories hold serialized job, result, and error records. Optional auditing adds JSON-LD provenance and resource records; this differs from Necroflow's default plain TOML sidecars. |
| **Multi-sample reuse** | Add one `Pipeline` per sample to a shared `DAG`; identical upstream nodes collapse by key automatically. | Wildcards and target expansion naturally handle large sample sets; shared products can be modelled explicitly or through cached rules. | Channels are well suited to fan-out/fan-in and streaming many samples. | split/combine on tasks and workflows expresses mapped/gathered dimensions; identical effective tasks can reuse a shared checksum cache. |
| **Inspection from Python** | `Pipeline` and `DAG` are importable objects; tests can call the workflow, inspect `P.nodes`, resolve commands, and assert node types without spawning a runner. | Snakemake has APIs and dry-run/DAG commands, but the primary graph source is a Snakefile DSL. | No. Nextflow graphs are scripts executed by the Nextflow runtime; Python unit tests cannot directly inspect pipeline objects. | Workflow tasks expose construct() to build inspectable Python workflow/graph objects; constructor and wiring behavior can be checked without a cluster backend. |
| **Execution backends** | Local `ThreadPoolExecutor` with resource caps, pluggable scheduler, `keep_going`, dry-run, and autoclean. No native HPC/cloud executor yet. | Local, server, cluster, grid, and cloud execution without changing the workflow definition; executor plugin ecosystem. | Local, HPC schedulers, Kubernetes, AWS Batch, Azure Batch, and other cloud executors through configuration. | Bundled debug, ConcurrentFutures, SLURM, and SGE workers; Dask and PS/IJ are separate worker packages. No bundled AWS Batch executor in inspected 1.0a11. |
| **Environment management** | Built-in Docker configuration with image digest policy, container-capable commands, read-only inputs, and writable workdirs. No bundled Conda/environment solver; host tools remain user-managed. | Conda, containers, wrappers, reports, and workflow archives are mature. | Containers, Conda, Spack, modules, Wave, and cloud deployment are first-class in the ecosystem. | shell tasks support Docker and Singularity environments, with separate native Python tasks. Host tool versions and undeclared dependencies still need explicit environment modeling. |
| **GUI / observability** | Optional local browser GUI for pipeline visualisation and target selection; job logs live next to outputs. | DAG visualisation, reports, monitoring, and community UI tooling. | Reports, traces, logs, and Seqera Platform. | Python workflow visualization, cache/job/error records, and optional auditing/resource monitoring. A Necroflow-style request/result browser is not supplied by this example. |
| **Main advantage over Necroflow** | — | Much larger ecosystem, reusable wrappers, mature production deployment, environment management, and cluster/cloud support. | Best-in-class production portability, nf-core ecosystem, channel dataflow, and cloud/HPC execution. | Typed Python values, nested workflows, built-in split-dimension propagation/grouping, and existing SLURM/SGE workers. Necroflow loops/grid can also express static mapped DAGs. |
| **Main advantage of Necroflow** | Typed domain objects, lineage-addressed paths, per-output provenance, Python object inspection, and natural Python control flow in one small framework. | Avoids wildcard/path-pattern coupling for local development and catches semantic type mistakes at rule-call time. | Avoids a second JVM/Groovy language and lets Python tests inspect the graph directly. | Labels support user-selected target closures after full graph construction; default file-adjacent provenance and request-driven CoW result publication are built in. |
| **Best fit** | Exploratory and shared-filesystem bioinformatics projects where the author wants Python-native development, deterministic cache paths, and rich local provenance. | Published or production bioinformatics workflows requiring mature ecosystem, deployment profiles, and community practices. | Large consortia, nf-core-style pipelines, cloud/HPC portability, and complex fan-out/fan-in dataflow. | Scientific workflows mixing Python objects and files, repeated map/gather composition, reusable nested workflows, and shared-filesystem HPC execution. |

Pydra evidence: [function-defined workflows](https://nipype.github.io/pydra/tutorial/6-workflow.html),
[types and file formats](https://nipype.github.io/pydra/explanation/typing.html),
[caching](https://nipype.github.io/pydra/explanation/hashing-caching.html),
[execution backends](https://nipype.github.io/pydra/tutorial/2-advanced-execution.html),
and [software environments](https://nipype.github.io/pydra/explanation/environments.html).
The runnable example, inspected installed source, and request-selection probe
support the more detailed distinctions below. No HPC/cloud throughput or
production-reliability benchmark is implied.

### Pydra HPC and AWS support

These are capability assessments of the inspected 1.0a11 release, not numerical
benchmark scores. The local example tests do not validate cluster deployment.

| Target | Assessment | Supplied support and remaining setup |
|---|---|---|
| Existing SLURM cluster | Direct support | Bundled worker submits with sbatch, monitors with squeue/sacct, accepts sbatch_args, and includes requeue handling for selected states. The submit host/compute nodes must be able to access the serialized jobs, cache/input paths, and Python environment used by the generated scripts. |
| Existing SGE cluster | Direct support | Bundled SGE worker; cluster configuration and shared execution environment remain deployment responsibilities. |
| More schedulers/distributed execution | Plugin-based | Separate PS/IJ and Dask worker packages extend execution choices; availability and configuration depend on the installed plugin. |
| AWS EC2 machine | Ordinary host execution | Can run local process workers on a suitably configured EC2 host. This does not itself provision an elastic cluster or integrate AWS Batch/S3/IAM. |
| AWS Batch | No bundled worker | The inspected 1.0a11 worker set has no AWS Batch executor. A custom/integrated backend or separately managed execution setup is needed; this is not a claim that no third-party implementation could exist. |
| AWS-hosted SLURM cluster | Uses SLURM interface | A suitably provisioned AWS SLURM deployment can expose the same scheduler interface; Pydra does not thereby supply the AWS infrastructure or data staging. |

The SLURM worker's implementation writes serialized jobs and launches the same
Python executable on the compute node, so accessible paths and an available
Python/tool environment are practical deployment requirements. This is a
source-based assessment; no live SLURM or AWS job was submitted. See the
[pinned SLURM worker](https://github.com/nipype/pydra/blob/1.0a11/pydra/workers/slurm.py)
and [worker/plugin interface](https://github.com/nipype/pydra/blob/1.0a11/pydra/workers/base.py).

For comparison, Nextflow documents an
[AWS Batch executor](https://www.nextflow.io/docs/latest/executor.html#aws-batch),
and Snakemake provides an
[AWS Batch executor plugin](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/aws-batch.html).
Pydra has an existing path for conventional SLURM execution; those other engines
have documented dedicated integrations when AWS Batch is the intended backend.

## Current approaches: Necroflow vs Pydra 1.0a11

This table covers authoring, request selection, storage, and publication. Pydra
observations refer to the pinned 1.0a11 installation, not an
assumed property of every release.

| Aspect | Necroflow | Pydra 1.0a11 function-defined workflow |
|---|---|---|
| Define a workflow | `@workflow` function receives a `Pipeline`, normally with config. | `@workflow.define` function receives typed inputs and returns selected output fields. |
| Procedural style and local variables | `raw = import_text(...)`; pass `raw` to later rules. | `raw = workflow.add(import_text(...)).out`; pass `raw` to later tasks. Both support variable-based wiring. |
| Create/register graph work | A rule call immediately creates/interns its RuleCall and Nodes in the active Pipeline's DAG. | Instantiating a task defines its inputs; `workflow.add(task)` registers it during workflow construction and returns output fields. |
| Meaning of the local output variable | A typed Node with its lineage-derived path already determined. | A lazy output reference; execution supplies the actual typed value/file. |
| Meaning of `P.raw = raw` | Publishes a label for an existing Node; the rule call already registered the work. | No exact assignment counterpart. `workflow.add` registers work; workflow output names and the constructor's return publish returned fields. |
| Internal task name vs public output name | Pipeline labels identify requestable Nodes; several labels may alias one Node. | `workflow.add(task, name="raw")` names an internal task. A named workflow return is a separate output field. An internal task name is not a built-in user request selector. |
| Declare many outputs, request one later | Job TOML `.requests = ["raw"]` selects the labelled target after full workflow construction. Python callers can use `dag.require([P.raw])`. | No direct `.requests` equivalent in the inspected API. Selecting/returning one field does not automatically prune other registered tasks. |
| Execution scope for a request | Executes the selected Nodes' ancestor closure, subject to caching; unrelated and downstream calls remain inactive unless separately required. | Executes registered workflow tasks, subject to caching, including registered downstream/unrelated work not needed for returned fields. |
| User requests several targets | `.requests` can contain several labels; executor takes the union of their ancestor closures. | Author must implement requested-target parameters, graph construction, or suitable subworkflows. Reading several returned fields alone is not execution filtering. |
| Avoid unrequested work | Build the full graph, then select targets independently of construction. | Conditionally add tasks, return before adding later steps, or compose a smaller workflow. Returning after all tasks were added does not remove them. |
| Configuration-dependent branching | Ordinary Python `if`/loops while constructing the graph. | Ordinary Python `if`/loops on available constructor inputs. Lazy task outputs are placeholders, not computed values for immediate Python branching. |
| Reusable fragments | Helpers/decorated workflows can share a Pipeline; `P.subpipeline(prefix)` namespaces labels. | Decorated workflows can be nested as tasks; helpers can add tasks within construction. |
| Static mapped computation | Python loops and `__grid` enumerate concrete rule calls; DAG interning shares identical upstream work as the graph is built. | Splitters enumerate task states; built-in dimension propagation and combiners track how mapped results are passed and grouped. Static mapping is expressible in both approaches. |
| Grouping mapped products | Author collects ordered Node tuples by the intended grouping keys and passes them to a gather rule. | A combiner specifies which split dimensions to collect. Combining groups outputs; an additional task performs a numerical/file reduction. |
| Persistent cache in the executable example | Necroflow supplies its Node store and consumed-parent metadata. | Pydra supplies its task/workflow checksum cache; cross-interpreter reuse and input/config invalidation are verified in the example checks. |
| Task identity hash | Recipe/provenance identities use SHA-256 over the declared recipe, parameters, and parent lineage. | Task/input hashing uses BLAKE2b with a 16-byte (128-bit) digest in the inspected 1.0a11 implementation; the checksum names its cache directory. |
| File content hash | BLAKE3 by default, with SHA-256/custom alternatives; stored output digests are compared with consumers' recorded parent hashes. | Typed File/FileSet contents enter the BLAKE2b object hash; persistent path/mtime hash caching avoids unnecessary re-reading. Plain string paths are not equivalent to typed file inputs. |
| Storage equivalent to `nodes/` | Executed RuleCalls write file products under `nodes/<rule>/<provenance_hash>/<filename>`. | Executed tasks write file products under `<cache_root>/python-<hash>/...` in this example. Other task kinds have corresponding cache prefixes. |
| Workflow-level file collection | The node store remains canonical; no separate workflow cache bundle is required before publishing results. | Returned files are collected in the workflow's own `<cache_root>/workflow-<hash>/...` directory. Unreturned task products can remain in task cache directories. |
| Storage equivalent to `results/` | CLI publishes requested labelled outputs under `results/<job>/<label>/...`, using CoW clones where supported, plus a manifest. | No matching built-in publication step in this example's Pydra API. Our wrapper exports the four returned files into `results/<label>/...` with `shutil.copy2` and writes `outputs.json`. |
| Result storage allocation | Linux publication requests reflinks; macOS requests clones. Data extents are initially shared when supported, with physical-copy fallback when cloning is unavailable. | Our wrapper has no explicit CoW/reflink policy or guarantee. Its `shutil.copy2` publication should not be presented as matching Necroflow's clone policy. |
| User-facing results and launch interface | Built-in job TOML, request labels, requested result clones where supported, and manifest. | Our example supplies argument parsing, `shutil.copy2` exports, and `outputs.json`; these are application code around `Submitter`. |

**Request-selection probe:** with a fresh cache, a 1.0a11 workflow registered
`selected`, a task downstream of `selected`, and an unrelated task, then returned
only `selected`. All three tasks executed. Returning an intermediate field is
therefore not equivalent to Necroflow's target selection. This was checked with
the local `debug` worker; the execution graph implementation also constructs
execution nodes from every registered workflow node. See the
[Pydra workflow tutorial](https://nipype.github.io/pydra/tutorial/6-workflow.html),
[execution graph implementation](https://github.com/nipype/pydra/blob/1.0a11/pydra/engine/workflow.py),
and Necroflow's [request selection documentation](docs/job-toml.md).

### Storage and publication in the executable example

Pydra provides the cache layer: each executed task has a hash-addressed working
directory containing its file outputs and metadata. The workflow has a separate
cached result; returned file products are collected in its cache directory. That
workflow directory remains part of the engine cache, not a user-facing job
publication tree. Collecting returned files does not mean every task product is
exported from the workflow, and it does not restrict which registered tasks run.

The additional `results/` tree below is created by our script's `shutil.copy2`
loop, which explicitly exports `raw`, `tool_config`, `processed`, and `summary`:

```text
build/pydra-canonical/
  cache/
    python-<task-hash>/...         # task files and cached result metadata
    workflow-<workflow-hash>/...   # workflow result and returned file products
  results/                        # our wrapper's publication step
    raw/input.txt
    tool_config/tool_config.json
    processed/processed.txt
    summary/summary.txt
    outputs.json
```

Necroflow's CLI supplies both the canonical node store and the separate,
request-driven publication step. Publication uses **copy-on-write clones where
supported**, rather than necessarily duplicating all file data: Linux requests
GNU `cp --reflink=auto`, while macOS requests cloning with `cp -c`. Separate
result files initially share data extents with their cached sources; writes to
a clone do not modify its source. Physical copying is the fallback when cloning
is unavailable.

Pydra's cache is analogous to the node store; our wrapper adds the familiar
results layout using `shutil.copy2`, without an explicit CoW policy. Matching the
directory layout alone therefore does not reproduce Necroflow's result storage
behavior. This comparison does not claim that Pydra's internal workflow file
collection always physically duplicates data; that is a separate mechanism from
our wrapper's publication step. See the
[Pydra cache documentation](https://nipype.github.io/pydra/explanation/hashing-caching.html),
[workflow output collection](https://github.com/nipype/pydra/blob/1.0a11/pydra/compose/workflow.py),
[example export code](examples/pydra_canonical/pipeline.py), and
[Necroflow storage documentation](docs/caching.md).

### Hash algorithm versus cache identity

Both engines hash work and inputs to support persistent reuse. Necroflow's
default **BLAKE3 file-content hashes** are distinct from its **SHA-256 recipe and
provenance identities** used to name node-store paths. Pydra 1.0a11's generic
hashing implementation calls `blake2b(digest_size=16, person=b"pydra-hash")`;
task checksums use those hashes, and typed file data participates in them.
Identical goals do not imply identical hash keys or lineage/reuse semantics.
See [Necroflow identity implementation](src/necroflow/fingerprints.py),
[Necroflow file hashers](src/necroflow/hashers.py),
[Pydra hashing implementation](https://github.com/nipype/pydra/blob/1.0a11/pydra/utils/hash.py),
and [Pydra cache documentation](https://nipype.github.io/pydra/explanation/hashing-caching.html).

## Splitters versus explicit DAG construction

Read against Jarecka et al.,
[Pydra: a flexible and lightweight dataflow engine for scientific analyses](https://doi.org/10.25080/majora-342d178e-012),
SciPy 2020, pp. 132-139, supplied locally as `pydra.pdf`. The splitter figures
are on printed pp. 135-136 (PDF pages 4-5); the sine example is on printed
p. 137 (PDF page 6). This paper uses the older Pydra API; the runnable adaptation
uses the pinned 1.0a11 API.

For statically known parameter sets, Necroflow can construct the corresponding
concrete computation using normal Python and `__grid`, then immediately intern
equivalent rule calls in the shared DAG. Splitters do not introduce a kind of
static parallel computation that Necroflow cannot express. Pydra's additional
abstraction is explicit state/dimension tracking and automatic propagation and
grouping of mapped outputs.

Expansion and reuse are separate mechanisms: a Pydra splitter creates mapped
states, while its cache avoids repeated work. In Necroflow, loops/`__grid`
enumerate calls, DAG interning deduplicates them during construction, and the
node store supplies reuse across executions.

| Paper construct | Meaning | Necroflow construction |
|---|---|---|
| `S = x` | One call per element of `x`. | `for x in xs`, or a single `x__grid`. |
| `S = (x, y)` (Fig. 4) | Pair corresponding positions; input lengths must match. | `zip(xs, ys, strict=True)` or a single grid dimension of paired records. Two independent `__grid` keys would instead form a product. |
| `S = [x, y]` (Fig. 5) | Every combination of `x` and `y`. | Nested loops, `itertools.product`, or independent `x__grid` and `y__grid` keys. |
| `S = [a, (b, c)]` | Cartesian product of `a` with paired `b/c` values. | `product(as_, zip(bs, cs, strict=True))`. |
| Propagated split state | Downstream mapped tasks inherit the appropriate indexed combinations. | Explicitly pass the corresponding Nodes while constructing each indexed branch; preserve keys/collections needed later. |
| `C = y` (Fig. 6) | Collect all `y` outputs for each fixed `x`. | Build an ordered tuple of result Nodes for each `x`; feed each tuple to a gather rule if a file product is needed. |
| `C = [x, y]` (Fig. 7) | Collect all mapped outputs into one ordered list. | Collect all result Nodes into one ordered tuple and pass it to a gather rule. |
| A task produces a list which another task splits | Determine mapped inputs after upstream execution. | If the list is derivable from config, enumerate it during construction. Otherwise discover/materialize it first and construct a later graph/stage; `__grid` alone does not add runtime fan-out to a finished Pipeline. |

### Concrete examples from the paper

Figures 4-7 apply addition to `x=[1,2]`, `y=[10,100]`:

| Operation | Scalar calls | Collected output |
|---|---|---|
| Paired split `(x,y)` | `add(1,10)`, `add(2,100)` | `[11,102]` |
| Product split `[x,y]`, full combine | `add(1,10)`, `add(1,100)`, `add(2,10)`, `add(2,100)` | `[11,101,12,102]` |
| Product split `[x,y]`, combine `y` | Same four calls | `[[11,101],[12,102]]`, one group per `x`. |

The paper's phrase "nested loops" largely describes this splitter syntax. In a
function-defined Pydra workflow, an ordinary Python loop constructs graph nodes;
it is not an instruction to iterate over a lazy upstream result that has not
been calculated yet. Splitters also handle that runtime-list case and keep its
index/group state. Nested workflows offer another way to defer construction
until those values become available.

The sine example demonstrates the latter interface: `powers(n_max)` returns
`[0, ..., n_max]`; `term(x, n)` is split over that returned list; `.combine("n")`
collects term values; a separate sum task computes the approximation. In this
particular example, the indices are also derivable from `n_max` before execution,
so an explicit loop can construct the same term computations without a discovery
task. A genuinely data-discovered list is a different case.

[splitter_example.py](examples/pydra_canonical/splitter_example.py) runs both the
addition examples and the sine workflow using the existing isolated venv:

```sh
make pydra-splitter-example
```

The sine workflow keeps the mapping/grouping/reduction steps visible:

```python
indices = workflow.add(powers(n_max=n_max)).out
terms = workflow.add(term(x=x).split(n=indices).combine("n")).out
approximation = workflow.add(sum_terms(terms=terms)).out
```

The machine-learning example on printed p. 138 similarly reuses fitted models
when metrics are changed. In Necroflow, keep metric parameters confined to
downstream scoring rules: the same model-training calls retain their identity
and can be shared/reused. This reuse does not require a splitter abstraction.

### Verified Necroflow expansion and grouping

A local construction-only probe used `sample__grid` with 2 values,
`aligner__grid` with 3 values, and `threshold__grid` with 4 values. All 24 configs
called source, alignment, and filtering rules in separate Pipelines sharing one
DAG. Immediate interning produced:

| Stage | Unique RuleCalls |
|---|---:|
| Source depends only on sample | 2 |
| Alignment depends on source and aligner | 6 |
| Filtering depends on alignment and threshold | 24 |
| **Before grouping** | **32** |
| Gather thresholds for each sample/aligner pair | 6 |
| **Including gather rules** | **38** |

The repeated source calls returned the same Node objects. Six groups of four
filtered Nodes were accepted as ordered variadic inputs to gather rules. This
checks graph construction and grouping, not execution speed. Deduplication
depends on declaring relevant inputs/parameters: passing unrelated grid values
into an upstream rule would change its identity and can defeat intended reuse.

### Remaining distinctions

- Pydra tracks dimensions and groups for the author. Necroflow authors maintain
  grouping keys and ordered Node collections explicitly; ordering and repeated
  positions must be preserved even when the underlying computation is shared.
- Pydra can unpack an upstream-produced sequence and extend nested execution.
  Necroflow's finished Pipeline is fixed; genuinely unknown fan-out requires
  discovery followed by further construction/staging in the current API.
- Printed p. 134 describes submission using a serialized task plus a state index
  rather than duplicating a full task object per mapped job. Necroflow eagerly
  stores concrete unique calls/Nodes. This is a representation and construction
  cost tradeoff; neither paper figures nor our probe benchmark memory scaling.
- Pydra tasks can exchange typed Python values. Necroflow's gather dependencies
  are file/artifact Nodes, so representation/materialization differs.
- This is a mapping of computational patterns, not a claim of identical cache
  keys: Necroflow's lineage identity and Pydra's task/input hashing can yield
  different reuse behavior for equal bytes from different upstream lineages.

## Measured Python source size

Measured 2026-10-19 from current upstream Pydra `main`, commit
[`928ff51f9f03a076e854538fd798a606108d6bbb`](https://github.com/nipype/pydra/tree/928ff51f9f03a076e854538fd798a606108d6bbb)
(commit date 2026-09-25), and this Necroflow working tree. Pydra's source archive
was downloaded and counted locally; these are measurements, not upstream size
claims.

| Scope | Python files | Code lines excluding blanks, comments, docstrings | All physical Python lines |
|---|---:|---:|---:|
| Pydra main: runtime package | 46 | 9,757 | 15,014 |
| Pydra main: tests and test configuration | 46 | 17,355 | 25,056 |
| Pydra main: other Python files | 4 | 157 | 519 |
| **Pydra main: all Python** | **96** | **27,269** | **40,589** |
| Pydra 1.0a11: installed runtime package used by our example | 47 | 9,814 | 15,106 |
| Necroflow: runtime package | 29 | 5,303 | 6,733 |
| Necroflow: tests | 28 | 7,823 | 10,939 |

The runtime-package code count is approximately **1.84 times** Necroflow's with
the same rules. Dependencies, virtual environments, generated `_version.py`,
bytecode, Markdown documentation, and notebook cells are excluded. Test files,
test directories, and `conftest.py` are counted separately from runtime code.
Code lines are physical nonblank lines containing Python tokens after removing
comments and module/class/function docstrings identified through the AST.

For comparison with the existing `necroflow-count-loc` convention, which keeps
docstrings but excludes blank and full-line `#` comments, Pydra main's runtime
package has **12,485** lines and Necroflow's has **5,781**. The physical-line
column above includes comments, docstrings, and blanks.

## Historical feature matrix: Pydra 0.25

The Pydra column refers to **0.25** throughout. “Application code” means a feature
can be built around the engine but has no direct counterpart to the Necroflow
interface described here. This is a feature comparison, not a speed benchmark.
For the executed 1.0a11 workflow and its procedural authoring style, see the
next two sections.

| Feature | Necroflow in this tree | Pydra 0.25 |
|---|---|---|
| Workflow definition | `@workflow` functions call rules in an active `Pipeline`; explicit `rule(P, ...)` also works. | `Workflow.add(task(...))` with lazy input/output links (`lzin`, `lzout`); `set_output` declares workflow returns. |
| Native task abstraction | File-producing rules: shell commands, command-generating callbacks, built-in text/symlink materializers. Other execution can use a custom runner. | `@mark.task` / `FunctionTask` wraps ordinary Python computations; `ShellCommandTask` models executable arguments and file outputs. |
| In-memory values | Scalar/container parameters configure rules; dependency products are `Node` file/artifact values. | Tasks can exchange typed Python values as well as files. A file is not required for every intermediate. |
| Type contracts | Domain-specific `NodeType` subclasses, subtyping, unions, mixed node/value inputs, bounded ordered variadic inputs. Wiring errors fail at rule calls. | Annotated functions and attrs specifications validate/coerce inputs and check lazy connections. `fileformats` supplies typed file contracts. Pydra also has semantic file types. |
| Output naming | `NodeType.filename`; framework derives `rule/provenance_hash/filename` before execution. | Framework derives checksum task directories; functions choose filenames inside their working directory or shell specifications describe outputs. |
| Multiple outputs | Co-output Nodes share one atomic `RuleCall`, identity, execution, state, and cleanup. | One task may have multiple typed output fields in one cached result. |
| Reusable composition | Helpers and decorated subworkflows add nodes; `P.subpipeline(prefix)` namespaces labels on the same DAG. | A `Workflow` is itself a task and can be nested inside another workflow. |
| Branching | Ordinary Python `if`/loops during graph construction. | Ordinary Python during graph construction; lazy task results are resolved later, so they are not ordinary eager values for Python `if`. |
| Parameter sweeps | TOML `__grid` expansion or Python loops adding pipelines to a shared DAG. | `.split(...)` expresses paired or Cartesian iteration on tasks/workflows; state propagates through dependent tasks. |
| Gathering results | Connect ordered collections of Nodes to a variadic rule; construct grouping with Python. | `.combine(...)` gathers selected split dimensions; a downstream task can perform the reduction. Combining alone is not a numeric reducer. |
| Shared work | Equivalent calls immediately intern to canonical Nodes/RuleCalls across Pipelines in one DAG. | Equivalent task definitions and inputs can reuse checksum results in a shared cache; `cache_locations` adds other cache search locations. This is different from Necroflow's eager object interning. |
| Cache keys | Recipe plus parameter values, shell policy, declared I/O, and parent lineage. Constraints/retry count are excluded. | Task/input hashing includes the serialized callable for `FunctionTask`; workflows account for their graph. Actual typed file inputs contribute file data hashes. |
| Input file tracking | Use an ingested Node, e.g. `symlink(Type, path)`, to track external bytes. A string path parameter tracks only its text. | Use a typed `File`/`FileSet`; a `str` or ordinary `Path` is not a substitute for content-tracked file input. |
| Hash optimization | Mtime-gated stored content hashes; BLAKE3 default, SHA-256 or custom hasher available. | FileSet hashing has a persistent path/mtime keyed hash cache; object hashes use BLAKE2b. Neither should be presented as detecting arbitrary edits with preserved timestamps. |
| Rebuild propagation | Active descendants rerun by default after a rebuild. `reuse_unchanged=True` / `--reuse-unchanged` permits content-based preservation. | Content-based task caching plus `rerun` and workflow `propagate_rerun` controls. These are not a one-to-one replacement for Necroflow's rebuild policy. |
| Cache validation | Records consumed parent hashes and failed/interrupted/running state; executor classifies missing, stale, and compromised calls. | Successful cached result loading is the main fast path. It does not implement Necroflow's consumed-parent-hash/state classifier. Do not infer equal detection of edited/deleted cached products from support for input hashing. |
| Provenance and state | Plain `.rip/` files: dependencies, content hashes, state, logs, runtime record, ancestor graph. CLI adds TOML manifests and run summaries. | Serialized task/result/error records; optional JSON-LD provenance and resource monitoring through audit flags/messengers. Provenance exists in both, with different formats and defaults. |
| Local execution | Thread pool orchestrates subprocesses/materializers. Resource pool admits calls using aggregate thread/RAM/custom caps. | Serial or `ConcurrentFutures` process workers. Local `n_procs` limits worker processes; it is not Necroflow's weighted aggregate resource pool. |
| HPC/distributed execution | No bundled cluster/cloud backend in this tree. Scheduler callbacks order local ready calls; they do not submit remote jobs. | Bundled SLURM and SGE workers; Dask and PS/IJ integrations require extra dependencies. Backend support does not imply those backends were exercised by this example. |
| Resource requests | Per-rule `threads`, `ram`, custom constraints; `threads="all"`; scheduler receives remaining capacity. | Worker-specific options, such as SLURM submission arguments and SGE thread/memory controls. Port resource settings explicitly for the selected worker. |
| Containers | Built-in `Docker` configuration, leading `{env}:` command prefix, image digest policy, read-only inputs/writable workdir. | Docker and Singularity environments for shell tasks. A Python function that starts a subprocess remains a host Python task unless explicitly wrapped differently. |
| Errors and retries | Missing declared outputs fail; `repeat=N` retries subprocess failures; `keep_going` preserves independent work and reports blocked descendants. | Exceptions/error results prevent dependent tasks from succeeding; `rerun` permits explicit reruns. Retry/resubmission behavior depends on the worker; there is no direct equivalent to the complete Necroflow interface above. |
| Requests and publication | `.requests` selects labelled target closure; CLI publishes requested outputs as CoW clones where supported, with physical-copy fallback, plus `manifest.toml`. | `set_output` chooses returned fields. It is not a `.requests` interface for pruning arbitrary unused tasks. Build the intended workflow; publication layout is application code. |
| Inspection and CLI | Python DAG objects; `graph`, `outputs`, `explain`, `doctor`, `provenance`, dry-run, optional read-only MCP. | Python workflow/task graph inspection and DOT export. Comparable job TOML, preflight/why-run CLI, result browser, or MCP adapters require application/integration code. |
| Cleanup | Intermediate/orphan autoclean and provenance-aware `gc` with preservation rules. | Cache directories are configurable; no equivalent Necroflow preservation-policy GC interface is used here. Application/deployment manages retention. |
| Environment identity | Declare tools, configs, and container choices explicitly. Installed host tool versions are not automatically captured. | Also requires explicit environment/tool identity. A hash of a Python wrapper does not automatically hash every external binary or imported package it invokes. |
| Runtime/platform | Python >=3.11; POSIX, with Linux/macOS support. | 0.25 package metadata requires Python >=3.8. This example is tested with Python 3.12 on Linux; its `tr`/`wc` commands require POSIX tools regardless of engine platform support. |
| Dependencies/license | Required runtime dependencies currently `blake3`, `tomlkit`; MIT. | Broader runtime set including attrs, cloudpickle, fileformats, and filelock; Apache-2.0. Optional workers add dependencies. |

## What the runnable port demonstrates

[examples/pydra_canonical/pipeline.py](examples/pydra_canonical/pipeline.py) ports
[examples/canonical/pipeline.py](examples/canonical/pipeline.py), the small
workflow exercised by Necroflow's canonical CLI tests. It imports a text file,
writes deterministic JSON config, uppercases the input, and counts output bytes.
No bioinformatics binaries, Docker, datasets, or compiler toolchain are needed.
The executed port uses **1.0a11**, not the 0.25 matrix's authoring API.

| Necroflow construct | Executed Pydra counterpart |
|---|---|
| `RawText`, `ToolConfig`, `ProcessedText`, `Summary` Nodes | Typed `File` task inputs/returns and named workflow outputs. The port uses generic files; it does not reproduce the four distinct domain types. |
| `P.raw = import_text(...)` | Inside `@workflow.define`, `raw = workflow.add(import_text(source=source)).out`. |
| `@text_file` config rule | Python task writing the identical sorted, indented JSON plus newline. |
| `process_text(P.raw, P.tool_config)` | `process_text(raw_text=raw, tool_config=tool_config)` uses local lazy output variables; config remains a dependency even though `tr` does not read it. |
| shell `tr` and `wc` rules | `@python.define` tasks invoking the same POSIX utilities with argv and file streams. The port does not use `@shell.define` tasks. |
| `DAG.require` and `dag.run` | Workflow function returns named products; `Submitter(worker="debug"/"cf", cache_root=...)` executes the task and returns `result.outputs`. All four tasks participate. |
| CLI result publication (CoW clones where supported) | Example's own `shutil.copy2` export wrapper and `outputs.json`; it has no explicit CoW policy and is not claimed as built-in Pydra publication. |

Static output bytes match, except that `wc` writes an engine-specific input path
alongside the count. The checks compare the numeric count rather than pretending
the entire summary file is path independent. With the bundled input, processed
text is `HELLO NECROFLOW` plus a newline and the count is **16**.

There is one deliberate ingestion improvement: the original canonical rule
accepts `path: str` and copies it with `cp`, so editing that source at the same
path does not itself invalidate Necroflow's cached import. The port uses
`source: File` so those edits change its cache identity. Necroflow can provide
content tracking by ingesting with `symlink(Type, path)`; this is an example modeling
difference, not a missing Necroflow capability. See
[external dataset ingestion](docs/caching.md#external-dataset-ingestion).

From the repository root:

```sh
make pydra-example  # create isolated venv, byte-compile, execute with two workers
make pydra-test     # compare engines; verify persistent caching and invalidation
```

See [example instructions](examples/pydra_canonical/README.md) for standalone
installation and CLI options. Tests cover both bundled inputs, serial/parallel
execution, paths containing spaces, cache reuse across fresh interpreters,
same-path source edits, and config dependency changes. They do not benchmark
throughput or validate remote workers, containers, multi-output atomicity, or
equivalence of every failure/recovery policy.

## Pydra 1.0 prerelease scope

Pydra's current development documentation uses `@python.define`, `@shell.define`,
`@workflow.define`, and worker-based submission rather than the 0.25
`@mark.task`/`Workflow.add` API. The 1.0a11 release requires Python >=3.11 and
advertises composable typed tasks, split/combine, shared caches, SLURM/SGE, and
separate Dask/PS-IJ worker packages. See the
[upstream architecture guide](https://github.com/nipype/pydra/blob/main/CLAUDE.md)
and [release description](https://pypi.org/project/pydra/1.0a11/).

The example now pins and tests **1.0a11**. Its workflow is:

```python
@workflow.define(outputs=["raw", "tool_config", "processed", "summary"])
def canonical_pipeline(source: File, tool_text: str) -> tuple[File, File, File, File]:
    raw = workflow.add(import_text(source=source)).out
    tool_config = workflow.add(write_tool_config(text=tool_text)).out
    processed = workflow.add(process_text(raw_text=raw, tool_config=tool_config)).out
    summary = workflow.add(summarize(processed_text=processed)).out
    return raw, tool_config, processed, summary
```

Pydra therefore supports procedural-looking, variable-based workflow authoring.
Even 0.25 permits variables, although each task needs explicit registration and
lazy output access. In 1.0a11, `workflow.add` returns output fields, so `.out`
extracts the single default field and the resulting variables can be passed
directly downstream. Necroflow rule calls return typed output Nodes directly
and obtain registration context from `@workflow`; there is still an API
difference, but it should not be described as Pydra lacking procedural authoring.

The runnable checks validate this 1.0a11 construction, serial/parallel execution,
output parity, and cache reuse/invalidation. They do not revalidate every row of
the historical 0.25 feature matrix against 1.0. Version-matched task libraries
and worker configuration remain necessary for a new Pydra deployment.

The pinned implementation was inspected directly:
[workflow definitions and registration](https://github.com/nipype/pydra/blob/1.0a11/pydra/compose/workflow.py),
[Python task definitions](https://github.com/nipype/pydra/blob/1.0a11/pydra/compose/python.py),
[submission](https://github.com/nipype/pydra/blob/1.0a11/pydra/engine/submitter.py),
and [results](https://github.com/nipype/pydra/blob/1.0a11/pydra/engine/result.py).

## Practical choice

Necroflow is a good fit when the main objects are persistent file products and
the desired interface is Python graph construction plus job TOMLs, target
selection, local resource limits, and metadata next to outputs. Pydra is a good
fit when workflows exchange Python objects as well as files, need repeated
map/gather composition or nested workflows, or require an existing HPC worker.
The feature overlap is substantial; output identity, environment modeling,
failure semantics, and operational requirements should drive a migration.

## Evidence

Necroflow: [rules/types/containers](docs/rules.md),
[cache and metadata](docs/caching.md), [executor](docs/executor.md),
[scheduler protocol](docs/schedulers.md), [job selection/grids](docs/job-toml.md),
[CLI](docs/cli.md), and [package metadata](pyproject.toml). The existing
[broader comparison](COMPARISON.md) predates some current features; this file
uses the inspected working tree rather than copying those older descriptions.

Pydra 0.25 evidence was checked against the installed release source, with these
upstream equivalents: [task definitions](https://github.com/nipype/pydra/blob/0.25/pydra/engine/task.py),
[workflow, caching, split/combine and output selection](https://github.com/nipype/pydra/blob/0.25/pydra/engine/core.py),
[type specifications](https://github.com/nipype/pydra/blob/0.25/pydra/engine/specs.py),
[type validation](https://github.com/nipype/pydra/blob/0.25/pydra/utils/typing.py),
[object/file hashing](https://github.com/nipype/pydra/blob/0.25/pydra/utils/hash.py),
[workers/resources](https://github.com/nipype/pydra/blob/0.25/pydra/engine/workers.py),
[container environments](https://github.com/nipype/pydra/blob/0.25/pydra/engine/environments.py),
[auditing](https://github.com/nipype/pydra/blob/0.25/pydra/engine/audit.py),
[submission/dependency handling](https://github.com/nipype/pydra/blob/0.25/pydra/engine/submitter.py),
and [release metadata/license](https://pypi.org/project/pydra/0.25/).
