# Necroflow Manuscript: Argument Conspect

This document condenses the manuscript arguments into a review-oriented outline. References in parentheses point to sections and labels in `main.tex`.

## 1. Central Thesis

- Bioinformatics workflows need reproducible dependency tracking, caching, and provenance; these needs should not force authors into a separate workflow language. (`sec:intro`, Abstract)
- Necroflow is proposed as a Python-native framework that preserves ordinary Python control flow while giving file-producing computations explicit, typed workflow meaning. (`sec:intro`, `sec:design`)
- The intended result is a framework whose pipeline logic remains inspectable and editable as Python, while its execution remains reproducible through a lineage-derived cache identity. (`sec:intro`, `sec:design:paths`)

## 2. Why Python-Embedded Construction Matters

- Bioinformatics pipelines often contain data-dependent calibration, QC, conditional branches, parameter sweeps, and alternative analysis paths; ordinary Python expresses these naturally. (`sec:intro`, `sec:design:abstractions`)
- Keeping construction in Python avoids translating between host-language logic and a separate DSL or dataflow language. (`sec:intro`, `sec:related`)
- Pipeline authors can use the normal Python ecosystem for parsing, validation, testing, debugging, IDE support, and library reuse. (`sec:intro`, `sec:discussion`)
- Dynamic graph edits are first-class authoring operations rather than exceptional escape hatches. (`sec:intro`, `sec:design:abstractions`)

## 3. Typed Rule Boundaries

- A `Rule` declares the accepted input artifact types, output artifact types, parameters, resources, and implementation. (`sec:design:abstractions`)
- Calling a rule yields typed nodes rather than immediately running a function; the nodes form a dependency graph. (`sec:design:abstractions`)
- Typed boundaries make invalid compositions visible early, before expensive execution, and make individual pipeline stages easier to understand. (`sec:design:abstractions`, `sec:related`)
- The framework treats files as meaningful artifacts rather than untyped strings passed between generic tasks. (`sec:design:abstractions`, `sec:prefect-side-by-side`)

## 4. Lineage-Derived Output Paths and Cache Identity

- Each generated output path incorporates rule identity, normalized parameter values, input identities, and output name through a fingerprint. (`sec:design:paths`)
- The output path is therefore a representation of the computation lineage, not a manually curated naming convention. (`sec:design:paths`)
- Identical logical work converges on the same cache location; a changed implementation, parameter, or upstream lineage receives a different location. (`sec:design:paths`)
- The directory layout keeps distinct logical computations separate even when their human-readable filenames coincide. (`sec:design:paths`)

## 5. Why Content-Addressed Paths Reduce Path Combinatorics

- Conventional workflow layouts often encode sample identity, tool choice, parameters, branch, and step in filenames or wildcard patterns. (`sec:development-effort`, `sec:massimo-side-by-side`)
- As choices multiply, authors must design and maintain an ever larger path taxonomy, while also preventing collisions and handling path-length limits. (`sec:development-effort`, `sec:path-combinatorics-example`)
- Necroflow moves this bookkeeping into the fingerprinted cache layout, so source code describes computations rather than output-path algebra. (`sec:design:paths`, `sec:path-combinatorics-example`)
- This is an authoring and provenance argument, not a claim that explicit paths are impossible in other frameworks. (`sec:massimo-side-by-side`, `sec:discussion`)

## 6. Longer Pipelines Are an Advantage When Decomposed

- The manuscript favors longer pipelines made of small rules with narrow, explicit responsibilities. (`sec:long-pipelines`, `sec:development-effort`)
- A longer graph can expose intermediate artifacts, make local changes more isolated, and enable reuse of stable stages across analyses. (`sec:long-pipelines`, `sec:design:abstractions`)
- This design becomes costly in systems where every additional stage requires additional wildcard and path management; lineage-derived paths lower that cost. (`sec:development-effort`, `sec:path-combinatorics-example`)
- `P.section(name)` records author-declared pipeline stages for graph presentation without changing dependencies, execution, caching, or provenance. (`sec:long-pipelines`)

## 7. Specialised Tools and Competing Implementations

- Narrow rule responsibilities allow a pipeline to select a specialised tool for one task instead of relying on a broad tool to perform several unrelated tasks. (`sec:specialised-tools`)
- Alternatives such as scoring or rescoring implementations can share the same typed input and output contract. (`sec:specialised-tools`, `sec:case:fragpipe`)
- Compatible alternatives can be substituted or compared without redesigning downstream stages, allowing rapid empirical competition between implementations. (`sec:specialised-tools`)
- Separate rule identities and output paths keep the outputs, caches, and provenance of alternatives distinct. (`sec:specialised-tools`, `sec:design:paths`, `sec:design:prov`)

## 8. Multi-Pipeline Reuse Is Structural

- A `Pipeline` can collect rule calls, and a `DAG` can combine pipelines and canonicalize shared nodes. (`sec:design:abstractions`, `sec:impl:executor`)
- Shared work is recognized by computation identity rather than by manually coordinating target filenames between separate scripts. (`sec:design:abstractions`, `sec:design:paths`)
- Independent pipeline factories can each construct the same upstream subpipeline, then branch into separate analyses; adding both pipelines to one DAG executes the identical prefix once. (`sec:development-effort`, `lst:shared-subpipeline`)
- This preserves independent, testable pipeline factories while sharing only work whose input, reference, command, and parameters are identical; a changed lineage deliberately creates a separate node. (`sec:development-effort`, `lst:shared-subpipeline`, `sec:design:paths`)
- This supports modular pipelines and side-by-side branches while retaining a single coherent dependency graph. (`sec:design:abstractions`)

## 9. Staleness Detection

- Cached outputs must not merely exist; they must still correspond to their recorded inputs and implementation. (`sec:design:stale`)
- Necroflow supports modification-time checks and stronger SHA-256 checks. (`sec:design:stale`, `sec:impl:cli`)
- Strong checking is presented as a correctness option for situations where timestamps alone are insufficient, with an explicit cost tradeoff. (`sec:design:stale`)
- Staleness state is connected to the output-local lineage record so it can be inspected and explained. (`sec:design:stale`, `sec:design:prov`)

## 10. Output-Local Provenance

- Each output is accompanied by local metadata recording its rule, parameters, inputs, command or implementation information, and execution state. (`sec:design:prov`)
- Provenance travels with the artifact location rather than living only in a central run database. (`sec:design:prov`)
- The manuscript argues that this makes later inspection, sharing, and debugging more direct. (`sec:design:prov`, `sec:discussion`)
- Output-local provenance also provides the substrate for commands that explain why an output is current or stale. (`sec:design:prov`, `sec:impl:cli`)

## 11. Local Executor and Resource Model

- The executor runs the required dependency closure for selected targets on a local machine. (`sec:impl:executor`)
- It respects declared resource requirements and configured capacity, allowing concurrent independent work when resources permit. (`sec:impl:executor`)
- A multi-output rule is executed once for its logical node, avoiding duplicate work for sibling targets. (`sec:impl:executor`)
- The executor can continue independent work after failures and can clean incomplete outputs, supporting iterative local development. (`sec:impl:executor`)

## 12. Scheduling

- FIFO scheduling provides a simple default that follows graph readiness. (`sec:impl:schedulers`)
- A connected-components scheduler is offered to favor locality within related regions of the graph. (`sec:impl:schedulers`)
- Schedulers receive ready and remaining nodes plus the available capped resources, allowing projects to implement resource-aware policies in Python. (`sec:impl:schedulers`)
- Snakemake offers greedy and ILP scheduler plugins aimed at global resource use, runtime, and temporary-file disk use; Necroflow keeps optimization optional because development-stage pipelines rarely have reliable runtime estimates. (`sec:impl:schedulers`)
- The scheduling discussion is deliberately practical and local-first rather than a claim of global cluster optimization. (`sec:impl:schedulers`, `sec:discussion`)

## 13. Crash Recovery and State

- Execution state is stored under `.rip`, separate from cached outputs and their provenance. (`sec:impl:statedb`)
- This state supports resuming work, tracking failures, and cleaning partial execution products. (`sec:impl:statedb`, `sec:impl:executor`)
- Separating ephemeral execution state from artifact provenance helps preserve the latter as a durable local record. (`sec:impl:statedb`, `sec:design:prov`)

## 14. One Job Description and Configuration Grids

- A TOML job description provides a declarative entry point for running a Python-defined pipeline. (`sec:impl:cli`)
- A grid mechanism expands parameter combinations for systematic analyses. (`sec:impl:cli`, `sec:case:fragpipe`)
- The manuscript positions TOML as a compact invocation interface, not as a replacement for Python pipeline definition. (`sec:impl:cli`, `sec:intro`)
- A labelled result folder provides named symlinks to requested artifacts, so a developer can request the complete upstream input set for a procedure under development, inspect or use it without copying cache files, then add the new procedure output to the same request. (`sec:impl:cli`)

## 15. Inspection and GUI

- CLI commands expose graph visualization, dry runs, stale explanations, cleanup, and cache inspection. (`sec:impl:cli`)
- The GUI is a thin local view over the same graph and provenance model, allowing users to inspect node state and relationships. (`sec:impl:gui`)
- Observability is treated as part of the workflow system rather than a separate reporting afterthought. (`sec:impl:cli`, `sec:impl:gui`, `sec:related`)

## 16. Comparison With Snakemake

- Snakemake is recognized as mature and capable, with strong rule-based workflow support. (`sec:related`, `sec:massimo-side-by-side`)
- The manuscript distinguishes Necroflow primarily through Python-native dynamic construction, typed artifact contracts, and automatic lineage-derived paths. (`sec:related`, `sec:massimo-side-by-side`)
- The comparison argues that a complex Snakemake workflow can be expressed, but explicit paths and wildcard constraints bear more of the complexity as variants accumulate. (`sec:massimo-side-by-side`, `sec:development-effort`)
- The paper does not claim that Snakemake lacks reproducibility features; it argues for a different abstraction boundary and maintenance profile. (`sec:related`, `sec:discussion`)

## 17. Comparison With Prefect

- Prefect demonstrates that Python-native dynamic orchestration is feasible. (`sec:prefect-side-by-side`, `sec:related`)
- The manuscript argues that generic task orchestration does not automatically provide typed file artifacts, lineage-derived cache paths, or file-oriented staleness semantics. (`sec:prefect-side-by-side`, `sec:design`)
- Necroflow therefore targets a more opinionated, file-centric workflow domain rather than general orchestration. (`sec:prefect-side-by-side`, `sec:related`)

## 18. Scope Relative to Nextflow

- Nextflow is presented as an established system for portable, scalable execution across containers, clouds, and HPC schedulers. (`sec:nextflow-scope`, `sec:related`)
- Necroflow intentionally emphasizes local Python-native composition, typed file boundaries, lineage-derived paths, and output-local provenance. (`sec:nextflow-scope`, `sec:design`)
- The contrast is a scope boundary, not a claim that Necroflow replaces a mature distributed-execution platform. (`sec:nextflow-scope`, `sec:discussion`)
- Users needing remote executors, broad deployment portability, or large-scale distributed scheduling remain outside the demonstrated Necroflow scope. (`sec:nextflow-scope`, `sec:discussion`)

## 19. CWL, WDL, and Luigi

- CWL and WDL foreground portability and declarative interoperability, with the corresponding cost of separate workflow representations. (`sec:related`)
- Luigi is an influential Python workflow system but is not centered on the same typed file contracts and lineage-derived output layout. (`sec:related`)
- These comparisons frame Necroflow as a specific combination of Python authoring, file semantics, and local reproducibility rather than a universal successor. (`sec:related`, `sec:discussion`)

## 20. RNA-seq Case Study

- The RNA-seq example demonstrates how common stages can be written as Python rules and composed into a visible graph. (`sec:case`)
- Its purpose is to evaluate readability and modeling of a realistic pipeline, not to benchmark biological accuracy, runtime, or resource use. (`sec:case`, `sec:discussion`)
- The example supports the argument that explicit artifact types and graph nodes make a multi-stage analysis legible. (`sec:case`, `sec:design:abstractions`)

## 21. FragPipe Translation

- The FragPipe-oriented case shows how a familiar proteomics pipeline can be decomposed into step-level rules. (`sec:case:fragpipe`)
- The decomposition exposes alternatives, calibration operations, rescoring stages, and parameter grids as independently addressable graph elements. (`sec:case:fragpipe`, `sec:specialised-tools`)
- This example anchors the argument for long pipelines and specialised competing implementations in a domain where those choices are scientifically relevant. (`sec:case:fragpipe`, `sec:long-pipelines`)

## 22. AI-Assisted Development Argument

- The manuscript observes that AI tools can make an initial pipeline implementation easier to produce, but do not eliminate the need for expert review and later adaptation. (`sec:discussion`)
- Python-native workflows keep that review and adaptation inside familiar programming tools and test practices. (`sec:discussion`, `sec:intro`)
- The framework value is therefore framed around maintainable semantic structure, not merely generating a first draft of code. (`sec:discussion`)

## 23. Limits and Future Work

- The manuscript does not claim a performance benchmark, complete comparison with other engines, or demonstrated cloud and HPC execution. (`sec:case`, `sec:discussion`)
- The local-first executor and schedulers are current implementation choices, not evidence that distributed execution is unimportant. (`sec:impl:executor`, `sec:impl:schedulers`, `sec:discussion`)
- Future work includes broader executors, stronger empirical evaluation, and continued validation on real scientific pipelines. (`sec:discussion`, `sec:conclusion`)
- Claims about scientific tool substitution are constrained by the need for empirical validation of results. (`sec:specialised-tools`, `sec:discussion`)

## 24. Condensed Logical Flow

1. Real bioinformatics pipelines need reproducibility, caching, provenance, and flexible control flow. (`sec:intro`)
2. Python offers the needed control flow and software ecosystem, but generic Python orchestration does not by itself give file-level workflow semantics. (`sec:intro`, `sec:prefect-side-by-side`)
3. Typed rules and graph nodes provide explicit artifact contracts and early validation. (`sec:design:abstractions`)
4. Lineage-derived paths give each computation a stable cache identity and remove much manual path bookkeeping. (`sec:design:paths`, `sec:path-combinatorics-example`)
5. This makes fine-grained, longer pipelines and specialised interchangeable stages practical to author and compare. (`sec:long-pipelines`, `sec:specialised-tools`)
6. Output-local provenance, staleness checking, local execution, and inspection tools make the graph operationally reproducible. (`sec:design:stale`, `sec:design:prov`, `sec:impl`)
7. The resulting system complements rather than replaces distributed workflow platforms such as Nextflow. (`sec:nextflow-scope`, `sec:discussion`)
