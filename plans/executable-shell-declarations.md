# Executable Shell declarations

Status: implemented and verified. Full suite: 693 passed, 2 skipped; Pyright clean.
Paper examples and cache scenarios verified; manuscript/supplement PDFs build.
Changes remain uncommitted.

The approved replacement executes ordinary Python inside a ContextVar-backed
construction context. Decoration analyses fixed top-level outputs assignments,
parameter/resource types, and normalized body source once. Each invocation binds
arguments and overrides, creates unregistered typed artifacts with final paths,
executes the function, validates exactly one registered Shell and exact returned
artifacts, and only then interns the call. Context always resets on failure.

No argument conversion, generated closures, public construction context manager,
or return of Shell is required. Commands cannot call other commands while building;
compose them in workflows. Outputs remain a fixed prefix and returned shape.
Paths are assigned before body execution because output schema and source identity
are known; no provisional Node class is needed. Repeated calls execute bodies again,
then reuse canonical artifacts. No framework directories are created by compilation.

Source logic and Python identity form a versioned recipe. Resource defaults/values
and built templates are excluded; ordinary config and input lineage remain hashed.
Migrated declarations get new hashes. Existing factories, external callbacks, and
built-in file materializers retain their established lifecycle. Construction
requires concrete threads (no deferred all sentinel); float resources and memory_bytes
remain supported. Resources must not change semantic outputs or dependencies.

Templates use existing quoting and brace escaping. Inspection executes construction
bodies, so printing/side effects occur there rather than in subprocess logs. External
state and helper implementations are not automatically captured by source hashes.

Migrate source, examples, typing/runtime tests, current docs, and active paper usage;
leave archived material and external ionmaiden checkouts unchanged. Verify context
cleanup/isolation, failed-call atomicity, source/resource hash behavior, types,
quoting, and existing execution/cache/provenance regressions. Run Pyright/full suite,
compile paper examples, and build PDFs. No commit or push requested for this step.
