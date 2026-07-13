# TODO

## GitHub Actions follow-up plan

1. [ ] Add a packaging validation job on Ubuntu with Python 3.14.
   - Build the source distribution and wheel with `uv build`.
   - Validate both artifacts with `twine check dist/*`.
   - Install the wheel into a clean virtual environment.
   - Smoke-test `necroflow --help` and `necroflow-config-set --help`.
   - Run `necroflow init` and verify the packaged canonical example works.

2. [ ] Add a formatting job, run once rather than in every test-matrix job.
   - Run `black --check src tests examples` with Python 3.15.

3. [x] Resolve lower-version support coverage.
   - CI covers Python 3.10 through 3.15, matching `requires-python = ">=3.10"`.

4. [ ] Add a coverage regression threshold.
   - Start with `--cov-fail-under=80`; current coverage is approximately 84%.

5. [ ] Add GitHub Actions workflow linting.
   - Run `actionlint` against `.github/workflows/*.yml`.

6. [ ] Add dependency and security maintenance.
   - Configure Dependabot for Python dependencies and GitHub Actions.
   - Evaluate adding CodeQL scanning as a lower-priority follow-up.

7. [ ] Add and document a tag-based GitHub release flow.
   - Keep the package version in `src/necroflow/__init__.py` aligned with release tags.
   - Run tests and `make check-dist` before tagging.
   - Create version tags with `make tag-release TAG=vX.Y.Z` and push them with `git push origin vX.Y.Z`.
   - Configure GitHub Actions to run the release checks for tags matching `v*`.
   - Publish the tag as a GitHub Release with generated notes, for example `gh release create vX.Y.Z --verify-tag --generate-notes --title "vX.Y.Z"`.
   - Consider attaching the checked source distribution and wheel to the GitHub Release and using trusted publishing for PyPI.

## Pipeline sections (author-declared grouping in pipeline functions)

Motivated by `necroflow graph --png` (see `src/necroflow/graphviz_render.py`):
it currently clusters nodes by dependency depth only, since necroflow has no
concept of a named "stage" — the alternative (a hardcoded rule-name→stage map,
prototyped for one downstream project's `sage_pipeline`) doesn't generalize
across pipelines. A first-class way for a pipeline function to declare its own
section boundaries would let rendering (and possibly other tooling —
`explain`, `provenance`) use author intent instead of a structural guess.

1. [ ] Design a `Pipeline.section(name)` context manager (or similar) usable
   inside a factory function:
   ```python
   def sage_pipeline(cfg, R):
       P = Pipeline()
       with P.section("MS1 Scale Calibration"):
           P.scale_estimates = R.fit_ms1_scale_estimates(...)
       ...
   ```
   - Decide storage: tag each `Node` with `section` at assignment time
     (mirrors how `pipeline_label` is already stamped in `Pipeline.__setattr__`).
   - Decide nesting rules (allowed vs. flattened) and what happens to nodes
     assigned outside any `section(...)` block (no section / implicit "misc").
2. [ ] Expose `section` in the JSON graph payload (`_node_json` in `cli.py`)
   so external tooling can consume it without depending on the ASCII/PNG
   renderers.
3. [ ] Teach `graphviz_render.py` to cluster by `section` when every node
   carries one, falling back to depth-based clustering otherwise (keeps the
   generic renderer generic for pipelines that don't opt in).
4. [ ] Consider whether the ASCII renderer (`pipeline.py`'s `_GraphBase.__str__`)
   should also use sections to place same-section nodes adjacently, reducing
   the long cross-diagram edges noted for high-fan-out root nodes
   (e.g. a raw input consumed directly by several distant rules).
