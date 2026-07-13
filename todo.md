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
