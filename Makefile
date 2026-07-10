EXAMPLE_OUTDIR = examples/output
PYTHON ?= .venv/bin/python
PYTEST ?= .venv/bin/pytest
TWINE ?= .venv/bin/twine
DIST_FILES = dist/*
VERSION_GOALS = release_pypi release_test_pypi bump-version tag-release
POSITIONAL_VERSION = $(if $(filter $(VERSION_GOALS),$(MAKECMDGOALS)),$(firstword $(filter-out $(VERSION_GOALS),$(MAKECMDGOALS))))
REQUESTED_VERSION = $(if $(VERSION),$(VERSION),$(POSITIONAL_VERSION))
CURRENT_VERSION = $(shell PYTHONPATH=src $(PYTHON) -c 'import necroflow; print(necroflow.__version__)')
RELEASE_VERSION = $(if $(REQUESTED_VERSION),$(REQUESTED_VERSION),$(CURRENT_VERSION))
TAG ?= v$(RELEASE_VERSION)

.PHONY: all venv test example clean-example clean-dist bump-version maybe-bump-version build check-dist upload_test_pypi upload_pypi release_test_pypi release_pypi tag-release

all: venv

venv: .venv/bin/pytest

.venv/bin/pytest:
	uv venv .venv --python 3.14
	uv pip install --python .venv/bin/python -e ".[dev]"

example:
	$(PYTHON) -m necroflow.cli --outdir $(EXAMPLE_OUTDIR) examples/necroalchemy_job.toml

clean-example:
	rm -rf $(EXAMPLE_OUTDIR)

test: venv
	$(PYTEST) -q

clean-dist:
	rm -rf dist build src/necroflow.egg-info

bump-version:
	@test -n "$(REQUESTED_VERSION)" || (echo "usage: make bump-version VERSION=X.Y.Z or make bump-version X.Y.Z" >&2; exit 2)
	$(PYTHON) -c "from pathlib import Path; import re, sys; version = sys.argv[1]; assert re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+([a-zA-Z0-9.-]+)?', version), f'invalid version: {version}'; path = Path('src/necroflow/__init__.py'); text = path.read_text(); new_text, count = re.subn(r'^__version__ = \"[^\"]+\"', f'__version__ = \"{version}\"', text, count=1, flags=re.M); assert count == 1, 'missing __version__ assignment'; path.write_text(new_text)" "$(REQUESTED_VERSION)"

maybe-bump-version:
	@if test -n "$(REQUESTED_VERSION)"; then $(MAKE) bump-version VERSION="$(REQUESTED_VERSION)"; fi

build: venv clean-dist
	$(PYTHON) -m build

check-dist: build
	$(TWINE) check $(DIST_FILES)

upload_test_pypi: check-dist
	$(TWINE) upload --repository testpypi $(DIST_FILES)

upload_pypi: check-dist
	$(TWINE) upload $(DIST_FILES)

release_test_pypi: maybe-bump-version
	$(MAKE) test
	$(MAKE) check-dist
	$(TWINE) upload --repository testpypi $(DIST_FILES)

release_pypi: maybe-bump-version
	$(MAKE) test
	$(MAKE) check-dist
	$(TWINE) upload $(DIST_FILES)

tag-release:
	git diff --quiet
	git diff --cached --quiet
	git tag $(TAG)

ifneq ($(POSITIONAL_VERSION),)
.PHONY: $(POSITIONAL_VERSION)
$(POSITIONAL_VERSION):
	@:
endif

ve_necroflow:
	python3 -m venv ve_necroflow
