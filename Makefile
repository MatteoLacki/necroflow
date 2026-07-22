EXAMPLE_OUTDIR = examples/output
PYTHON ?= .venv/bin/python
PYTEST ?= .venv/bin/pytest
PYRIGHT ?= .venv/bin/pyright
TWINE ?= .venv/bin/twine
DIST_FILES = dist/*
SAGE_RECAL_DIR = examples/sage_recal
SAGE_VERSION ?= v0.14.7
SAGE_SHA256 ?= e3dc6b41015cb167574f6c82525b75e946c094f30bd700271b05c051c30cbe8a
SAGE_IMAGE = necroflow-sage-recal:$(SAGE_VERSION)
SAGE_IMAGE_ARCHIVE = dist/necroflow-sage-recal-$(SAGE_VERSION).tar.gz
VERSION_GOALS = release_pypi release_test_pypi bump-version tag-release
POSITIONAL_VERSION = $(if $(filter $(VERSION_GOALS),$(MAKECMDGOALS)),$(firstword $(filter-out $(VERSION_GOALS),$(MAKECMDGOALS))))
REQUESTED_VERSION = $(if $(VERSION),$(VERSION),$(POSITIONAL_VERSION))
CURRENT_VERSION = $(shell PYTHONPATH=src $(PYTHON) -c 'import necroflow; print(necroflow.__version__)')
RELEASE_VERSION = $(if $(REQUESTED_VERSION),$(REQUESTED_VERSION),$(CURRENT_VERSION))
TAG ?= v$(RELEASE_VERSION)

.PHONY: all venv test typecheck example clean-example clean-dist bump-version maybe-bump-version build check-dist upload_test_pypi upload_pypi release_test_pypi release_pypi tag-release sage-image sage-image-archive

all: venv

venv: .venv/bin/pytest

.venv/bin/pytest:
	uv venv .venv --python 3.14
	uv pip install --python .venv/bin/python -e ".[dev]"

$(PYRIGHT): pyproject.toml
	uv venv --allow-existing .venv --python 3.14
	uv pip install --python .venv/bin/python -e ".[dev]"

typecheck: $(PYRIGHT)
	$(PYRIGHT) --pythonversion 3.10 tests/typing/rule_dsl.py examples/canonical/pipeline.py examples/callable_fingerprint/pipeline.py examples/callable_fingerprint/fingerprint.py examples/sage_recal/pipeline.py

example:
	$(PYTHON) -m necroflow.cli --outdir $(EXAMPLE_OUTDIR) examples/necroalchemy_job.toml

clean-example:
	rm -rf $(EXAMPLE_OUTDIR)

# Builds the examples/sage_recal Docker image, pinned to a checksum-verified Sage
# release binary (see examples/sage_recal/Dockerfile, README.md's "Provenance" section).
# Bumping SAGE_VERSION without also updating SAGE_SHA256 fails the build closed (the
# checksum check in the Dockerfile won't match) rather than silently trusting an
# unverified binary.
sage-image:
	docker build \
		--build-arg SAGE_VERSION=$(SAGE_VERSION) \
		--build-arg SAGE_SHA256=$(SAGE_SHA256) \
		-t $(SAGE_IMAGE) \
		$(SAGE_RECAL_DIR)

# Archives the built image for Zenodo, the same way necroflowpaper/submission.mk
# archives the source tarball (checksum alongside, ready to attach as a deposition
# file) -- durable against the upstream GitHub release asset later disappearing.
sage-image-archive: sage-image
	mkdir -p dist
	docker save $(SAGE_IMAGE) | gzip > $(SAGE_IMAGE_ARCHIVE)
	sha256sum $(SAGE_IMAGE_ARCHIVE) > $(SAGE_IMAGE_ARCHIVE).sha256

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
