EXPERIMENT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PYTHON ?= $(EXPERIMENT)../../.venv/bin/python
READ_PAIRS ?= 1000

.PHONY: download preprocess setup necroflow nextflow compare test integration

download:
	cd "$(EXPERIMENT)" && "$(PYTHON)" prepare.py download

preprocess: download
	cd "$(EXPERIMENT)" && "$(PYTHON)" prepare.py preprocess --read-pairs $(READ_PAIRS)

setup:
	cd "$(EXPERIMENT)" && "$(PYTHON)" prepare.py setup

necroflow: preprocess setup
	cd "$(EXPERIMENT)" && "$(PYTHON)" run.py

nextflow: preprocess setup
	cd "$(EXPERIMENT)" && "$(PYTHON)" prepare.py nextflow

compare: necroflow nextflow
	cd "$(EXPERIMENT)" && "$(PYTHON)" compare.py

test:
	cd "$(EXPERIMENT)" && "$(PYTHON)" -m pytest -q --no-cov tests

integration: compare
	cd "$(EXPERIMENT)" && "$(PYTHON)" integration.py
