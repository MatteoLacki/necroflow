EXAMPLE_OUTDIR = examples/output

make:
	echo "Welcome to Project necroflow"

venv:
	uv venv .venv --python 3.14
	uv pip install --python .venv/bin/python -e ".[dev]"

example:
	.venv/bin/necroflow --outdir $(EXAMPLE_OUTDIR) examples/necroalchemy_job.toml

clean-example:
	rm -rf $(EXAMPLE_OUTDIR)

upload_test_pypi:
	twine check dist/*
	python -m pip install --upgrade twine
	twine upload --repository testpypi dist/*

upload_pypi:
	twine check dist/*
	python -m pip install --upgrade twine
	twine upload dist/*

ve_necroflow:
	python3 -m venv ve_necroflow
