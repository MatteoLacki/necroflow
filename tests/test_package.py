from importlib import resources

import necroflow


def test_version_is_exposed():
    assert isinstance(necroflow.__version__, str)
    assert necroflow.__version__


def test_canonical_template_is_packaged():
    template = resources.files("necroflow") / "templates" / "canonical"
    assert (template / "pipeline.py").is_file()
    assert (template / "job.toml").is_file()
