"""The installed usage guide must teach a working, cacheable pipeline."""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib

from necroflow.cli import main


def test_guide_example_runs_and_reuses_cache(tmp_path, monkeypatch, capsys):
    """The printed skill's exact example and commands work outside the repo."""
    monkeypatch.chdir(tmp_path)
    main(["guide"])
    guide = capsys.readouterr().out
    assert guide.startswith("---\nname: use-necroflow\n")
    assert not list(tmp_path.iterdir())
    for language, filename in (("python", "pipeline.py"), ("toml", "job.toml")):
        example = re.search(rf"```{language}\n(.*?)```", guide, re.DOTALL)
        assert example is not None
        (tmp_path / filename).write_text(example.group(1))

    commands = re.search(r"```bash\n(.*?)```", guide, re.DOTALL).group(1)
    env = os.environ.copy()
    source = str(Path(__file__).resolve().parents[1] / "src")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [source, env.get("PYTHONPATH")]))
    launcher = (
        f'necroflow() {{ {shlex.quote(sys.executable)} -m necroflow.cli "$@"; }}\n'
    )
    runs = 0
    for command in commands.splitlines():
        result = subprocess.run(
            ["/bin/sh", "-c", launcher + command],
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )
        if command.startswith("necroflow doctor"):
            assert json.loads(result.stdout)["ok"] is True
        elif command.startswith("necroflow explain"):
            calls = json.loads(result.stdout)["calls"]
            assert len(calls) == 2
            assert all(call["will_run"] is True for call in calls)
            assert not list((tmp_path / "nodes").rglob("uppercase.txt"))
        elif command.startswith("necroflow run"):
            summary = tomllib.loads(
                (tmp_path / "results/job/execution.toml").read_text()
            )
            assert len(summary["rules"]) == 2
            assert all(rule["cached"] is bool(runs) for rule in summary["rules"])
            runs += 1
        elif command.startswith("cat "):
            assert result.stdout == "HELLO NECROFLOW\n"
    assert runs == 2
    manifest = tomllib.loads((tmp_path / "results/job/manifest.toml").read_text())
    assert set(manifest["outputs"]) == {"uppercase"}
