"""The migration guide's current examples must run against the installed API."""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib


def test_migration_examples_execute_and_cache(tmp_path):
    """Migrated examples preserve requests, resource policy, content, and caching.

    Execute the current Python/TOML/shell examples verbatim, including the
    direct-Python entry point; historical reference code is intentionally skipped.
    """
    repo = Path(__file__).resolve().parents[1]
    guide = (repo / "skills/update.md").read_text()
    for filename in ("pipeline.py", "run_pipeline.py"):
        block = re.search(
            rf"```python\n# {re.escape(filename)}\n(.*?)```", guide, re.DOTALL
        )
        assert block is not None
        (tmp_path / filename).write_text(block.group(1))
    job = re.search(r"```toml\n(.*?)```", guide, re.DOTALL)
    assert job is not None
    (tmp_path / "job.toml").write_text(job.group(1))
    commands = re.search(r"```bash\n(.*?)```", guide, re.DOTALL)
    assert commands is not None

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(repo / "src"), env.get("PYTHONPATH")])
    )
    launcher = (
        f'necroflow() {{ {shlex.quote(sys.executable)} -m necroflow.cli "$@"; }}\n'
    )
    runs = 0
    for command in commands.group(1).splitlines():
        result = subprocess.run(
            ["/bin/sh", "-c", launcher + command],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        if "--json" in command:
            payload = json.loads(result.stdout)
            if command.startswith("necroflow doctor"):
                assert payload["ok"] is True
            elif command.startswith("necroflow explain"):
                calls = {call["rule"]: call for call in payload["calls"]}
                assert set(calls) == {"symlink_Source", "convert"}
                assert calls["convert"]["resources"]["threads"] == 4
                assert all(call["will_run"] is True for call in calls.values())
                assert not list((tmp_path / "nodes").rglob("result.txt"))
        elif command.startswith("necroflow run"):
            summary = tomllib.loads(
                (tmp_path / "results/job/execution.toml").read_text()
            )
            assert {rule["name"] for rule in summary["rules"]} == {
                "symlink_Source",
                "convert",
            }
            assert all(rule["cached"] is bool(runs) for rule in summary["rules"])
            runs += 1
        elif command.startswith("cat "):
            assert result.stdout == "migration example\n"
    assert runs == 2
    manifest = tomllib.loads((tmp_path / "results/job/manifest.toml").read_text())
    assert set(manifest["outputs"]) == {"result"}
    assert manifest["outputs"]["result"]["path"] == "result/result.txt"

    subprocess.run(
        [sys.executable, "run_pipeline.py"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    direct_results = list((tmp_path / "python-nodes/convert").rglob("result.txt"))
    assert len(direct_results) == 1
    assert direct_results[0].read_text() == "migration example\n"
