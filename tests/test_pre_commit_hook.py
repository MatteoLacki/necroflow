"""Integration tests for the repository pre-commit hook."""

import os
from pathlib import Path
import shutil
import stat
import subprocess

HOOK = Path(__file__).parents[1] / ".githooks" / "pre-commit"


def _clean_git_env() -> dict[str, str]:
    """Return the environment with GIT_* repo-discovery overrides stripped.

    Git exports GIT_DIR/GIT_INDEX_FILE (and sometimes GIT_WORK_TREE) into a
    hook's process when it invokes one during a real commit. Those leak into
    this pytest process and, unless stripped here, into every git subprocess
    these tests spawn — silently redirecting `git init`/`git add`/the hook's
    own `git diff --cached` from the throwaway repo below onto the real repo
    that is mid-commit. Without this, these tests only pass when run outside
    a git hook and fail (nondeterministically, depending on real staged
    state) when the pre-commit hook's own `pytest -q` runs them.
    """
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _hook_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, env=_clean_git_env())
    hook = repo / ".githooks" / "pre-commit"
    hook.parent.mkdir()
    shutil.copy2(HOOK, hook)
    bin_dir = repo / ".venv" / "bin"
    bin_dir.mkdir(parents=True)
    calls = repo / "calls.txt"
    _write_executable(
        bin_dir / "black",
        f"#!/usr/bin/env bash\nprintf 'black %s\\n' \"$*\" >> {calls!s}\n",
    )
    _write_executable(
        bin_dir / "pytest",
        f"#!/usr/bin/env bash\nprintf 'pytest %s\\n' \"$*\" >> {calls!s}\n",
    )
    return repo, calls


def test_pre_commit_runs_checks_only_for_staged_python_changes(tmp_path):
    """Documentation-only commits skip checks; Python changes run both tools."""

    repo, calls = _hook_repo(tmp_path)
    env = _clean_git_env()
    readme = repo / "README.md"
    readme.write_text("documentation\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True, env=env)

    docs_only = subprocess.run(
        [".githooks/pre-commit"],
        cwd=repo,
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )

    assert "no staged Python changes" in docs_only.stdout
    assert not calls.exists()

    module = repo / "module.py"
    module.write_text("value = 1\n")
    subprocess.run(["git", "add", "module.py"], cwd=repo, check=True, env=env)

    subprocess.run([".githooks/pre-commit"], cwd=repo, check=True, env=env)

    assert calls.read_text().splitlines() == ["black module.py", "pytest -q"]
