"""Integration tests for the repository pre-commit hook."""

from pathlib import Path
import shutil
import stat
import subprocess

HOOK = Path(__file__).parents[1] / ".githooks" / "pre-commit"


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _hook_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
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
    readme = repo / "README.md"
    readme.write_text("documentation\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)

    docs_only = subprocess.run(
        [".githooks/pre-commit"],
        cwd=repo,
        check=True,
        text=True,
        capture_output=True,
    )

    assert "no staged Python changes" in docs_only.stdout
    assert not calls.exists()

    module = repo / "module.py"
    module.write_text("value = 1\n")
    subprocess.run(["git", "add", "module.py"], cwd=repo, check=True)

    subprocess.run([".githooks/pre-commit"], cwd=repo, check=True)

    assert calls.read_text().splitlines() == ["black module.py", "pytest -q"]
