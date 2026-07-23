"""Count source lines without external dependencies."""

from __future__ import annotations

import argparse
from pathlib import Path

_TEXT_SUFFIXES = {".md", ".py", ".toml", ".txt"}


def _is_generated(path: Path) -> bool:
    return any(
        part.startswith(".") or part == "__pycache__" or part.endswith(".egg-info")
        for part in path.parts
    )


def count_lines(root: Path) -> tuple[int, int, int]:
    """Return Python code, physical Python, and total text lines below *root*."""
    python_code = 0
    python_physical = 0
    total = 0

    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if not path.is_file() or _is_generated(relative):
            continue
        if path.suffix not in _TEXT_SUFFIXES:
            continue

        lines = path.read_text(encoding="utf-8").splitlines()
        total += len(lines)
        if path.suffix == ".py":
            python_physical += len(lines)
            python_code += sum(
                bool(line.strip()) and not line.lstrip().startswith("#")
                for line in lines
            )

    return python_code, python_physical, total


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Count lines in a source tree.")
    parser.add_argument("path", nargs="?", type=Path, default=Path("src"))
    args = parser.parse_args(argv)
    if not args.path.is_dir():
        parser.error(f"not a directory: {args.path}")

    python_code, python_physical, total = count_lines(args.path)
    print(f"Python code:          {python_code}")
    print(f"Python physical:      {python_physical}")
    print(f"All supported text:   {total}")


if __name__ == "__main__":
    main()
