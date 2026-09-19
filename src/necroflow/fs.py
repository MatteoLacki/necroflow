"""Filesystem limits and output metadata."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path

def _normalize_shellpath(shellpath: str | Path | None) -> str | None:
    """Resolve and validate an optional executable shell path."""
    if shellpath is None:
        return None
    path = Path(shellpath).expanduser()
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError(f"shellpath does not exist: {path}") from exc
    if not resolved.is_file():
        raise ValueError(f"shellpath is not a file: {resolved}")
    if not os.access(resolved, os.X_OK):
        raise ValueError(f"shellpath is not executable: {resolved}")
    return str(resolved)


@contextmanager
def _acquire_lock(outdir: Path):
    """Hold the exclusive per-node-store lock for one filesystem operation."""
    lock_path = outdir / ".rip" / "necroflow.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(
                f"Another necroflow instance is already running against {outdir}.\n"
                "Only one instance per node store is supported."
            ) from exc
        yield


def _filesystem_limits(path: Path) -> tuple[int | None, int | None]:
    """Return (NAME_MAX, PATH_MAX) for nearest existing parent of path."""
    for candidate in (path, *path.parents):
        if not candidate.exists():
            continue
        try:
            name_max = os.pathconf(candidate, "PC_NAME_MAX")
        except (OSError, ValueError):
            name_max = None
        try:
            path_max = os.pathconf(candidate, "PC_PATH_MAX")
        except (OSError, ValueError):
            path_max = None
        return name_max, path_max
    return None, None


def _check_path_limits(path: Path) -> None:
    name_max, path_max = _filesystem_limits(path)
    if name_max is not None:
        for part in path.parts:
            if part in (path.anchor, os.sep, ""):
                continue
            length = len(os.fsencode(part))
            # not len(part): fs limits NAME_MAX in bytes, not Unicode chars.
            if length > name_max:
                raise ValueError(
                    f"path component too long ({length} > NAME_MAX {name_max}): {part!r}"
                )
    if path_max is not None:
        length = len(os.fsencode(os.fspath(path)))
        if length > path_max:
            raise ValueError(f"path too long ({length} > PATH_MAX {path_max}): {path}")


def _output_mtime(path: Path) -> int:
    """Newest output mtime; directory entries detect rename and deletion."""
    if path.is_dir():
        entries = [path]
        entries.extend(
            entry
            for entry in path.rglob("*")
            if ".rip" not in entry.relative_to(path).parts
        )
        return max(entry.stat().st_mtime_ns for entry in entries)
    return path.stat().st_mtime_ns
