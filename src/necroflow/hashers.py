"""Content hashers for node outputs.

A hasher turns an output path (file or directory) into a hex digest. Stored
digests are tagged with the hasher's name (`blake3:<hex>`), so a record made
by one hasher is never compared as if it came from another: it simply stops
counting as a valid consumed hash, and the consumer reruns.

Built-ins are `blake3` (default) and `sha256`. A user hasher is loaded from
`path.py:ClassName`; the class is instantiated with no arguments and must
provide a `name` attribute and a `hash_path(path, threads)` method.
"""

from __future__ import annotations

import hashlib
import mmap
import re
from pathlib import Path
from typing import Protocol, runtime_checkable

from necroflow.config import load_callable

DEFAULT_HASHER = "blake3"

_HASH_CHUNK_SIZE = 1024 * 1024
_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_.-]*")


@runtime_checkable
class Hasher(Protocol):
    """Hash one output path using up to `threads` threads."""

    name: str

    def hash_path(self, path: Path, threads: int) -> str: ...


def _output_files(path: Path) -> list[tuple[str, Path]]:
    """Files contributing to `path`'s digest, with their names, in order.

    A file contributes only its bytes. A directory contributes every non-.rip
    file below it, sorted by path, each preceded by its relative path so that
    renames change the digest.
    """
    if path.is_file():
        return [("", path)]
    return [
        (str(file.relative_to(path)), file)
        for file in sorted(path.rglob("*"))
        if file.is_file() and ".rip" not in file.parts
    ]


class Sha256Hasher:
    """SHA-256 over the output bytes; single-threaded."""

    name = "sha256"

    def hash_path(self, path: Path, threads: int) -> str:
        digest = hashlib.sha256()
        for relative, file in _output_files(path):
            if relative:
                digest.update(relative.encode())
            with file.open("rb") as handle:
                while chunk := handle.read(_HASH_CHUNK_SIZE):
                    digest.update(chunk)
        return digest.hexdigest()


class Blake3Hasher:
    """BLAKE3 over the output bytes, spreading each file across `threads`."""

    name = "blake3"

    def hash_path(self, path: Path, threads: int) -> str:
        import blake3

        digest = blake3.blake3(max_threads=max(1, threads))
        for relative, file in _output_files(path):
            if relative:
                digest.update(relative.encode())
            with file.open("rb") as handle:
                if file.stat().st_size == 0:
                    continue
                # One whole-file buffer is what lets BLAKE3 split the work
                # across threads; mmap makes that buffer free.
                with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as view:
                    digest.update(view)
        return digest.hexdigest()


_BUILTIN_HASHERS = {"blake3": Blake3Hasher, "sha256": Sha256Hasher}


def load_hasher(spec: str | Hasher | None) -> Hasher:
    """Resolve a hasher from a built-in name, a `path.py:Class` spec, or an
    already-constructed hasher. `None` means the default."""
    if spec is None:
        spec = DEFAULT_HASHER
    if not isinstance(spec, str):
        hasher = spec
    elif spec in _BUILTIN_HASHERS:
        hasher = _BUILTIN_HASHERS[spec]()
    else:
        hasher = load_callable(spec, kind="hasher")()
    if not isinstance(hasher, Hasher):
        raise TypeError(
            f"hasher {spec!r} must provide a `name` and `hash_path(path, threads)`"
        )
    if not isinstance(hasher.name, str) or not _NAME_PATTERN.fullmatch(hasher.name):
        raise ValueError(
            f"hasher name must match {_NAME_PATTERN.pattern!r}, got {hasher.name!r}"
        )
    return hasher


def tagged_hash(hasher: Hasher, path: Path, threads: int) -> str:
    """`<hasher name>:<hex digest>` of `path`."""
    return f"{hasher.name}:{hasher.hash_path(path, threads)}"


def is_tagged_by(value: object, hasher: Hasher) -> bool:
    """Whether `value` is a well-formed digest produced by `hasher`."""
    if not isinstance(value, str):
        return False
    name, separator, digest = value.partition(":")
    return (
        separator == ":"
        and name == hasher.name
        and len(digest) > 0
        and all(char in "0123456789abcdef" for char in digest)
    )
