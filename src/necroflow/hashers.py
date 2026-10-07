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
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from necroflow.config import load_callable
from necroflow.fs import _output_mtime

if TYPE_CHECKING:
    from necroflow.nodes import Node

DEFAULT_HASHER = "blake3"

_HASH_CHUNK_SIZE = 1024 * 1024
# Large enough that BLAKE3 splits each chunk across its threads, small enough
# that hashing never holds more than one chunk however big the output is.
_PARALLEL_CHUNK_SIZE = 64 * 1024 * 1024
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
    """BLAKE3 over the output bytes, spreading each chunk across `threads`."""

    name = "blake3"

    def hash_path(self, path: Path, threads: int) -> str:
        import blake3

        digest = blake3.blake3(max_threads=max(1, threads))
        buffer = bytearray(_PARALLEL_CHUNK_SIZE)
        view = memoryview(buffer)
        for relative, file in _output_files(path):
            if relative:
                digest.update(relative.encode())
            # Chunked reads, not mmap: a mapped file's pages count towards
            # this process's memory as they are read, which for a 140 GB
            # output column exhausts RAM.
            with file.open("rb", buffering=0) as handle:
                while size := handle.readinto(buffer):
                    digest.update(view[:size])
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


class TimedHasher:
    """Delegate to `hasher`, accumulating wall seconds spent in `hash_path`.

    The total equals hashing wall time only while hashes run one at a time,
    as they do on the executor's coordinator thread and in CLI result
    materialization.
    """

    def __init__(self, hasher: Hasher) -> None:
        self.hasher = hasher
        self.name = hasher.name
        self.seconds = 0.0

    def hash_path(self, path: Path, threads: int) -> str:
        started = time.perf_counter()
        try:
            return self.hasher.hash_path(path, threads)
        finally:
            self.seconds += time.perf_counter() - started


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


class OutputHashes:
    """Current tagged content hashes of Node outputs during one invocation.

    A stored `.rip/{filename}.hash` is trusted while the output is no newer
    than it and `hasher` made it; otherwise the output is read in full.
    Without `may_hash`, an untrusted stored hash gives None instead of a full
    read. With `store`, a fresh digest replaces the stored one so later runs
    trust it again, unless the output changed while it was being hashed. Only
    node-store lock holders may store.
    """

    def __init__(self, hasher: Hasher, *, may_hash: bool = True, store: bool = False):
        self.hasher = hasher
        self.may_hash = may_hash
        self.store = store
        self._digests: dict[Path, str] = {}

    def current(self, node: Node, threads: int) -> str | None:
        """Return the node's current digest, trusting its stored hash when safe."""
        known = self._digests.get(node.relative_path)
        if known is not None:
            return known
        mtime = _output_mtime(node.path)
        hash_file = node.hash_file
        if hash_file.exists() and mtime <= hash_file.stat().st_mtime_ns:
            stored = hash_file.read_text().strip()
            if is_tagged_by(stored, self.hasher):
                self._digests[node.relative_path] = stored
                return stored
        if not self.may_hash:
            return None
        digest = tagged_hash(self.hasher, node.path, threads)
        if self.store and _output_mtime(node.path) == mtime:
            hash_file.write_text(digest)
        self._digests[node.relative_path] = digest
        return digest

    def record(self, node: Node, threads: int) -> str:
        """Hash a freshly produced output and store its digest."""
        digest = tagged_hash(self.hasher, node.path, threads)
        node.hash_file.write_text(digest)
        self._digests[node.relative_path] = digest
        return digest
