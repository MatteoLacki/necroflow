"""Tests for filesystem limits and content hashing."""

import hashlib
from pathlib import Path

import necroflow.fs as fs_core


def _record_hash_reads(monkeypatch):
    real_open = Path.open
    reads = []

    class RecordingReader:
        def __init__(self, path, *args, **kwargs):
            self.path = path
            self.file = real_open(path, *args, **kwargs)

        def __enter__(self):
            self.file.__enter__()
            return self

        def __exit__(self, *args):
            return self.file.__exit__(*args)

        def read(self, size=-1):
            reads.append((self.path, size))
            return self.file.read(size)

    def recording_open(path, *args, **kwargs):
        return RecordingReader(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", recording_open)
    return reads


def test_content_hash_streams_file_in_bounded_chunks(tmp_path, monkeypatch):
    """Hashing a large output must never request the complete file at once."""
    path = tmp_path / "large.bin"
    content = b"x" * (fs_core._HASH_CHUNK_SIZE * 2 + 17)
    path.write_bytes(content)
    reads = _record_hash_reads(monkeypatch)

    digest = fs_core._content_hash(path)

    assert digest == hashlib.sha256(content).hexdigest()
    assert {read_path for read_path, _size in reads} == {path}
    assert all(size == fs_core._HASH_CHUNK_SIZE for _path, size in reads)


def test_content_hash_streams_directory_files_in_bounded_chunks(tmp_path, monkeypatch):
    """Directory hashing must stream every included file and continue ignoring .rip."""
    root = tmp_path / "tree"
    nested = root / "nested"
    rip = root / ".rip"
    nested.mkdir(parents=True)
    rip.mkdir()
    first_content = b"a" * (fs_core._HASH_CHUNK_SIZE + 3)
    second_content = b"b" * (fs_core._HASH_CHUNK_SIZE * 2 + 5)
    first = root / "a.bin"
    second = nested / "b.bin"
    ignored = rip / "metadata"
    first.write_bytes(first_content)
    second.write_bytes(second_content)
    ignored.write_bytes(b"ignored")
    reads = _record_hash_reads(monkeypatch)

    digest = fs_core._content_hash(root)

    expected = hashlib.sha256()
    expected.update(b"a.bin")
    expected.update(first_content)
    expected.update(b"nested/b.bin")
    expected.update(second_content)
    assert digest == expected.hexdigest()
    assert ignored not in {read_path for read_path, _size in reads}
    assert all(size == fs_core._HASH_CHUNK_SIZE for _path, size in reads)
