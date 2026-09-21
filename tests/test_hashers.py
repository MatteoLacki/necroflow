"""Tests for output content hashers."""

import hashlib
from pathlib import Path

import blake3
import pytest

import necroflow.hashers as hashers
from necroflow.hashers import (
    Blake3Hasher,
    Sha256Hasher,
    is_tagged_by,
    load_hasher,
    tagged_hash,
)


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


def test_sha256_streams_file_in_bounded_chunks(tmp_path, monkeypatch):
    """Hashing a large output must never request the complete file at once."""
    path = tmp_path / "large.bin"
    content = b"x" * (hashers._HASH_CHUNK_SIZE * 2 + 17)
    path.write_bytes(content)
    reads = _record_hash_reads(monkeypatch)

    digest = Sha256Hasher().hash_path(path, threads=1)

    assert digest == hashlib.sha256(content).hexdigest()
    assert {read_path for read_path, _size in reads} == {path}
    assert all(size == hashers._HASH_CHUNK_SIZE for _path, size in reads)


def test_sha256_streams_directory_files_in_bounded_chunks(tmp_path, monkeypatch):
    """Directory hashing must stream every included file and continue ignoring .rip."""
    root = tmp_path / "tree"
    nested = root / "nested"
    rip = root / ".rip"
    nested.mkdir(parents=True)
    rip.mkdir()
    first_content = b"a" * (hashers._HASH_CHUNK_SIZE + 3)
    second_content = b"b" * (hashers._HASH_CHUNK_SIZE * 2 + 5)
    first = root / "a.bin"
    second = nested / "b.bin"
    ignored = rip / "metadata"
    first.write_bytes(first_content)
    second.write_bytes(second_content)
    ignored.write_bytes(b"ignored")
    reads = _record_hash_reads(monkeypatch)

    digest = Sha256Hasher().hash_path(root, threads=1)

    expected = hashlib.sha256()
    expected.update(b"a.bin")
    expected.update(first_content)
    expected.update(b"nested/b.bin")
    expected.update(second_content)
    assert digest == expected.hexdigest()
    assert ignored not in {read_path for read_path, _size in reads}
    assert all(size == hashers._HASH_CHUNK_SIZE for _path, size in reads)


def _tree(tmp_path):
    root = tmp_path / "tree"
    (root / "nested").mkdir(parents=True)
    (root / ".rip").mkdir()
    (root / "a.bin").write_bytes(b"a" * 1000)
    (root / "nested" / "b.bin").write_bytes(b"b" * 3000)
    (root / "empty.bin").write_bytes(b"")
    (root / ".rip" / "metadata").write_bytes(b"ignored")
    return root


def test_blake3_matches_reference_for_a_file(tmp_path):
    path = tmp_path / "data.bin"
    content = bytes(range(256)) * 5000
    path.write_bytes(content)

    assert Blake3Hasher().hash_path(path, threads=4) == blake3.blake3(content).hexdigest()


def test_blake3_directory_covers_names_and_bytes_in_sorted_order(tmp_path):
    root = _tree(tmp_path)

    expected = blake3.blake3()
    for name, content in [
        (b"a.bin", b"a" * 1000),
        (b"empty.bin", b""),
        (b"nested/b.bin", b"b" * 3000),
    ]:
        expected.update(name)
        expected.update(content)
    assert Blake3Hasher().hash_path(root, threads=2) == expected.hexdigest()


def test_blake3_digest_does_not_depend_on_thread_count(tmp_path):
    path = tmp_path / "data.bin"
    path.write_bytes(bytes(range(256)) * 20000)

    digests = {Blake3Hasher().hash_path(path, threads=n) for n in (1, 2, 8)}
    assert len(digests) == 1


def test_directory_rename_changes_digest(tmp_path):
    root = _tree(tmp_path)
    before = Blake3Hasher().hash_path(root, threads=1)
    (root / "a.bin").rename(root / "c.bin")

    assert Blake3Hasher().hash_path(root, threads=1) != before


def test_tagged_hash_names_the_hasher(tmp_path):
    path = tmp_path / "data.bin"
    path.write_bytes(b"payload")

    value = tagged_hash(load_hasher("sha256"), path, threads=1)
    assert value == "sha256:" + hashlib.sha256(b"payload").hexdigest()
    assert is_tagged_by(value, Sha256Hasher())
    assert not is_tagged_by(value, Blake3Hasher())


@pytest.mark.parametrize(
    "value",
    [None, 42, "", "sha256", "sha256:", "sha256:XYZ", "0" * 64, "blake3:" + "0" * 64],
)
def test_is_tagged_by_rejects_malformed_or_foreign_values(value):
    assert not is_tagged_by(value, Sha256Hasher())


def test_default_hasher_is_blake3():
    assert load_hasher(None).name == "blake3"


def test_load_hasher_loads_a_user_class(tmp_path):
    plugin = tmp_path / "plugin.py"
    plugin.write_text(
        "class LengthHasher:\n"
        "    name = 'length'\n"
        "    def hash_path(self, path, threads):\n"
        "        return format(path.stat().st_size, 'x')\n"
    )
    hasher = load_hasher(f"{plugin}:LengthHasher")

    data = tmp_path / "data.bin"
    data.write_bytes(b"12345")
    assert tagged_hash(hasher, data, threads=1) == "length:5"


def test_load_hasher_rejects_names_that_would_break_tags(tmp_path):
    plugin = tmp_path / "plugin.py"
    plugin.write_text(
        "class Bad:\n"
        "    name = 'has:colon'\n"
        "    def hash_path(self, path, threads):\n"
        "        return '00'\n"
    )
    with pytest.raises(ValueError, match="hasher name"):
        load_hasher(f"{plugin}:Bad")


def test_blake3_reads_large_files_in_bounded_chunks(tmp_path, monkeypatch):
    """Memory must not grow with file size: never map or read a whole file."""
    monkeypatch.setattr(hashers, "_PARALLEL_CHUNK_SIZE", 1024)
    path = tmp_path / "large.bin"
    content = bytes(range(256)) * 20  # 5 chunks
    path.write_bytes(content)
    requested = []
    real_open = Path.open

    class Recorder:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def readinto(self, buffer):
            requested.append(len(buffer))
            return self.handle.readinto(buffer)

    monkeypatch.setattr(Path, "open", lambda self, *a, **k: Recorder(real_open(self, *a, **k)))

    assert Blake3Hasher().hash_path(path, threads=4) == blake3.blake3(content).hexdigest()
    assert requested and set(requested) == {1024}
