"""Unit tests for the ROM Organizer hasher module."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from hasher import hash_file


class TestHashFile(unittest.TestCase):
    """Tests for hash_file()."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()

    def test_hash_empty_file(self) -> None:
        path = Path(self.tmpdir) / "empty.nes"
        path.write_bytes(b"")
        result = hash_file(path)
        expected = hashlib.sha256(b"").hexdigest()
        self.assertEqual(result, expected)

    def test_hash_known_content(self) -> None:
        path = Path(self.tmpdir) / "known.nes"
        content = b"Hello, ROM Organizer!"
        path.write_bytes(content)
        result = hash_file(path)
        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_hash_with_different_algorithm(self) -> None:
        path = Path(self.tmpdir) / "known.nes"
        content = b"Test content"
        path.write_bytes(content)
        result = hash_file(path, algorithm="sha1")
        expected = hashlib.sha1(content).hexdigest()
        self.assertEqual(result, expected)

    def test_hash_unsupported_algorithm(self) -> None:
        path = Path(self.tmpdir) / "known.nes"
        path.write_bytes(b"test")
        with self.assertRaises(ValueError):
            hash_file(path, algorithm="notarealalgo")

    def test_hash_nonexistent_file(self) -> None:
        path = Path(self.tmpdir) / "nonexistent.nes"
        with self.assertRaises(FileNotFoundError):
            hash_file(path)

    def test_hash_large_file_in_chunks(self) -> None:
        """Verify chunked hashing produces the same result as hashing all at once."""
        path = Path(self.tmpdir) / "large.nes"
        content = b"A" * (2 * 1024 * 1024)  # 2 MiB
        path.write_bytes(content)
        result = hash_file(path, chunk_size=65536)
        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()