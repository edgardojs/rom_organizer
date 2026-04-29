"""Unit tests for the ROM Organizer archiver module."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
import zipfile
from pathlib import Path

from archiver import (
    ArchiveInspection,
    ArchiveEntry,
    compute_archive_fingerprint,
    inspect_archive,
    inspect_zip,
)


class TestInspectZip(unittest.TestCase):
    """Tests for inspect_zip()."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()

    def test_inspect_valid_zip(self) -> None:
        zip_path = Path(self.tmpdir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("game.nes", b"NES ROM DATA")
            zf.writestr("readme.txt", "Hello World")

        result = inspect_zip(zip_path)
        self.assertIsNone(result.error)
        self.assertEqual(result.archive_type, "zip")
        self.assertEqual(len(result.entries), 2)
        self.assertEqual(result.total_uncompressed_size, len(b"NES ROM DATA") + len("Hello World"))

    def test_inspect_empty_zip(self) -> None:
        zip_path = Path(self.tmpdir) / "empty.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            pass  # Empty zip.

        result = inspect_zip(zip_path)
        self.assertIsNone(result.error)
        self.assertEqual(len(result.entries), 0)

    def test_inspect_corrupted_zip(self) -> None:
        bad_path = Path(self.tmpdir) / "bad.zip"
        bad_path.write_bytes(b"not a zip file at all")

        result = inspect_zip(bad_path)
        self.assertIsNotNone(result.error)

    def test_inspect_zip_hashes_entries(self) -> None:
        zip_path = Path(self.tmpdir) / "hashed.zip"
        content = b"NES ROM DATA"
        expected_hash = hashlib.sha256(content).hexdigest()

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("game.nes", content)

        result = inspect_zip(zip_path)
        self.assertEqual(len(result.entries), 1)
        self.assertEqual(result.entries[0].sha256, expected_hash)

    def test_inspect_nonexistent_zip(self) -> None:
        result = inspect_zip(Path("/nonexistent/path.zip"))
        self.assertIsNotNone(result.error)


class TestComputeArchiveFingerprint(unittest.TestCase):
    """Tests for compute_archive_fingerprint()."""

    def test_same_contents_same_fingerprint(self) -> None:
        entries1 = [
            ArchiveEntry(name="a.nes", size=100, sha256="hash_a"),
            ArchiveEntry(name="b.nes", size=200, sha256="hash_b"),
        ]
        entries2 = [
            ArchiveEntry(name="b.nes", size=200, sha256="hash_b"),
            ArchiveEntry(name="a.nes", size=100, sha256="hash_a"),
        ]
        insp1 = ArchiveInspection(archive_path="/test1.zip", archive_type="zip", entries=entries1)
        insp2 = ArchiveInspection(archive_path="/test2.zip", archive_type="zip", entries=entries2)

        # Fingerprints should be the same regardless of entry order.
        self.assertEqual(
            compute_archive_fingerprint(insp1),
            compute_archive_fingerprint(insp2),
        )

    def test_different_contents_different_fingerprint(self) -> None:
        entries1 = [ArchiveEntry(name="a.nes", size=100, sha256="hash_a")]
        entries2 = [ArchiveEntry(name="a.nes", size=100, sha256="hash_b")]
        insp1 = ArchiveInspection(archive_path="/test1.zip", archive_type="zip", entries=entries1)
        insp2 = ArchiveInspection(archive_path="/test2.zip", archive_type="zip", entries=entries2)

        self.assertNotEqual(
            compute_archive_fingerprint(insp1),
            compute_archive_fingerprint(insp2),
        )


class TestInspectArchive(unittest.TestCase):
    """Tests for inspect_archive() dispatch."""

    def test_dispatches_zip(self) -> None:
        tmpdir = tempfile.mkdtemp()
        zip_path = Path(tmpdir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("game.nes", b"data")

        result = inspect_archive(zip_path)
        self.assertEqual(result.archive_type, "zip")

    def test_unsupported_extension(self) -> None:
        result = inspect_archive(Path("/test.rar"))
        self.assertEqual(result.archive_type, "unknown")
        self.assertIsNotNone(result.error)


if __name__ == "__main__":
    unittest.main()