"""Tests for the progress reporting module."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

from progress import (
    ProgressBar,
    _format_duration,
    _format_size,
    _is_terminal,
    estimate_folder,
)


class TestFormatSize(unittest.TestCase):
    """Tests for _format_size helper."""

    def test_bytes(self) -> None:
        self.assertEqual(_format_size(0), "0 B")
        self.assertEqual(_format_size(512), "512 B")

    def test_kibibytes(self) -> None:
        self.assertEqual(_format_size(1024), "1.0 KiB")
        self.assertEqual(_format_size(1536), "1.5 KiB")

    def test_mebibytes(self) -> None:
        self.assertEqual(_format_size(1024 * 1024), "1.0 MiB")
        self.assertEqual(_format_size(50 * 1024 * 1024), "50.0 MiB")

    def test_gibibytes(self) -> None:
        self.assertEqual(_format_size(1024 ** 3), "1.0 GiB")

    def test_tebibytes(self) -> None:
        self.assertEqual(_format_size(1024 ** 4), "1.0 TiB")

    def test_large(self) -> None:
        self.assertEqual(_format_size(1024 ** 5), "1.0 PiB")


class TestFormatDuration(unittest.TestCase):
    """Tests for _format_duration helper."""

    def test_seconds(self) -> None:
        self.assertEqual(_format_duration(0), "0s")
        self.assertEqual(_format_duration(30), "30s")
        self.assertEqual(_format_duration(59), "59s")

    def test_minutes(self) -> None:
        self.assertEqual(_format_duration(60), "1m 0s")
        self.assertEqual(_format_duration(135), "2m 15s")

    def test_hours(self) -> None:
        self.assertEqual(_format_duration(3600), "1h 0m")
        self.assertEqual(_format_duration(7380), "2h 3m")

    def test_negative(self) -> None:
        self.assertEqual(_format_duration(-1), "--")


class TestIsTerminal(unittest.TestCase):
    """Tests for _is_terminal helper."""

    def test_returns_bool(self) -> None:
        result = _is_terminal()
        self.assertIsInstance(result, bool)


class TestEstimateFolder(unittest.TestCase):
    """Tests for estimate_folder function."""

    def setUp(self) -> None:
        """Create a temp directory with test ROM files."""
        self.tmpdir = tempfile.mkdtemp()
        # Create some ROM files.
        for name, size in [
            ("game1.nes", 4096),
            ("game2.sfc", 8192),
            ("game3.gen", 2048),
        ]:
            p = Path(self.tmpdir) / name
            p.write_bytes(b"\x00" * size)

        # Create a file that's too small.
        (Path(self.tmpdir) / "tiny.nes").write_bytes(b"\x00" * 100)

        # Create a non-ROM file.
        (Path(self.tmpdir) / "readme.txt").write_bytes(b"\x00" * 4096)

        # Create a hidden file.
        (Path(self.tmpdir) / ".hidden.nes").write_bytes(b"\x00" * 4096)

        # Create a subdirectory with a ROM.
        subdir = Path(self.tmpdir) / "subdir"
        subdir.mkdir()
        (subdir / "game4.gba").write_bytes(b"\x00" * 16384)

    def tearDown(self) -> None:
        """Clean up temp directory."""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_counts_matching_files(self) -> None:
        extensions = {".nes", ".sfc", ".gen", ".gba"}
        est = estimate_folder(
            Path(self.tmpdir), extensions, set(), min_file_size=1024
        )
        # game1.nes, game2.sfc, game3.gen, game4.gba = 4 files
        self.assertEqual(est.total_files, 4)

    def test_total_bytes(self) -> None:
        extensions = {".nes", ".sfc", ".gen", ".gba"}
        est = estimate_folder(
            Path(self.tmpdir), extensions, set(), min_file_size=1024
        )
        expected = 4096 + 8192 + 2048 + 16384
        self.assertEqual(est.total_bytes, expected)

    def test_skips_small_files(self) -> None:
        extensions = {".nes", ".sfc", ".gen", ".gba"}
        est = estimate_folder(
            Path(self.tmpdir), extensions, set(), min_file_size=1024
        )
        # tiny.nes is below min_file_size
        self.assertEqual(est.skipped_files, 1)

    def test_excludes_dirs(self) -> None:
        extensions = {".nes", ".sfc", ".gen", ".gba"}
        est = estimate_folder(
            Path(self.tmpdir), extensions, {"subdir"}, min_file_size=1024
        )
        # game4.gba is in subdir which is excluded
        self.assertEqual(est.total_files, 3)

    def test_human_size(self) -> None:
        extensions = {".nes", ".sfc", ".gen", ".gba"}
        est = estimate_folder(
            Path(self.tmpdir), extensions, set(), min_file_size=1024
        )
        self.assertIn("KiB", est.human_size)

    def test_elapsed_positive(self) -> None:
        extensions = {".nes", ".sfc", ".gen", ".gba"}
        est = estimate_folder(
            Path(self.tmpdir), extensions, set(), min_file_size=1024
        )
        self.assertGreaterEqual(est.elapsed, 0)

    def test_nonexistent_dir(self) -> None:
        est = estimate_folder(
            Path("/nonexistent/path"), {".nes"}, set()
        )
        self.assertEqual(est.total_files, 0)
        self.assertEqual(est.total_bytes, 0)


class TestProgressBar(unittest.TestCase):
    """Tests for ProgressBar."""

    def test_basic_usage(self) -> None:
        """Test that ProgressBar can be created, updated, and closed."""
        bar = ProgressBar(total=100, label="Test", unit="items")
        self.assertEqual(bar.current, 0)
        bar.update(10)
        self.assertEqual(bar.current, 10)
        bar.update(90)
        self.assertEqual(bar.current, 100)
        bar.close()

    def test_set_current(self) -> None:
        """Test set_current method."""
        bar = ProgressBar(total=100, label="Test")
        bar.set_current(50)
        self.assertEqual(bar.current, 50)
        bar.close()

    def test_zero_total(self) -> None:
        """Test ProgressBar with zero total."""
        bar = ProgressBar(total=0, label="Test")
        bar.close()  # Should not crash.

    def test_close_idempotent(self) -> None:
        """Test that close() can be called multiple times safely."""
        bar = ProgressBar(total=10, label="Test")
        bar.update(10)
        bar.close()
        bar.close()  # Second close should be a no-op.

    def test_update_beyond_total(self) -> None:
        """Test that update works even beyond total."""
        bar = ProgressBar(total=10, label="Test")
        bar.update(15)
        self.assertEqual(bar.current, 15)
        bar.close()


class TestFolderEstimateStr(unittest.TestCase):
    """Tests for FolderEstimate __str__."""

    def test_basic(self) -> None:
        from progress import FolderEstimate
        est = FolderEstimate(total_files=100, total_bytes=1024 * 1024 * 50)
        result = str(est)
        self.assertIn("100", result)
        self.assertIn("50.0 MiB", result)

    def test_with_skipped(self) -> None:
        from progress import FolderEstimate
        est = FolderEstimate(total_files=50, total_bytes=1024, skipped_files=5)
        result = str(est)
        self.assertIn("5", result)
        self.assertIn("filtered out", result)


if __name__ == "__main__":
    unittest.main()