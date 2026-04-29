"""Tests for the interactive review module."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from config import Config
from database import Database
from reporter import generate_report
from reviewer import review_corrupted, review_duplicates, review_actions


class TestReviewCorrupted(unittest.TestCase):
    """Tests for review_corrupted using batch_mode."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.db = Database(self.db_path)
        self.config = Config(output_dir=self.tmpdir)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_error_files(self) -> None:
        """When there are no error files, review_corrupted reports zero."""
        stats = review_corrupted(self.db, self.config, dry_run=True, batch_mode="skip")
        self.assertEqual(stats["moved"], 0)
        self.assertEqual(stats["skipped"], 0)

    def test_batch_skip(self) -> None:
        """In batch_mode='skip', all error files are skipped."""
        test_file = Path(self.tmpdir) / "bad.nes"
        test_file.write_bytes(b"\x00" * 1024)

        fid = self.db.upsert_file(
            path=str(test_file.resolve()),
            original_name="bad.nes",
            extension=".nes",
            size=1024,
        )
        self.db.update_file_status(fid, "scan_error", "corrupt zip")

        stats = review_corrupted(self.db, self.config, dry_run=True, batch_mode="skip")
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["moved"], 0)

    def test_batch_move_dry_run(self) -> None:
        """In batch_mode='move' + dry_run, files are counted but not moved."""
        test_file = Path(self.tmpdir) / "bad.nes"
        test_file.write_bytes(b"\x00" * 1024)

        fid = self.db.upsert_file(
            path=str(test_file.resolve()),
            original_name="bad.nes",
            extension=".nes",
            size=1024,
        )
        self.db.update_file_status(fid, "scan_error", "corrupt zip")

        stats = review_corrupted(self.db, self.config, dry_run=True, batch_mode="move")
        self.assertEqual(stats["moved"], 1)
        self.assertEqual(stats["skipped"], 0)
        # File should still exist in original location.
        self.assertTrue(test_file.exists())

    def test_batch_move_applied(self) -> None:
        """In batch_mode='move' + apply, files are actually moved."""
        test_file = Path(self.tmpdir) / "bad.nes"
        test_file.write_bytes(b"\x00" * 1024)

        fid = self.db.upsert_file(
            path=str(test_file.resolve()),
            original_name="bad.nes",
            extension=".nes",
            size=1024,
        )
        self.db.update_file_status(fid, "scan_error", "corrupt zip")

        stats = review_corrupted(self.db, self.config, dry_run=False, batch_mode="move")
        self.assertEqual(stats["moved"], 1)
        # Original file should be gone.
        self.assertFalse(test_file.exists())
        # File should be in corrupted dir.
        corrupted_dir = Path(self.tmpdir) / "corrupted"
        self.assertTrue((corrupted_dir / "bad.nes").exists())


class TestReviewDuplicates(unittest.TestCase):
    """Tests for review_duplicates using batch_mode."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.db = Database(self.db_path)
        self.config = Config(output_dir=self.tmpdir)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_duplicates(self) -> None:
        """When there are no duplicates, review_duplicates reports zero."""
        stats = review_duplicates(self.db, self.config, dry_run=True, batch_mode="skip")
        self.assertEqual(stats["quarantined"], 0)

    def test_batch_skip(self) -> None:
        """In batch_mode='skip', all duplicate groups are kept."""
        # Create two identical files.
        f1 = Path(self.tmpdir) / "game.nes"
        f2 = Path(self.tmpdir) / "game_dup.nes"
        f1.write_bytes(b"\x00" * 4096)
        f2.write_bytes(b"\x00" * 4096)

        fid1 = self.db.upsert_file(str(f1.resolve()), "game.nes", ".nes", 4096)
        fid2 = self.db.upsert_file(str(f2.resolve()), "game_dup.nes", ".nes", 4096)
        self.db.update_file_hash(fid1, "abc123")
        self.db.update_file_hash(fid2, "abc123")

        # Create duplicate group.
        gid = self.db.create_duplicate_group("exact", "abc123")
        self.db.add_file_to_duplicate_group(gid, fid1, is_canonical=True)
        self.db.add_file_to_duplicate_group(gid, fid2, is_canonical=False)

        stats = review_duplicates(self.db, self.config, dry_run=True, batch_mode="skip")
        self.assertEqual(stats["kept"], 1)
        self.assertEqual(stats["quarantined"], 0)


class TestReviewActions(unittest.TestCase):
    """Tests for review_actions using batch_mode."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.db = Database(self.db_path)
        self.config = Config(output_dir=self.tmpdir)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_pending_actions(self) -> None:
        """When there are no pending actions, review_actions reports zero."""
        stats = review_actions(self.db, self.config, dry_run=True, batch_mode="skip")
        self.assertEqual(stats["applied"], 0)

    def test_batch_skip(self) -> None:
        """In batch_mode='skip', all actions are skipped."""
        f1 = Path(self.tmpdir) / "game.nes"
        f1.write_bytes(b"\x00" * 4096)

        fid = self.db.upsert_file(str(f1.resolve()), "game.nes", ".nes", 4096)
        self.db.add_proposed_action(fid, "rename", str(f1), str(f1.parent / "Game.nes"), "normalize")

        stats = review_actions(self.db, self.config, dry_run=True, batch_mode="skip")
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["applied"], 0)

    def test_batch_apply_dry_run(self) -> None:
        """In batch_mode='apply' + dry_run, actions are counted but not applied."""
        f1 = Path(self.tmpdir) / "game.nes"
        f1.write_bytes(b"\x00" * 4096)

        fid = self.db.upsert_file(str(f1.resolve()), "game.nes", ".nes", 4096)
        self.db.add_proposed_action(fid, "rename", str(f1), str(f1.parent / "Game.nes"), "normalize")

        stats = review_actions(self.db, self.config, dry_run=True, batch_mode="apply")
        self.assertEqual(stats["applied"], 1)
        # File should still exist at original path.
        self.assertTrue(f1.exists())


class TestReporterErrors(unittest.TestCase):
    """Tests that the reporter includes error files."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.db = Database(self.db_path)

    def tearDown(self) -> None:
        self.db.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_report_includes_error_section(self) -> None:
        """Report should include ERROR / CORRUPTED FILES section when errors exist."""
        fid = self.db.upsert_file("/roms/bad.zip", "bad.zip", ".zip", 1024)
        self.db.update_file_status(fid, "scan_error", "corrupt zip")

        report = generate_report(self.db)
        self.assertIn("ERROR / CORRUPTED FILES", report)
        self.assertIn("bad.zip", report)

    def test_report_includes_error_counts(self) -> None:
        """Report summary should include error counts."""
        fid = self.db.upsert_file("/roms/bad.zip", "bad.zip", ".zip", 1024)
        self.db.update_file_status(fid, "scan_error", "corrupt zip")

        report = generate_report(self.db)
        self.assertIn("Scan errors:", report)


if __name__ == "__main__":
    unittest.main()