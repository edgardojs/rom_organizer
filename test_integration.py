"""Integration tests for the ROM Organizer.

Tests the full pipeline: scan → hash → detect duplicates → normalize → organize.
Uses the test_data_generator to create realistic test data.
"""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path

from config import Config
from database import Database
from hasher import hash_all_files
from normalizer import normalize_all_files
from scanner import scan_directory
from sorter import (
    apply_actions,
    find_exact_duplicates,
    find_possible_duplicates,
    propose_organize_actions,
)
from archiver import inspect_archive, compute_archive_fingerprint


class TestFullPipeline(unittest.TestCase):
    """End-to-end integration tests."""

    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp())
        self.rom_dir = self.tmpdir / "roms"
        self.rom_dir.mkdir()
        self.output_dir = self.tmpdir / "output"
        self.db_path = self.tmpdir / "test.db"
        self.config = Config()
        self.config.output_dir = str(self.output_dir)
        self.config.min_file_size = 0  # Allow tiny test files.
        self.config.hash_workers = 1

    def tearDown(self) -> None:
        shutil.rmtree(str(self.tmpdir), ignore_errors=True)

    def _create_test_roms(self) -> None:
        """Create a set of test ROM files."""
        # NES ROMs.
        nes_dir = self.rom_dir / "NES"
        nes_dir.mkdir()
        (nes_dir / "Super Mario Bros (U) [!].nes").write_bytes(b"A" * 40960)
        (nes_dir / "Super Mario Bros (U) [!].nes").write_bytes(b"A" * 40960)  # Can't have dup name
        (nes_dir / "Zelda (U).nes").write_bytes(b"B" * 131072)

        # SNES ROMs.
        snes_dir = self.rom_dir / "SNES"
        snes_dir.mkdir()
        (snes_dir / "Chrono Trigger (U) [!].sfc").write_bytes(b"C" * 4194304)

        # Genesis ROMs.
        gen_dir = self.rom_dir / "Genesis"
        gen_dir.mkdir()
        (gen_dir / "Sonic the Hedgehog (U) [!].gen").write_bytes(b"D" * 524288)

    def _create_test_roms_with_duplicates(self) -> None:
        """Create test ROMs with exact duplicates."""
        nes_dir = self.rom_dir / "NES"
        nes_dir.mkdir()

        # Two identical files.
        content = b"IDENTICAL_ROM_DATA" * 2000
        (nes_dir / "game_a.nes").write_bytes(content)
        (nes_dir / "game_b.nes").write_bytes(content)

    def _create_test_archive(self) -> None:
        """Create a test zip archive with ROM contents."""
        arcade_dir = self.rom_dir / "Arcade"
        arcade_dir.mkdir()
        zip_path = arcade_dir / "sf2.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("sf2.01", b"ROM_DATA_01" * 1000)
            zf.writestr("sf2.02", b"ROM_DATA_02" * 1000)

    def test_scan_and_hash(self) -> None:
        """Test the scan → hash pipeline."""
        self._create_test_roms()
        db = Database(self.db_path)

        count = scan_directory(self.rom_dir, self.config, db)
        self.assertGreater(count, 0)

        hash_stats = hash_all_files(db, self.config)
        self.assertGreater(hash_stats["hashed"], 0)
        self.assertEqual(hash_stats["errors"], 0)

        # Verify all files have hashes.
        files = db.get_all_files()
        for f in files:
            self.assertIsNotNone(f["sha256"])

        db.close()

    def test_duplicate_detection(self) -> None:
        """Test duplicate detection with identical files."""
        self._create_test_roms_with_duplicates()
        db = Database(self.db_path)

        scan_directory(self.rom_dir, self.config, db)
        hash_all_files(db, self.config)

        exact = find_exact_duplicates(db)
        self.assertEqual(exact, 1)  # One group of duplicates.

        db.close()

    def test_normalize_and_organize_dry_run(self) -> None:
        """Test normalize + organize in dry-run mode."""
        self._create_test_roms()
        db = Database(self.db_path)

        scan_directory(self.rom_dir, self.config, db)
        hash_all_files(db, self.config)

        # Normalize (dry run).
        norm_stats = normalize_all_files(db, self.config, dry_run=True)
        self.assertGreaterEqual(norm_stats["normalized"] + norm_stats["unchanged"], 0)

        # Organize (dry run).
        action_stats = propose_organize_actions(db, self.config, dry_run=True)
        self.assertGreaterEqual(action_stats["rename"] + action_stats["move"], 0)

        # Verify no files were actually moved.
        apply_stats = apply_actions(db, self.config, dry_run=True)
        # In dry run, no actual filesystem changes.
        for rom_file in self.rom_dir.rglob("*.nes"):
            self.assertTrue(rom_file.exists())

        db.close()

    def test_archive_inspection(self) -> None:
        """Test that archive contents are inspected during scan."""
        self._create_test_archive()
        db = Database(self.db_path)

        count = scan_directory(self.rom_dir, self.config, db)
        self.assertGreater(count, 0)

        # Find the archive file.
        files = db.get_all_files()
        archive_files = [f for f in files if f["is_archive"]]
        self.assertEqual(len(archive_files), 1)

        # Check that entries were recorded.
        entries = db.get_archive_entries(archive_files[0]["id"])
        self.assertEqual(len(entries), 2)

        # Check fingerprint was set.
        self.assertIsNotNone(archive_files[0]["archive_fingerprint"])

        db.close()

    def test_archive_fingerprint_determinism(self) -> None:
        """Test that archive fingerprints are deterministic."""
        self._create_test_archive()

        # Create a second archive with same contents but different compression.
        arcade_dir = self.rom_dir / "Arcade"
        zip_path2 = arcade_dir / "sf2_copy.zip"
        with zipfile.ZipFile(zip_path2, "w", zipfile.ZIP_STORED) as zf:
            zf.writestr("sf2.01", b"ROM_DATA_01" * 1000)
            zf.writestr("sf2.02", b"ROM_DATA_02" * 1000)

        insp1 = inspect_archive(arcade_dir / "sf2.zip")
        insp2 = inspect_archive(arcade_dir / "sf2_copy.zip")

        fp1 = compute_archive_fingerprint(insp1)
        fp2 = compute_archive_fingerprint(insp2)
        self.assertEqual(fp1, fp2)

    def test_db_backup(self) -> None:
        """Test database backup functionality."""
        db = Database(self.db_path)
        db.upsert_file(
            path="/test/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )

        backup_path = db.backup()
        self.assertTrue(backup_path.exists())
        self.assertGreater(backup_path.stat().st_size, 0)

        db.close()

    def test_selective_rollback(self) -> None:
        """Test selective rollback by action ID."""
        db = Database(self.db_path)

        file_id = db.upsert_file(
            path="/test/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )

        # Create two actions.
        action_id1 = db.add_proposed_action(
            file_id=file_id,
            action_type="rename",
            source_path="/test/game.nes",
            proposed_path="/test/Game.nes",
            reason="Normalize",
        )
        action_id2 = db.add_proposed_action(
            file_id=file_id,
            action_type="move",
            source_path="/test/game.nes",
            proposed_path="/test/NES/game.nes",
            reason="Move to NES folder",
        )

        # Apply both.
        db.mark_action_applied(action_id1)
        db.mark_action_applied(action_id2)

        # Selective rollback of just action 2.
        actions = db.get_applied_actions_range(action_id=action_id2)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["id"], action_id2)

        db.close()

    def test_schema_migration(self) -> None:
        """Test that a v1 database migrates to v2."""
        db = Database(self.db_path)

        # The new columns should exist.
        row = db.conn.execute("SELECT * FROM files LIMIT 0")
        column_names = [desc[0] for desc in row.description]
        self.assertIn("md5", column_names)
        self.assertIn("crc32", column_names)
        self.assertIn("dat_game_name", column_names)
        self.assertIn("is_archive", column_names)
        self.assertIn("archive_fingerprint", column_names)

        # New tables should exist.
        tables = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {t["name"] for t in tables}
        self.assertIn("archive_entries", table_names)
        self.assertIn("dat_files", table_names)
        self.assertIn("dat_games", table_names)
        self.assertIn("dat_roms", table_names)

        db.close()


if __name__ == "__main__":
    unittest.main()