"""Unit tests for the ROM Organizer database module."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from database import Database, _Transaction


class TestDatabase(unittest.TestCase):
    """Tests for the Database class."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.db = Database(self.db_path)

    def tearDown(self) -> None:
        self.db.close()

    # ── Basic operations ───────────────────────────────────────────────

    def test_upsert_file(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        self.assertIsNotNone(file_id)
        self.assertGreater(file_id, 0)

    def test_upsert_file_idempotent(self) -> None:
        id1 = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        id2 = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        self.assertEqual(id1, id2)

    def test_update_file_hash(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        self.db.update_file_hash(file_id, "abc123")
        row = self.db.get_file_by_id(file_id)
        self.assertEqual(row["sha256"], "abc123")

    def test_update_file_normalized_name(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        self.db.update_file_normalized_name(file_id, "Game (U).nes")
        row = self.db.get_file_by_id(file_id)
        self.assertEqual(row["normalized_name"], "Game (U).nes")
        self.assertEqual(row["status"], "normalized")

    # ── Re-scan preserves data ────────────────────────────────────────

    def test_upsert_preserves_normalized_name_on_re_scan(self) -> None:
        """Re-scanning should not overwrite an existing normalized_name."""
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        self.db.update_file_normalized_name(file_id, "Game (U).nes")

        # Re-scan (same path, no normalized_name provided).
        self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        row = self.db.get_file_by_id(file_id)
        self.assertEqual(row["normalized_name"], "Game (U).nes")

    def test_upsert_preserves_hash_on_re_scan(self) -> None:
        """Re-scanning should not overwrite an existing hash."""
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        self.db.update_file_hash(file_id, "abc123")

        # Re-scan without hash.
        self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        row = self.db.get_file_by_id(file_id)
        self.assertEqual(row["sha256"], "abc123")

    # ── Duplicate groups ───────────────────────────────────────────────

    def test_create_duplicate_group(self) -> None:
        group_id = self.db.create_duplicate_group("exact", sha256="abc123")
        self.assertIsNotNone(group_id)

    def test_add_file_to_duplicate_group(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        group_id = self.db.create_duplicate_group("exact", sha256="abc123")
        self.db.add_file_to_duplicate_group(group_id, file_id, is_canonical=True)

    def test_get_member_canonical_status(self) -> None:
        file_id1 = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        file_id2 = self.db.upsert_file(
            path="/roms/game2.nes",
            original_name="game2.nes",
            extension=".nes",
            size=40960,
        )
        group_id = self.db.create_duplicate_group("exact", sha256="abc123")
        self.db.add_file_to_duplicate_group(group_id, file_id1, is_canonical=True)
        self.db.add_file_to_duplicate_group(group_id, file_id2, is_canonical=False)

        self.assertTrue(self.db.get_member_canonical_status(group_id, file_id1))
        self.assertFalse(self.db.get_member_canonical_status(group_id, file_id2))

    # ── Proposed actions ───────────────────────────────────────────────

    def test_add_proposed_action(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        action_id = self.db.add_proposed_action(
            file_id=file_id,
            action_type="rename",
            source_path="/roms/game.nes",
            proposed_path="/roms/Game.nes",
            reason="Normalize",
        )
        self.assertIsNotNone(action_id)

    def test_mark_action_applied(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        action_id = self.db.add_proposed_action(
            file_id=file_id,
            action_type="rename",
            source_path="/roms/game.nes",
            proposed_path="/roms/Game.nes",
            reason="Normalize",
        )
        self.db.mark_action_applied(action_id)
        pending = self.db.get_pending_actions()
        self.assertEqual(len(pending), 0)

    def test_update_proposed_action_source_path(self) -> None:
        file_id = self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        action_id = self.db.add_proposed_action(
            file_id=file_id,
            action_type="move",
            source_path="/roms/game.nes",
            proposed_path="/roms/NES/game.nes",
            reason="Move to NES folder",
        )
        self.db.update_proposed_action_source_path(action_id, "/roms/Game.nes")
        pending = self.db.get_pending_actions()
        self.assertEqual(pending[0]["source_path"], "/roms/Game.nes")

    # ── Transaction support ───────────────────────────────────────────

    def test_transaction_commits_on_success(self) -> None:
        with self.db.transaction():
            self.db.upsert_file(
                path="/roms/game.nes",
                original_name="game.nes",
                extension=".nes",
                size=40960,
            )
        # Data should be committed.
        files = self.db.get_all_files()
        self.assertEqual(len(files), 1)

    def test_transaction_rolls_back_on_exception(self) -> None:
        try:
            with self.db.transaction():
                self.db.upsert_file(
                    path="/roms/game.nes",
                    original_name="game.nes",
                    extension=".nes",
                    size=40960,
                )
                raise RuntimeError("Simulated error")
        except RuntimeError:
            pass
        # Data should be rolled back.
        files = self.db.get_all_files()
        self.assertEqual(len(files), 0)

    # ── Stats ─────────────────────────────────────────────────────────

    def test_get_stats_empty(self) -> None:
        stats = self.db.get_stats()
        self.assertEqual(stats["total_files"], 0)
        self.assertEqual(stats["unique_hashes"], 0)
        self.assertEqual(stats["exact_duplicate_groups"], 0)

    def test_get_stats_with_data(self) -> None:
        self.db.upsert_file(
            path="/roms/game.nes",
            original_name="game.nes",
            extension=".nes",
            size=40960,
        )
        stats = self.db.get_stats()
        self.assertEqual(stats["total_files"], 1)

    def test_error_file_tracking(self) -> None:
        """Test that scan_error and hash_error statuses are tracked."""
        fid = self.db.upsert_file(
            path="/roms/bad.zip",
            original_name="bad.zip",
            extension=".zip",
            size=1024,
        )
        self.db.update_file_status(fid, "scan_error", "Archive inspection failed: corrupt")
        errors = self.db.get_error_files()
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["status"], "scan_error")
        self.assertIn("corrupt", errors[0]["notes"])

    def test_get_stats_error_counts(self) -> None:
        """Test that get_stats includes scan_errors and hash_errors."""
        fid1 = self.db.upsert_file(
            path="/roms/bad1.zip",
            original_name="bad1.zip",
            extension=".zip",
            size=1024,
        )
        fid2 = self.db.upsert_file(
            path="/roms/bad2.nes",
            original_name="bad2.nes",
            extension=".nes",
            size=4096,
        )
        self.db.update_file_status(fid1, "scan_error", "corrupt archive")
        self.db.update_file_status(fid2, "hash_error", "permission denied")
        stats = self.db.get_stats()
        self.assertEqual(stats["scan_errors"], 1)
        self.assertEqual(stats["hash_errors"], 1)


if __name__ == "__main__":
    unittest.main()