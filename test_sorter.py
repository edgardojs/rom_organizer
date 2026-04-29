"""Unit tests for the ROM Organizer sorter module."""

from __future__ import annotations

import unittest
from pathlib import Path

from sorter import _is_safe_path, _is_relative_to


class TestIsSafePath(unittest.TestCase):
    """Tests for _is_safe_path()."""

    def test_safe_path_within_base(self) -> None:
        base = Path("/roms/output")
        path = Path("/roms/output/NES/game.nes")
        self.assertTrue(_is_safe_path(path, base))

    def test_safe_path_is_base(self) -> None:
        base = Path("/roms/output")
        path = Path("/roms/output")
        self.assertTrue(_is_safe_path(path, base))

    def test_unsafe_path_traversal(self) -> None:
        base = Path("/roms/output")
        path = Path("/roms/output/../../etc/passwd")
        self.assertFalse(_is_safe_path(path, base))

    def test_unsafe_path_outside_base(self) -> None:
        base = Path("/roms/output")
        path = Path("/etc/passwd")
        self.assertFalse(_is_safe_path(path, base))

    def test_safe_quarantine_path(self) -> None:
        base = Path("/roms/output/quarantine")
        path = Path("/roms/output/quarantine/game.nes")
        self.assertTrue(_is_safe_path(path, base))


class TestIsRelativeTo(unittest.TestCase):
    """Tests for _is_relative_to()."""

    def test_path_inside_base(self) -> None:
        path = Path("/roms/NES/game.nes")
        base = Path("/roms/NES")
        self.assertTrue(_is_relative_to(path, base))

    def test_path_outside_base(self) -> None:
        path = Path("/roms/SNES/game.sfc")
        base = Path("/roms/NES")
        self.assertFalse(_is_relative_to(path, base))

    def test_path_is_base(self) -> None:
        path = Path("/roms/NES")
        base = Path("/roms/NES")
        self.assertTrue(_is_relative_to(path, base))

    def test_path_with_similar_prefix(self) -> None:
        """Ensure /roms/SNES2 is not considered inside /roms/SNES."""
        path = Path("/roms/SNES2/game.sfc")
        base = Path("/roms/SNES")
        self.assertFalse(_is_relative_to(path, base))


if __name__ == "__main__":
    unittest.main()