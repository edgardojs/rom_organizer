"""Unit tests for the ROM Organizer normalizer module."""

from __future__ import annotations

import unittest

from config import Config
from normalizer import normalize_filename, is_unusual_name, matches_naming_rules


class TestNormalizeFilename(unittest.TestCase):
    """Tests for normalize_filename()."""

    def setUp(self) -> None:
        self.config = Config()

    # ── Basic normalization ────────────────────────────────────────────

    def test_strips_whitespace(self) -> None:
        result = normalize_filename("  Super Mario Bros  .nes", self.config)
        self.assertEqual(result, "Super Mario Bros.nes")

    def test_collapses_multiple_spaces(self) -> None:
        result = normalize_filename("Super  Mario  Bros.nes", self.config)
        self.assertEqual(result, "Super Mario Bros.nes")

    def test_replaces_underscores_with_spaces(self) -> None:
        result = normalize_filename("Super_Mario_Bros.nes", self.config)
        self.assertEqual(result, "Super Mario Bros.nes")

    def test_replaces_dashes_with_spaces(self) -> None:
        result = normalize_filename("Super-Mario-Bros.nes", self.config)
        self.assertEqual(result, "Super Mario Bros.nes")

    def test_replaces_plus_with_spaces(self) -> None:
        result = normalize_filename("Game+With+Plus.gba", self.config)
        self.assertEqual(result, "Game With Plus.gba")

    def test_preserves_extension_case(self) -> None:
        result = normalize_filename("game.NES", self.config)
        self.assertTrue(result.endswith(".nes"))

    def test_preserves_region_tag(self) -> None:
        result = normalize_filename("Super Mario Bros (U).nes", self.config)
        self.assertIn("(U)", result)

    def test_preserves_bracket_tag(self) -> None:
        result = normalize_filename("Super Mario Bros [!].nes", self.config)
        self.assertIn("[!]", result)

    # ── Region code uppercasing ────────────────────────────────────────

    def test_uppercases_known_region_codes(self) -> None:
        result = normalize_filename("super mario bros (u).nes", self.config)
        self.assertIn("(U)", result)

    def test_uppercases_known_multi_char_regions(self) -> None:
        result = normalize_filename("game (usa).nes", self.config)
        self.assertIn("(USA)", result)

    def test_does_not_uppercase_unknown_short_codes(self) -> None:
        """Version tags like (v1) should NOT be uppercased."""
        result = normalize_filename("game (v1).nes", self.config)
        self.assertIn("(v1)", result)

    def test_does_not_uppercase_unknown_three_letter_codes(self) -> None:
        """Unknown 3-letter codes should not be uppercased."""
        result = normalize_filename("game (abc).nes", self.config)
        self.assertIn("(abc)", result)

    def test_uppercases_pal_region(self) -> None:
        result = normalize_filename("game (pal).nes", self.config)
        self.assertIn("(PAL)", result)

    # ── Title casing ──────────────────────────────────────────────────

    def test_title_cases_all_lowercase(self) -> None:
        result = normalize_filename("super mario bros.nes", self.config)
        self.assertEqual(result, "Super Mario Bros.nes")

    def test_title_cases_all_uppercase(self) -> None:
        result = normalize_filename("SUPER MARIO BROS.nes", self.config)
        self.assertEqual(result, "Super Mario Bros.nes")

    def test_does_not_mangle_mixed_case(self) -> None:
        result = normalize_filename("SuperMarioBros.nes", self.config)
        # Mixed case should be preserved (not re-title-cased).
        self.assertIn("SuperMarioBros", result)

    # ── Separator handling ────────────────────────────────────────────

    def test_preserves_separators_in_brackets(self) -> None:
        result = normalize_filename("Game [T+Eng].nes", self.config)
        self.assertIn("[T+Eng]", result)

    def test_preserves_counter_suffix_underscore(self) -> None:
        result = normalize_filename("Game_1.nes", self.config)
        self.assertIn("_1", result)

    def test_preserves_counter_suffix_dash(self) -> None:
        result = normalize_filename("Game-2.nes", self.config)
        self.assertIn("-2", result)

    # ── Edge cases ─────────────────────────────────────────────────────

    def test_no_extension(self) -> None:
        result = normalize_filename("GameFile", self.config)
        self.assertNotIn(".", result)

    def test_multiple_extensions_detected(self) -> None:
        result = normalize_filename("weird.rom.tar.nes", self.config)
        # Should still produce a valid normalized name.
        self.assertIsNotNone(result)

    def test_unchanged_filename_returns_original(self) -> None:
        original = "Super Mario Bros (U) [!].nes"
        result = normalize_filename(original, self.config)
        self.assertEqual(result, original)

    def test_long_filename_truncated(self) -> None:
        config = Config()
        config.max_filename_length = 50
        long_name = "A" * 100 + ".nes"
        result = normalize_filename(long_name, config)
        self.assertLessEqual(len(result), 50)


class TestIsUnusualName(unittest.TestCase):
    """Tests for is_unusual_name()."""

    def test_normal_name(self) -> None:
        self.assertFalse(is_unusual_name("Super Mario Bros (U) [!].nes"))

    def test_non_ascii(self) -> None:
        self.assertTrue(is_unusual_name("Pokémon Red (U).gb"))

    def test_very_long_name(self) -> None:
        self.assertTrue(is_unusual_name("A" * 150 + ".nes"))

    def test_leading_whitespace(self) -> None:
        self.assertTrue(is_unusual_name("  Game.nes"))

    def test_multiple_extensions(self) -> None:
        self.assertTrue(is_unusual_name("game.tar.gz"))

    def test_no_alphanumeric(self) -> None:
        self.assertTrue(is_unusual_name("___.___"))


class TestMatchesNamingRules(unittest.TestCase):
    """Tests for matches_naming_rules()."""

    def test_well_named_rom(self) -> None:
        self.assertTrue(matches_naming_rules("Super Mario Bros (U) [!].nes"))

    def test_name_without_tags(self) -> None:
        self.assertTrue(matches_naming_rules("Super Mario Bros.nes"))

    def test_empty_name(self) -> None:
        self.assertFalse(matches_naming_rules(".nes"))

    def test_no_alphanumeric(self) -> None:
        self.assertFalse(matches_naming_rules("___.___"))


if __name__ == "__main__":
    unittest.main()