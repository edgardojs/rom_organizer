"""Unit tests for the ROM Organizer config module."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from config import Config, load_config


class TestConfig(unittest.TestCase):
    """Tests for the Config class."""

    def test_default_values(self) -> None:
        config = Config()
        self.assertEqual(config.hash_algorithm, "sha256")
        self.assertEqual(config.min_file_size, 1024)
        self.assertEqual(config.max_filename_length, 255)
        self.assertEqual(config.quarantine_subdir, "quarantine")

    def test_from_dict(self) -> None:
        config = Config.from_dict({"hash_algorithm": "sha1", "min_file_size": 2048})
        self.assertEqual(config.hash_algorithm, "sha1")
        self.assertEqual(config.min_file_size, 2048)

    def test_from_dict_ignores_unknown_keys(self) -> None:
        config = Config.from_dict({"unknown_key": "value", "hash_algorithm": "sha1"})
        self.assertEqual(config.hash_algorithm, "sha1")
        self.assertFalse(hasattr(config, "unknown_key"))

    def test_from_dict_coerces_string_int(self) -> None:
        config = Config.from_dict({"min_file_size": "4096"})
        self.assertEqual(config.min_file_size, 4096)

    def test_from_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"hash_algorithm": "sha1", "min_file_size": 512}, f)
            f.flush()
            config = Config.from_file(Path(f.name))
        self.assertEqual(config.hash_algorithm, "sha1")
        self.assertEqual(config.min_file_size, 512)

    def test_load_config_with_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"min_file_size": 0}, f)
            f.flush()
            config = load_config(Path(f.name))
        self.assertEqual(config.min_file_size, 0)

    def test_load_config_without_file(self) -> None:
        config = load_config(None)
        self.assertEqual(config.hash_algorithm, "sha256")

    def test_get_extension_to_system_map(self) -> None:
        config = Config()
        mapping = config.get_extension_to_system_map()
        self.assertEqual(mapping[".nes"], "nes")
        self.assertEqual(mapping[".sfc"], "snes")
        self.assertEqual(mapping[".gen"], "genesis")

    def test_get_quarantine_path(self) -> None:
        config = Config()
        path = config.get_quarantine_path()
        self.assertEqual(str(path), "rom_organizer_output/quarantine")


if __name__ == "__main__":
    unittest.main()