"""Unit tests for the ROM Organizer DAT parser module."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from dat_parser import DATFile, DATGame, DATRom, parse_dat, identify_file


SAMPLE_DAT_NO_INTRO = """<?xml version="1.0" encoding="UTF-8"?>
<datafile>
    <header>
        <name>NES - No-Intro</name>
        <description>Nintendo Entertainment System</description>
        <version>20240101</version>
    </header>
    <game name="Super Mario Bros. (USA)">
        <description>Super Mario Bros. (USA)</description>
        <rom name="Super Mario Bros. (USA).nes" size="40960" crc="ABCD1234" md5="md5hash1234" sha256="sha256hash1234"/>
    </game>
    <game name="The Legend of Zelda (USA)">
        <description>The Legend of Zelda (USA)</description>
        <rom name="The Legend of Zelda (USA).nes" size="131072" crc="EFGH5678" md5="md5hash5678" sha256="sha256hash5678"/>
    </game>
</datafile>
"""

SAMPLE_DAT_MAME = """<?xml version="1.0" encoding="UTF-8"?>
<datafile>
    <header>
        <name>MAME 0.260</name>
        <description>MAME ROMs</description>
    </header>
    <game name="sf2" cloneof="sf2ce">
        <description>Street Fighter II</description>
        <year>1991</year>
        <manufacturer>Capcom</manufacturer>
        <rom name="sf2.01" size="524288" crc="11111111" sha256="arcade_hash_01"/>
        <rom name="sf2.02" size="524288" crc="22222222" sha256="arcade_hash_02"/>
    </game>
</datafile>
"""


class TestParseDat(unittest.TestCase):
    """Tests for parse_dat()."""

    def test_parse_no_intro_dat(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        self.assertEqual(dat.header_name, "NES - No-Intro")
        self.assertEqual(dat.header_version, "20240101")
        self.assertEqual(len(dat.games), 2)

        # Check first game.
        game = dat.games[0]
        self.assertEqual(game.name, "Super Mario Bros. (USA)")
        self.assertEqual(len(game.roms), 1)
        self.assertEqual(game.roms[0].sha256, "sha256hash1234")
        self.assertEqual(game.roms[0].size, 40960)

    def test_parse_mame_dat(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_MAME)
            f.flush()
            dat = parse_dat(Path(f.name))

        self.assertEqual(len(dat.games), 1)
        game = dat.games[0]
        self.assertEqual(game.name, "sf2")
        self.assertTrue(game.is_clone)
        self.assertEqual(game.clone_of, "sf2ce")
        self.assertEqual(game.year, "1991")
        self.assertEqual(game.manufacturer, "Capcom")
        self.assertEqual(len(game.roms), 2)

    def test_sha256_lookup(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        results = dat.lookup_by_sha256("sha256hash1234")
        self.assertEqual(len(results), 1)
        game, rom = results[0]
        self.assertEqual(game.name, "Super Mario Bros. (USA)")
        self.assertEqual(rom.name, "Super Mario Bros. (USA).nes")

    def test_md5_lookup(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        results = dat.lookup_by_md5("md5hash5678")
        self.assertEqual(len(results), 1)
        game, rom = results[0]
        self.assertEqual(game.name, "The Legend of Zelda (USA)")

    def test_crc32_lookup(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        results = dat.lookup_by_crc32("abcd1234")
        self.assertEqual(len(results), 1)

    def test_no_match(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        results = dat.lookup_by_sha256("nonexistent_hash")
        self.assertEqual(len(results), 0)

    def test_parse_invalid_xml(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write("not xml at all")
            f.flush()
            dat = parse_dat(Path(f.name))

        self.assertEqual(len(dat.games), 0)

    def test_parse_empty_dat(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write('<?xml version="1.0"?><datafile><header><name>Empty</name></header></datafile>')
            f.flush()
            dat = parse_dat(Path(f.name))

        self.assertEqual(len(dat.games), 0)
        self.assertEqual(dat.header_name, "Empty")


class TestIdentifyFile(unittest.TestCase):
    """Tests for identify_file()."""

    def test_identify_by_sha256(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        results = identify_file(sha256="sha256hash1234", dat_files=[dat])
        self.assertEqual(len(results), 1)
        _, game, rom = results[0]
        self.assertEqual(game.name, "Super Mario Bros. (USA)")

    def test_identify_no_match(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dat", delete=False) as f:
            f.write(SAMPLE_DAT_NO_INTRO)
            f.flush()
            dat = parse_dat(Path(f.name))

        results = identify_file(sha256="no_match", dat_files=[dat])
        self.assertEqual(len(results), 0)

    def test_identify_no_dat_files(self) -> None:
        results = identify_file(sha256="any_hash", dat_files=[])
        self.assertEqual(len(results), 0)


class TestLoadDatDir(unittest.TestCase):
    """Tests for load_dat_dir()."""

    def test_load_from_directory(self) -> None:
        from dat_parser import load_dat_dir

        tmpdir = tempfile.mkdtemp()
        dat_path = Path(tmpdir) / "test.dat"
        dat_path.write_text(SAMPLE_DAT_NO_INTRO)

        dats = load_dat_dir(Path(tmpdir))
        self.assertEqual(len(dats), 1)
        self.assertEqual(len(dats[0].games), 2)

    def test_load_from_nonexistent_dir(self) -> None:
        from dat_parser import load_dat_dir

        dats = load_dat_dir(Path("/nonexistent/path"))
        self.assertEqual(len(dats), 0)


if __name__ == "__main__":
    unittest.main()