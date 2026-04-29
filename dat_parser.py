"""DAT file parser for the ROM Organizer.

Parses No-Intro and Redump DAT files (XML format) to build a lookup table
of known ROMs by their hashes. This enables identification of ROM files
by matching their SHA-256/MD5/CRC32 hashes against known databases.

No-Intro DATs cover cartridge-based systems (NES, SNES, GBA, etc.).
Redump DATs cover disc-based systems (PS1, PS2, Dreamcast, etc.).
FBNeo/MAME DATs cover arcade ROMs (zip archives with multiple ROMs).

Design decision: We parse DATs at scan time and store the game info in
the database so lookups are fast. The parser is tolerant of variations
in DAT format since different groups use slightly different XML schemas.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DATRom:
    """A single ROM entry from a DAT file."""

    name: str
    size: int
    crc32: Optional[str] = None
    md5: Optional[str] = None
    sha256: Optional[str] = None
    merge_name: Optional[str] = None  # For MAME/FBNeo merged ROMs


@dataclass
class DATGame:
    """A game entry from a DAT file, containing one or more ROMs."""

    name: str
    description: str = ""
    category: str = ""  # System/category from the DAT header
    roms: list[DATRom] = field(default_factory=list)
    is_clone: bool = False
    clone_of: Optional[str] = None
    year: Optional[str] = None
    manufacturer: Optional[str] = None


@dataclass
class DATFile:
    """A parsed DAT file."""

    filename: str
    header_name: str = ""
    header_description: str = ""
    header_category: str = ""
    header_version: str = ""
    games: list[DATGame] = field(default_factory=list)

    # Reverse lookup indexes: hash → list of (game, rom) pairs.
    _sha256_index: dict[str, list[tuple[DATGame, DATRom]]] = field(
        default_factory=dict, repr=False
    )
    _md5_index: dict[str, list[tuple[DATGame, DATRom]]] = field(
        default_factory=dict, repr=False
    )
    _crc32_index: dict[str, list[tuple[DATGame, DATRom]]] = field(
        default_factory=dict, repr=False
    )

    def build_indexes(self) -> None:
        """Build reverse-lookup indexes from hash → (game, rom) pairs.

        Must be called after parsing and after all games are added.
        """
        self._sha256_index.clear()
        self._md5_index.clear()
        self._crc32_index.clear()

        for game in self.games:
            for rom in game.roms:
                if rom.sha256:
                    self._sha256_index.setdefault(rom.sha256.lower(), []).append(
                        (game, rom)
                    )
                if rom.md5:
                    self._md5_index.setdefault(rom.md5.lower(), []).append(
                        (game, rom)
                    )
                if rom.crc32:
                    self._crc32_index.setdefault(rom.crc32.lower(), []).append(
                        (game, rom)
                    )

    def lookup_by_sha256(self, sha256: str) -> list[tuple[DATGame, DATRom]]:
        """Look up games/ROMs by SHA-256 hash."""
        return self._sha256_index.get(sha256.lower(), [])

    def lookup_by_md5(self, md5: str) -> list[tuple[DATGame, DATRom]]:
        """Look up games/ROMs by MD5 hash."""
        return self._md5_index.get(md5.lower(), [])

    def lookup_by_crc32(self, crc32: str) -> list[tuple[DATGame, DATRom]]:
        """Look up games/ROMs by CRC32 hash."""
        return self._crc32_index.get(crc32.lower(), [])


def parse_dat(path: Path) -> DATFile:
    """Parse a DAT file (XML format) and return a DATFile object.

    Supports both No-Intro style and MAME/Logiqx style DAT files.

    No-Intro format:
        <datafile>
            <header><name>...</name><description>...</description></header>
            <game name="...">
                <description>...</description>
                <rom name="..." size="..." crc="..." md5="..." sha1="..."/>
            </game>
        </datafile>

    MAME/Logiqx format:
        <datafile>
            <header><name>...</name></header>
            <game name="..." cloneof="..." ismechanical="no">
                <description>...</description>
                <year>...</year>
                <manufacturer>...</manufacturer>
                <rom name="..." size="..." crc="..." md5="..." sha1="..."/>
            </game>
        </datafile>

    Args:
        path: Path to the DAT file.

    Returns:
        A parsed DATFile object with indexes built.
    """
    dat = DATFile(filename=str(path))

    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        logger.error("Failed to parse DAT file %s: %s", path, exc)
        return dat
    except Exception as exc:
        logger.error("Error reading DAT file %s: %s", path, exc)
        return dat

    root = tree.getroot()

    # Parse header.
    header = root.find("header")
    if header is not None:
        dat.header_name = _text(header, "name")
        dat.header_description = _text(header, "description")
        dat.header_category = _text(header, "category")
        dat.header_version = _text(header, "version")

    # Parse game entries.
    for game_elem in root.iter("game"):
        game = _parse_game(game_elem, dat.header_category)
        dat.games.append(game)

    dat.build_indexes()
    logger.info(
        "Parsed DAT: %s — %d games, %d ROMs, %d SHA-256 entries",
        path.name,
        len(dat.games),
        sum(len(g.roms) for g in dat.games),
        len(dat._sha256_index),
    )
    return dat


def _parse_game(game_elem: ET.Element, default_category: str = "") -> DATGame:
    """Parse a <game> element from a DAT file.

    Args:
        game_elem: The XML element.
        default_category: Default category from the DAT header.

    Returns:
        A DATGame object.
    """
    game = DATGame(
        name=game_elem.get("name", ""),
        description=_text(game_elem, "description"),
        category=_text(game_elem, "category") or default_category,
        clone_of=game_elem.get("cloneof"),
        is_clone=bool(game_elem.get("cloneof")),
        year=_text(game_elem, "year"),
        manufacturer=_text(game_elem, "manufacturer"),
    )

    for rom_elem in game_elem.iter("rom"):
        rom = _parse_rom(rom_elem)
        if rom is not None:
            game.roms.append(rom)

    return game


def _parse_rom(rom_elem: ET.Element) -> Optional[DATRom]:
    """Parse a <rom> element from a DAT file.

    Args:
        rom_elem: The XML element.

    Returns:
        A DATRom object, or None if the element has no useful data.
    """
    name = rom_elem.get("name", "")
    size_str = rom_elem.get("size", "0")
    try:
        size = int(size_str)
    except ValueError:
        size = 0

    if not name and size == 0:
        return None

    return DATRom(
        name=name,
        size=size,
        crc32=rom_elem.get("crc"),
        md5=rom_elem.get("md5"),
        sha256=rom_elem.get("sha256") or rom_elem.get("sha1"),  # Some DATs use sha1
        merge_name=rom_elem.get("merge"),
    )


def _text(element: ET.Element, tag: str) -> str:
    """Get text content of a child element, or empty string."""
    child = element.find(tag)
    return child.text.strip() if child is not None and child.text else ""


def load_dat_dir(dat_dir: Path) -> list[DATFile]:
    """Load all DAT files from a directory.

    Args:
        dat_dir: Directory containing .dat files.

    Returns:
        A list of parsed DATFile objects.
    """
    if not dat_dir.exists() or not dat_dir.is_dir():
        logger.warning("DAT directory does not exist: %s", dat_dir)
        return []

    dat_files = []
    for path in sorted(dat_dir.glob("*.dat")):
        dat = parse_dat(path)
        if dat.games:
            dat_files.append(dat)

    # Also check for .xml files (some DATs use .xml extension).
    for path in sorted(dat_dir.glob("*.xml")):
        dat = parse_dat(path)
        if dat.games:
            dat_files.append(dat)

    logger.info("Loaded %d DAT files from %s", len(dat_files), dat_dir)
    return dat_files


def identify_file(
    sha256: str | None = None,
    md5: str | None = None,
    crc32: str | None = None,
    dat_files: list[DATFile] | None = None,
) -> list[tuple[DATFile, DATGame, DATRom]]:
    """Identify a file by matching its hashes against loaded DAT files.

    Args:
        sha256: SHA-256 hash of the file.
        md5: MD5 hash of the file.
        crc32: CRC32 hash of the file.
        dat_files: List of loaded DAT files to search.

    Returns:
        A list of (dat_file, game, rom) matches.
    """
    if not dat_files:
        return []

    matches = []
    seen = set()

    # Try SHA-256 first (most specific).
    if sha256:
        for dat in dat_files:
            for game, rom in dat.lookup_by_sha256(sha256):
                key = (dat.filename, game.name, rom.name)
                if key not in seen:
                    seen.add(key)
                    matches.append((dat, game, rom))

    # Fall back to MD5.
    if md5 and not matches:
        for dat in dat_files:
            for game, rom in dat.lookup_by_md5(md5):
                key = (dat.filename, game.name, rom.name)
                if key not in seen:
                    seen.add(key)
                    matches.append((dat, game, rom))

    # Fall back to CRC32.
    if crc32 and not matches:
        for dat in dat_files:
            for game, rom in dat.lookup_by_crc32(crc32):
                key = (dat.filename, game.name, rom.name)
                if key not in seen:
                    seen.add(key)
                    matches.append((dat, game, rom))

    return matches