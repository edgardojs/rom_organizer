#!/usr/bin/env python3
"""Test data generator for the ROM Organizer.

Creates a temporary directory structure with fake ROM files for testing.
The files contain random data so they produce different hashes, except
for intentional duplicates which share the same content.

Usage:
    python test_data_generator.py [--output-dir /tmp/test_roms] [--count 30]

Design decision: This script generates real (small) files with actual
random content so the hasher and deduplication logic can be tested
end-to-end. It also creates intentionally problematic filenames to
exercise the normalizer and unusual-name detector.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import string
from pathlib import Path


# ── Fake ROM filenames that exercise various edge cases ────────────────────

SAMPLE_FILES: list[dict[str, str | int]] = [
    # Well-named ROMs.
    {"name": "Super Mario Bros. (U) [!].nes", "size": 40960},
    {"name": "Super Mario Bros. (U) [!].nes", "size": 40960},  # exact duplicate
    {"name": "The Legend of Zelda (U) [!].nes", "size": 131072},
    {"name": "Metroid (U) [!].nes", "size": 131072},
    {"name": "Chrono Trigger (U) [!].sfc", "size": 4194304},
    {"name": "Super Metroid (U) [!].sfc", "size": 3145728},
    {"name": "Sonic the Hedgehog (U) [!].gen", "size": 524288},
    {"name": "Sonic the Hedgehog (U) [!].gen", "size": 524288},  # exact duplicate
    {"name": "Pokémon Red (U) [S].gb", "size": 1048576},
    {"name": "Pokémon Blue (U) [S].gb", "size": 1048576},
    {"name": "Super Mario 64 (U) [!].z64", "size": 8388608},
    {"name": "Mario Kart 64 (U) [!].z64", "size": 12582912},
    {"name": "Final Fantasy VII (U) (Disc 1).bin", "size": 524288000},
    {"name": "Castlevania - Symphony of the Night (U).bin", "size": 524288000},

    # Possible duplicates: same name stem + same size, different content.
    {"name": "Super Mario Bros. (J) [!].nes", "size": 40960},
    {"name": "Sonic the Hedgehog (E) [!].gen", "size": 524288},

    # Unusual filenames.
    {"name": "  Super  Mario  Bros  (U).nes", "size": 40960},
    {"name": "Super_Mario_Bros_(U).nes", "size": 40960},
    {"name": "SUPER MARIO BROS (U).nes", "size": 40960},
    {"name": "super mario bros (u).nes", "size": 40960},
    {"name": "Zelda 3 (U).sfc", "size": 1048576},
    {"name": "A Link to the Past (U) (V1.1) [!].sfc", "size": 1048576},

    # Very unusual names.
    {"name": "___game___(USA)___[b1].nes", "size": 40960},
    {"name": "ROM with spaces   and   stuff   (E).gb", "size": 262144},
    {"name": "Game+With+Plus+Signs (J).gba", "size": 4194304},

    # Files with multiple extensions (unusual).
    {"name": "weird.rom.tar.nes", "size": 20480},

    # Long name.
    {"name": "Super Ultimate Mega Collection All Stars World Championship Edition Plus Alpha (U) [!].nes", "size": 65536},
]


def generate_random_content(size: int, seed: int | None = None) -> bytes:
    """Generate random bytes of the specified size.

    Args:
        size: Number of bytes to generate.
        seed: Optional random seed for reproducibility.

    Returns:
        Random bytes.
    """
    if seed is not None:
        random.seed(seed)
    # For large files, generate a small chunk and repeat to save time.
    if size > 65536:
        chunk = os.urandom(65536)
        repeats = size // 65536
        remainder = size % 65536
        return (chunk * repeats) + chunk[:remainder]
    return os.urandom(size)


def create_test_data(output_dir: Path, count: int | None = None) -> dict[str, str]:
    """Create a test ROM directory with fake files.

    Args:
        output_dir: Directory to create test files in.
        count: Optional limit on number of files to create.

    Returns:
        A dict mapping filename to SHA-256 hash.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Track content for exact duplicates.
    content_cache: dict[str, bytes] = {}
    hashes: dict[str, str] = {}

    files = SAMPLE_FILES[:count] if count else SAMPLE_FILES

    # Group by name+size for exact duplicate handling.
    seen: dict[tuple[str, int], bytes] = {}

    for entry in files:
        name = str(entry["name"])
        size = int(entry["size"])
        key = (name, size)

        # Create subdirectories for some files to test recursive scanning.
        ext = Path(name).suffix.lower()
        if ext in (".sfc", ".smc"):
            subdir = output_dir / "SNES"
        elif ext in (".nes", ".fds"):
            subdir = output_dir / "NES"
        elif ext in (".gen", ".md"):
            subdir = output_dir / "Genesis"
        elif ext in (".gb", ".gbc"):
            subdir = output_dir / "Gameboy"
        elif ext in (".gba",):
            subdir = output_dir / "GBA"
        elif ext in (".z64", ".n64"):
            subdir = output_dir / "N64"
        elif ext in (".bin",):
            subdir = output_dir / "PS1"
        else:
            subdir = output_dir

        subdir.mkdir(parents=True, exist_ok=True)
        file_path = subdir / name

        # For exact duplicates, reuse the same content.
        if key in seen:
            content = seen[key]
        else:
            # Use a deterministic seed based on name for reproducibility.
            seed = int(hashlib.sha256(name.encode()).hexdigest()[:8], 16)
            content = generate_random_content(size, seed=seed)
            seen[key] = content

        # Handle filename conflicts in the same directory.
        if file_path.exists():
            counter = 1
            stem = file_path.stem
            ext_suffix = file_path.suffix
            while file_path.exists():
                file_path = subdir / f"{stem}_{counter}{ext_suffix}"
                counter += 1

        file_path.write_bytes(content)
        sha256 = hashlib.sha256(content).hexdigest()
        hashes[str(file_path)] = sha256
        print(f"  Created: {file_path} ({size:,} bytes, SHA-256: {sha256[:16]}...)")

    return hashes


def main() -> None:
    """Generate test ROM data."""
    parser = argparse.ArgumentParser(description="Generate test ROM data for the organizer.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/test_roms"),
        help="Directory to create test files in (default: /tmp/test_roms).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Limit the number of files to create.",
    )
    args = parser.parse_args()

    print(f"Creating test ROM data in {args.output_dir}...")
    hashes = create_test_data(args.output_dir, args.count)
    print(f"\nCreated {len(hashes)} test ROM files.")
    print(f"Unique hashes: {len(set(hashes.values()))}")
    print(f"\nTo test the organizer, run:")
    print(f"  python main.py scan {args.output_dir} --config config.json")


if __name__ == "__main__":
    main()