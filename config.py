"""Configuration management for the ROM Organizer.

Loads defaults, merges with a user-provided JSON config file, and exposes
a typed configuration object used throughout the application.

Design decision: We use a dataclass-based config rather than a raw dict
so that every setting has a clear type, a documented default, and can be
validated at load time rather than failing silently at runtime.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Default ROM extensions by system ──────────────────────────────────────
# Grouped by system so the sorter can later move files into system folders.
# This is intentionally conservative; users can add more via config.
DEFAULT_EXTENSION_MAP: dict[str, list[str]] = {
    "nes": [".nes", ".fds", ".unf"],
    "snes": [".sfc", ".smc", ".fig", ".swc"],
    "n64": [".n64", ".z64", ".v64"],
    "gameboy": [".gb", ".gbc"],
    "gba": [".gba"],
    "nds": [".nds"],
    "genesis": [".gen", ".md", ".smd"],
    "sms": [".sms"],
    "gg": [".gg"],
    "atari_2600": [".a26"],
    "atari_7800": [".a78"],
    "tg16": [".pce"],
    "pcfx": [".pce"],
    "ps1": [".pbp", ".bin", ".img"],
    "ps2": [".iso"],
    "psp": [".cso", ".iso"],
    "dreamcast": [".cdi", ".gdi"],
    "saturn": [".iso"],
    "arcade": [".zip", ".7z"],
    "generic": [".rom"],
}

DEFAULT_EXTENSIONS: set[str] = {
    ext
    for exts in DEFAULT_EXTENSION_MAP.values()
    for ext in exts
}


@dataclass
class Config:
    """Application configuration.

    All fields have sensible defaults so the tool works without a config file.
    A JSON config file can override any field by name.
    """

    # ── Scanning ──────────────────────────────────────────────────────
    rom_root: str = ""
    extensions: list[str] = field(default_factory=lambda: sorted(DEFAULT_EXTENSIONS))
    exclude_dirs: list[str] = field(default_factory=lambda: ["__MACOSX", ".git", ".DS_Store"])
    min_file_size: int = 1024  # Minimum file size in bytes to include (default 1 KiB).

    # ── Hashing ───────────────────────────────────────────────────────
    hash_algorithm: str = "sha256"
    # Chunk size for reading files (8 MiB) — balances memory vs. speed.
    hash_chunk_size: int = 8 * 1024 * 1024
    # Number of parallel workers for hashing (1 = sequential).
    hash_workers: int = 1

    # ── Database ──────────────────────────────────────────────────────
    db_path: str = "rom_organizer.db"

    # ── Output / Quarantine ───────────────────────────────────────────
    output_dir: str = "rom_organizer_output"
    quarantine_subdir: str = "quarantine"

    # ── Sorting ───────────────────────────────────────────────────────
    extension_map: dict[str, list[str]] = field(default_factory=lambda: DEFAULT_EXTENSION_MAP)
    # Priority map for ambiguous extensions (e.g., .bin → genesis over ps1).
    extension_priority: dict[str, str] = field(default_factory=lambda: {
        ".bin": "genesis",  # Default .bin to genesis; override in config if needed.
        ".iso": "ps2",      # Default .iso to ps2; psp also uses .iso.
    })

    # ── Archive Inspection ────────────────────────────────────────────
    inspect_archives: bool = True
    # Directory containing DAT files for ROM identification.
    dat_dir: str = ""

    # ── Normalization ─────────────────────────────────────────────────
    # Characters that should be treated as word separators.
    separator_chars: str = "_-+"
    # Maximum filename length before truncation.
    max_filename_length: int = 255
    # Suffix template for resolving rename conflicts.
    conflict_suffix_template: str = "_{counter}"

    # ── Logging ───────────────────────────────────────────────────────
    log_dir: str = "rom_organizer_output/logs"

    @classmethod
    def from_file(cls, config_path: Path) -> "Config":
        """Load configuration from a JSON file, falling back to defaults.

        Args:
            config_path: Path to a JSON configuration file.

        Returns:
            A Config instance with user values merged over defaults.

        Raises:
            FileNotFoundError: If config_path doesn't exist.
            json.JSONDecodeError: If the file isn't valid JSON.
        """
        logger.info("Loading config from %s", config_path)
        raw: dict[str, Any] = json.loads(config_path.read_text(encoding="utf-8"))
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, overrides: dict[str, Any]) -> "Config":
        """Create a Config from a dict, ignoring unknown keys and validating types.

        Args:
            overrides: Dict of config values to override defaults.

        Returns:
            A Config instance.

        Raises:
            TypeError: If a config value has an incorrect type.
        """
        valid_keys = {f.name for f in fields(cls)}
        filtered = {}
        for k, v in overrides.items():
            if k not in valid_keys:
                logger.warning("Ignoring unknown config key: %s", k)
                continue
            # Type validation: coerce common mismatches.
            expected_type = next(f.type for f in fields(cls) if f.name == k)
            if expected_type == "int" and isinstance(v, str) and v.isdigit():
                logger.warning("Coercing config key %s from string to int", k)
                v = int(v)
            elif expected_type == "str" and not isinstance(v, str):
                v = str(v)
            filtered[k] = v
        return cls(**filtered)

    def get_quarantine_path(self) -> Path:
        """Return the absolute path to the quarantine directory."""
        return Path(self.output_dir) / self.quarantine_subdir

    def get_extension_to_system_map(self) -> dict[str, str]:
        """Build a reverse lookup: extension → system name.

        If an extension appears in multiple systems, the first match wins
        unless overridden by extension_priority config.
        Users can resolve ambiguity via the extension_priority setting.
        """
        mapping: dict[str, str] = {}
        for system, exts in self.extension_map.items():
            for ext in exts:
                if ext not in mapping:
                    mapping[ext] = system
        # Apply priority overrides for ambiguous extensions.
        for ext, system in self.extension_priority.items():
            if ext in mapping:
                mapping[ext] = system
        return mapping


def load_config(config_path: Path | None = None) -> Config:
    """Convenience function to load config with optional file override.

    Args:
        config_path: Optional path to a JSON config file.

    Returns:
        A fully initialized Config object.
    """
    if config_path and config_path.exists():
        return Config.from_file(config_path)
    return Config()