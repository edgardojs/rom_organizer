"""Archive inspection module for the ROM Organizer.

Inspects .zip and .7z archives to catalog their contents, hash individual
files inside, and detect duplicates at the sub-file level.

This is critical for arcade ROMs (MAME, FBNeo) which are distributed as
.zip archives containing multiple small ROM binaries. Without inspection,
two archives with the same game but different file ordering produce
different hashes, causing false non-duplicates.

Design decision: We use only stdlib for .zip (zipfile) and shell out to
7z for .7z files since there's no stdlib 7z support. All sub-file hashes
are stored in the database for duplicate detection.
"""

from __future__ import annotations

import hashlib
import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Archive extensions we can inspect.
INSPECTABLE_EXTENSIONS = {".zip", ".7z"}


@dataclass
class ArchiveEntry:
    """A single file inside an archive."""

    name: str
    size: int
    crc32: Optional[int] = None
    sha256: Optional[str] = None
    compressed_size: int = 0
    is_directory: bool = False


@dataclass
class ArchiveInspection:
    """Result of inspecting an archive."""

    archive_path: str
    archive_type: str  # "zip" or "7z"
    entries: list[ArchiveEntry] = field(default_factory=list)
    total_uncompressed_size: int = 0
    total_compressed_size: int = 0
    error: Optional[str] = None


def inspect_zip(path: Path) -> ArchiveInspection:
    """Inspect a .zip archive and catalog its contents.

    Args:
        path: Path to the .zip file.

    Returns:
        An ArchiveInspection with entry details.
    """
    result = ArchiveInspection(
        archive_path=str(path),
        archive_type="zip",
    )

    try:
        with zipfile.ZipFile(path, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue

                entry = ArchiveEntry(
                    name=info.filename,
                    size=info.file_size,
                    crc32=info.CRC,
                    compressed_size=info.compress_size,
                    is_directory=info.is_dir(),
                )

                # Hash the entry contents for deduplication.
                # Only hash files under 64 MiB to avoid memory issues
                # with very large archives.
                if info.file_size <= 64 * 1024 * 1024:
                    try:
                        with zf.open(info) as f:
                            h = hashlib.sha256()
                            while True:
                                chunk = f.read(8 * 1024 * 1024)
                                if not chunk:
                                    break
                                h.update(chunk)
                            entry.sha256 = h.hexdigest()
                    except Exception:
                        logger.debug("Could not hash entry %s in %s", info.filename, path)

                result.entries.append(entry)
                result.total_uncompressed_size += info.file_size
                result.total_compressed_size += info.compress_size

    except zipfile.BadZipFile:
        result.error = "Bad or corrupted zip file"
        logger.warning("Bad zip file: %s", path)
    except Exception as exc:
        result.error = str(exc)
        logger.warning("Error inspecting zip %s: %s", path, exc)

    return result


def inspect_7z(path: Path) -> ArchiveInspection:
    """Inspect a .7z archive by listing its contents via the 7z CLI.

    Requires the ``7z`` command to be available on the system.

    Args:
        path: Path to the .7z file.

    Returns:
        An ArchiveInspection with entry details.
    """
    import subprocess

    result = ArchiveInspection(
        archive_path=str(path),
        archive_type="7z",
    )

    try:
        proc = subprocess.run(
            ["7z", "l", "-slt", str(path)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode != 0:
            result.error = f"7z exited with code {proc.returncode}"
            logger.warning("7z error for %s: %s", path, proc.stderr.strip())
            return result

        # Parse 7z's "technical" listing format.
        # Each file block starts with "Path = ..." and has key = value lines.
        current_entry: dict[str, str] = {}
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                if current_entry and current_entry.get("Folder", "+") != "+":
                    # This is a file entry, not a directory.
                    entry = ArchiveEntry(
                        name=current_entry.get("Path", ""),
                        size=int(current_entry.get("Size", "0")),
                        compressed_size=int(current_entry.get("Packed Size", "0") or "0"),
                        crc32=int(current_entry.get("CRC", "0"), 16) if current_entry.get("CRC") else None,
                    )
                    result.entries.append(entry)
                    result.total_uncompressed_size += entry.size
                    result.total_compressed_size += entry.compressed_size
                current_entry = {}
                continue

            if "=" in line:
                key, _, value = line.partition("=")
                current_entry[key.strip()] = value.strip()

        # Handle last entry.
        if current_entry and current_entry.get("Folder", "+") != "+":
            entry = ArchiveEntry(
                name=current_entry.get("Path", ""),
                size=int(current_entry.get("Size", "0")),
                compressed_size=int(current_entry.get("Packed Size", "0") or "0"),
                crc32=int(current_entry.get("CRC", "0"), 16) if current_entry.get("CRC") else None,
            )
            result.entries.append(entry)
            result.total_uncompressed_size += entry.size
            result.total_compressed_size += entry.compressed_size

    except FileNotFoundError:
        result.error = "7z command not found — install p7zip-full"
        logger.warning("7z not found on system; cannot inspect .7z files")
    except subprocess.TimeoutExpired:
        result.error = "7z timed out"
        logger.warning("7z timed out inspecting %s", path)
    except Exception as exc:
        result.error = str(exc)
        logger.warning("Error inspecting 7z %s: %s", path, exc)

    return result


def inspect_archive(path: Path) -> ArchiveInspection:
    """Inspect an archive file and return its contents.

    Dispatches to the appropriate inspector based on extension.

    Args:
        path: Path to the archive file.

    Returns:
        An ArchiveInspection with entry details.
    """
    ext = path.suffix.lower()
    if ext == ".zip":
        return inspect_zip(path)
    elif ext == ".7z":
        return inspect_7z(path)
    else:
        result = ArchiveInspection(
            archive_path=str(path),
            archive_type="unknown",
            error=f"Unsupported archive type: {ext}",
        )
        return result


def compute_archive_fingerprint(inspection: ArchiveInspection) -> str:
    """Compute a deterministic fingerprint for an archive based on its contents.

    Unlike hashing the archive file itself (which varies with compression
    and file ordering), this fingerprint is based on the sorted set of
    (name, sha256) pairs of the entries. Two archives containing the same
    files in different order will produce the same fingerprint.

    Args:
        inspection: The archive inspection result.

    Returns:
        A hex digest fingerprint string.
    """
    h = hashlib.sha256()
    # Sort entries by name for determinism.
    for entry in sorted(inspection.entries, key=lambda e: e.name):
        h.update(entry.name.encode("utf-8"))
        h.update(entry.sha256.encode("utf-8") if entry.sha256 else b"")
        h.update(str(entry.size).encode("utf-8"))
        if entry.crc32 is not None:
            h.update(str(entry.crc32).encode("utf-8"))
    return h.hexdigest()