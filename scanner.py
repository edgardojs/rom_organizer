"""File system scanner for the ROM Organizer.

Recursively walks a root directory, identifies ROM files by extension,
and records metadata (path, name, size, extension) into the database.

For archive files (.zip, .7z), also inspects their contents and records
individual entries for sub-file duplicate detection.

Design decision: The scanner is decoupled from hashing. It records file
metadata first, and the hasher runs as a separate pass. This lets us
report progress incrementally and makes it easy to re-hash files without
re-scanning. A pre-scan estimate gives the user immediate feedback on
the size of their library before the real work begins.
"""

from __future__ import annotations

import logging
from pathlib import Path

from config import Config
from database import Database
from progress import ProgressBar, estimate_folder

logger = logging.getLogger(__name__)


def scan_directory(
    root: Path,
    config: Config,
    db: Database,
) -> int:
    """Recursively scan a directory for ROM files and insert them into the DB.

    Performs a quick pre-scan estimate first so the user sees the library
    size immediately, then walks the directory recording each file with a
    progress bar.

    Args:
        root: The root directory to scan.
        config: Application configuration.
        db: Database instance.

    Returns:
        The number of ROM files found.
    """
    if not root.exists():
        logger.error("Root directory does not exist: %s", root)
        return 0

    if not root.is_dir():
        logger.error("Root path is not a directory: %s", root)
        return 0

    extensions = {e.lower() for e in config.extensions}
    exclude_dirs = set(config.exclude_dirs)
    inspectable = {".zip", ".7z"}
    count = 0

    # Pre-scan estimate: count files and total size before the real work.
    logger.info("Estimating library size in %s ...", root)
    est = estimate_folder(root, extensions, exclude_dirs, config.min_file_size)
    logger.info(
        "Library estimate: %s (took %s to estimate)",
        est,
        f"{est.elapsed:.1f}s",
    )

    bar = ProgressBar(total=est.total_files, label="Scanning", unit="files")

    # Use a transaction for the entire scan to batch DB writes.
    with db.transaction():
        for path in root.rglob("*"):
            # Skip excluded directories.
            if any(part in exclude_dirs for part in path.parts):
                continue

            if not path.is_file():
                continue

            ext = path.suffix.lower()
            if ext not in extensions:
                continue

            # Skip hidden files (dot-prefix).
            if path.name.startswith("."):
                logger.debug("Skipping hidden file: %s", path)
                continue

            # Skip very small files — likely metadata or artifacts.
            size = path.stat().st_size
            if size < config.min_file_size:
                logger.debug("Skipping tiny file (%d bytes): %s", size, path)
                continue

            try:
                is_archive = ext in inspectable
                file_id = db.upsert_file(
                    path=str(path.resolve()),
                    original_name=path.name,
                    extension=ext,
                    size=size,
                )

                # Inspect archive contents if enabled.
                if is_archive and getattr(config, "inspect_archives", True):
                    _inspect_and_record(path, file_id, db)

                logger.debug("Found ROM: %s (%d bytes, id=%d)", path.name, size, file_id)
                count += 1
                bar.update(1)
            except Exception as exc:
                logger.exception("Failed to record file: %s", path)
                # Try to record the file with an error status so it shows up in reports.
                try:
                    file_id = db.upsert_file(
                        path=str(path.resolve()),
                        original_name=path.name,
                        extension=ext,
                        size=size,
                    )
                    db.update_file_status(file_id, "scan_error", str(exc))
                except Exception:
                    pass  # If even the basic insert fails, we can only log.
                bar.update(1)
                continue

    bar.close()
    logger.info("Scan complete: %d ROM files found.", count)
    return count


def _inspect_and_record(path: Path, file_id: int, db: Database) -> None:
    """Inspect an archive file and record its entries in the database.

    Args:
        path: Path to the archive file.
        file_id: The database row ID of the archive file.
        db: Database instance.
    """
    from archiver import inspect_archive, compute_archive_fingerprint, INSPECTABLE_EXTENSIONS

    ext = path.suffix.lower()
    if ext not in INSPECTABLE_EXTENSIONS:
        return

    inspection = inspect_archive(path)

    if inspection.error:
        logger.warning("Archive inspection failed for %s: %s", path.name, inspection.error)
        db.update_file_status(file_id, "scan_error", f"Archive inspection failed: {inspection.error}")
        return

    # Mark the file as an archive and store its content fingerprint.
    fingerprint = compute_archive_fingerprint(inspection)
    db.mark_file_as_archive(file_id, fingerprint=fingerprint)

    # Record each entry.
    for entry in inspection.entries:
        db.add_archive_entry(
            file_id=file_id,
            entry_name=entry.name,
            entry_size=entry.size,
            compressed_size=entry.compressed_size,
            crc32=f"{entry.crc32:08x}" if entry.crc32 is not None else None,
            sha256=entry.sha256,
        )

    logger.debug(
        "Archive %s: %d entries, fingerprint=%s",
        path.name,
        len(inspection.entries),
        fingerprint[:16],
    )


def get_rom_files(root: Path, config: Config) -> list[Path]:
    """Return a list of ROM file paths without inserting into the database.

    Useful for previewing what a scan would find.

    Args:
        root: The root directory to scan.
        config: Application configuration.

    Returns:
        A sorted list of Path objects for ROM files.
    """
    if not root.exists() or not root.is_dir():
        return []

    extensions = {e.lower() for e in config.extensions}
    exclude_dirs = set(config.exclude_dirs)
    results: list[Path] = []

    for path in root.rglob("*"):
        if any(part in exclude_dirs for part in path.parts):
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() not in extensions:
            continue
        if path.name.startswith("."):
            continue
        if path.stat().st_size < config.min_file_size:
            continue
        results.append(path)

    return sorted(results)