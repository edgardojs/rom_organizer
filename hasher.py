"""File hashing module for the ROM Organizer.

Computes SHA-256 (or other configured algorithm) hashes for ROM files.
Reads files in configurable chunks to handle large ROMs without excessive
memory usage.

Supports parallel hashing via ProcessPoolExecutor for large libraries.

Design decision: Hashing is a separate pass from scanning so we can
report progress and handle I/O errors gracefully without losing scan data.
A progress bar shows real-time status with ETA for large libraries.
"""

from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from config import Config
from database import Database
from progress import ProgressBar, _format_size

logger = logging.getLogger(__name__)


def hash_file(path: Path, algorithm: str = "sha256", chunk_size: int = 8 * 1024 * 1024) -> str:
    """Compute the hash of a file using the specified algorithm.

    Reads the file in chunks to avoid loading large ROMs entirely into memory.

    Args:
        path: Path to the file to hash.
        algorithm: Hash algorithm name (must be available in hashlib).
        chunk_size: Number of bytes to read per iteration.

    Returns:
        The hex digest of the file hash.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ValueError: If the algorithm is not supported.
    """
    try:
        h = hashlib.new(algorithm)
    except ValueError:
        logger.error("Unsupported hash algorithm: %s", algorithm)
        raise

    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def _hash_file_worker(args: tuple) -> tuple[int, str, str] | tuple[int, str, None]:
    """Worker function for parallel hashing.

    Args:
        args: Tuple of (row_id, path_str, algorithm, chunk_size).

    Returns:
        Tuple of (row_id, path_str, digest) or (row_id, path_str, None) on error.
    """
    row_id, path_str, algorithm, chunk_size = args
    try:
        path = Path(path_str)
        if not path.exists():
            return (row_id, path_str, None)
        digest = hash_file(path, algorithm, chunk_size)
        return (row_id, path_str, digest)
    except Exception:
        return (row_id, path_str, None)


def hash_all_files(db: Database, config: Config) -> dict[str, int]:
    """Hash all ROM files in the database that don't yet have a hash.

    Uses parallel processing when hash_workers > 1 for better performance
    on large libraries. Shows a progress bar with ETA.

    Args:
        db: Database instance.
        config: Application configuration.

    Returns:
        A dict with 'hashed', 'skipped', and 'errors' counts.
    """
    rows = db.conn.execute(
        "SELECT id, path, size FROM files WHERE sha256 IS NULL ORDER BY path"
    ).fetchall()

    stats = {"hashed": 0, "skipped": 0, "errors": 0}
    total = len(rows)

    if total == 0:
        logger.info("No files need hashing.")
        return stats

    total_bytes = sum(row["size"] for row in rows)
    logger.info(
        "Hashing %d files (%s) ...",
        total,
        _format_size(total_bytes),
    )

    bar = ProgressBar(total=total, label="Hashing", unit="files")
    workers = getattr(config, "hash_workers", 1)

    if workers > 1:
        # Parallel hashing.
        logger.info("Using %d workers for parallel hashing.", workers)
        work_items = [
            (row["id"], row["path"], config.hash_algorithm, config.hash_chunk_size)
            for row in rows
        ]

        with db.transaction():
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_hash_file_worker, item): item[0] for item in work_items}
                for future in as_completed(futures):
                    try:
                        row_id, path_str, digest = future.result()
                        if digest is None:
                            logger.warning("File no longer exists or error, skipping: %s", path_str)
                            db.update_file_status(row_id, "hash_error", "File missing or unreadable")
                            stats["skipped"] += 1
                        else:
                            db.update_file_hash(row_id, digest)
                            stats["hashed"] += 1
                            logger.debug("Hashed %s → %s", Path(path_str).name, digest[:16])
                    except Exception as exc:
                        logger.exception("Error hashing file")
                        stats["errors"] += 1

                    bar.update(1)
    else:
        # Sequential hashing (original behavior).
        for row in rows:
            file_path = Path(row["path"])

            if not file_path.exists():
                logger.warning("File no longer exists, skipping: %s", file_path)
                db.update_file_status(row["id"], "hash_error", "File missing")
                stats["skipped"] += 1
                bar.update(1)
                continue

            try:
                digest = hash_file(file_path, config.hash_algorithm, config.hash_chunk_size)
                db.update_file_hash(row["id"], digest)
                stats["hashed"] += 1
                logger.debug("Hashed %s → %s", file_path.name, digest[:16])
            except Exception as exc:
                logger.exception("Error hashing file: %s", file_path)
                db.update_file_status(row["id"], "hash_error", str(exc))
                stats["errors"] += 1

            bar.update(1)

    bar.close()
    logger.info(
        "Hashing complete: %d hashed, %d skipped, %d errors",
        stats["hashed"],
        stats["skipped"],
        stats["errors"],
    )
    return stats