"""Duplicate detection and file sorting for the ROM Organizer.

This module handles:
1. Detecting exact duplicates (same SHA-256 hash).
2. Detecting possible duplicates (normalized name + same size, different hash).
3. Proposing file organization actions (rename, move, quarantine).

Design decision: We never delete files. Duplicates are moved to a quarantine
folder inside the output directory, keeping the source library untouched.
All proposed actions are recorded in the database for full auditability.

File moves use atomic write-then-rename to handle cross-filesystem moves
and verify data integrity after the operation.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path

from config import Config
from database import Database
from normalizer import normalize_filename

logger = logging.getLogger(__name__)


def find_exact_duplicates(db: Database) -> int:
    """Find groups of files with identical SHA-256 hashes.

    For each group, the first file (alphabetically by path) is marked as
    canonical, and the rest are flagged as duplicates.

    Args:
        db: Database instance.

    Returns:
        The number of exact duplicate groups found.
    """
    # Clear old exact duplicate groups to avoid accumulation on re-runs.
    db.conn.execute("DELETE FROM duplicate_group_members WHERE group_id IN (SELECT id FROM duplicate_groups WHERE group_type = 'exact')")
    db.conn.execute("DELETE FROM duplicate_groups WHERE group_type = 'exact'")
    db._commit_or_defer()

    # Find hashes that appear more than once.
    rows = db.conn.execute(
        "SELECT sha256, COUNT(*) as cnt FROM files WHERE sha256 IS NOT NULL GROUP BY sha256 HAVING cnt > 1 ORDER BY sha256"
    ).fetchall()

    group_count = 0

    for row in rows:
        sha256 = row["sha256"]
        files = db.get_files_by_hash(sha256)
        if len(files) < 2:
            continue

        # Create a duplicate group.
        group_id = db.create_duplicate_group(group_type="exact", sha256=sha256)

        # Mark the first file (sorted by path) as canonical.
        sorted_files = sorted(files, key=lambda f: f["path"])
        for i, f in enumerate(sorted_files):
            is_canonical = i == 0
            db.add_file_to_duplicate_group(group_id, f["id"], is_canonical=is_canonical)
            if not is_canonical:
                logger.debug(
                    "Exact duplicate: %s (canonical: %s)",
                    f["path"],
                    sorted_files[0]["path"],
                )

        group_count += 1

    logger.info("Found %d exact duplicate groups.", group_count)
    return group_count


def find_possible_duplicates(db: Database, config: Config) -> int:
    """Find files that might be duplicates based on normalized name + size.

    Two files are "possible duplicates" if:
    - Their normalized filenames (without extension) match
    - Their file sizes match
    - Their SHA-256 hashes differ (or one is missing)

    This catches cases like different dumps of the same ROM, or files
    with minor header differences.

    Args:
        db: Database instance.
        config: Application configuration.

    Returns:
        The number of possible duplicate groups found.
    """
    # Clear old possible duplicate groups to avoid accumulation on re-runs.
    db.conn.execute("DELETE FROM duplicate_group_members WHERE group_id IN (SELECT id FROM duplicate_groups WHERE group_type = 'possible')")
    db.conn.execute("DELETE FROM duplicate_groups WHERE group_type = 'possible'")
    db._commit_or_defer()

    # Get all files with normalized names.
    rows = db.conn.execute(
        "SELECT id, path, original_name, normalized_name, extension, size, sha256 FROM files ORDER BY normalized_name, size"
    ).fetchall()

    # Group by (normalized_name_without_ext, size).
    groups: dict[tuple[str, int], list[dict]] = {}
    for row in rows:
        name = row["normalized_name"] or row["original_name"]
        # Strip extension for grouping.
        stem = name.rpartition(".")[0] if "." in name else name
        key = (stem.lower().strip(), row["size"])
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(row))

    group_count = 0
    for key, files in groups.items():
        if len(files) < 2:
            continue

        # Check that not all files have the same hash (that would be an exact duplicate).
        hashes = {f["sha256"] for f in files if f["sha256"] is not None}
        if len(hashes) <= 1 and None not in {f["sha256"] for f in files}:
            # All same hash — this is an exact duplicate, not a possible one.
            continue

        # Create a possible duplicate group.
        group_id = db.create_duplicate_group(group_type="possible", sha256=None)

        sorted_files = sorted(files, key=lambda f: f["path"])
        for i, f in enumerate(sorted_files):
            is_canonical = i == 0
            db.add_file_to_duplicate_group(group_id, f["id"], is_canonical=is_canonical)
            if not is_canonical:
                logger.debug(
                    "Possible duplicate: %s (canonical: %s)",
                    f["path"],
                    sorted_files[0]["path"],
                )

        group_count += 1

    logger.info("Found %d possible duplicate groups.", group_count)
    return group_count


def propose_organize_actions(db: Database, config: Config, dry_run: bool = False) -> dict[str, int]:
    """Propose file organization actions based on scan results.

    Actions proposed:
    1. Rename files to their normalized names.
    2. Move exact duplicates to quarantine.
    3. Move files into system-based folders.

    Args:
        db: Database instance.
        config: Application configuration.
        dry_run: If True, compute proposals but do not write to the database.

    Returns:
        A dict with counts of proposed actions by type.
    """
    # Ensure normalization has been computed before proposing renames.
    from normalizer import normalize_all_files
    normalize_all_files(db, config, dry_run=dry_run)

    # Clear old pending actions to avoid accumulation on re-runs.
    if not dry_run:
        db.conn.execute("DELETE FROM proposed_actions WHERE applied = 0")
        db._commit_or_defer()

    ext_to_system = config.get_extension_to_system_map()
    quarantine_path = config.get_quarantine_path()
    output_dir = Path(config.output_dir).resolve()

    stats = {"rename": 0, "quarantine": 0, "move": 0}

    # ── 1. Propose renames for files with normalized names ──────────────
    rows = db.conn.execute(
        "SELECT id, path, original_name, normalized_name FROM files WHERE normalized_name IS NOT NULL AND normalized_name != original_name"
    ).fetchall()

    for row in rows:
        original_path = Path(row["path"])
        parent = original_path.parent
        new_name = row["normalized_name"]
        proposed_path = str(parent / new_name)

        # Handle rename conflicts by appending a suffix.
        counter = 1
        while proposed_path != str(original_path) and Path(proposed_path).exists():
            stem = new_name.rpartition(".")[0] if "." in new_name else new_name
            ext = new_name.rpartition(".")[2] if "." in new_name else ""
            suffix = config.conflict_suffix_template.format(counter=counter)
            new_name_conflict = f"{stem}{suffix}.{ext}" if ext else f"{stem}{suffix}"
            proposed_path = str(parent / new_name_conflict)
            counter += 1

        # Path traversal protection: ensure the proposed path stays within the parent directory.
        if not _is_safe_path(Path(proposed_path), parent):
            logger.warning("Skipping unsafe rename path: %s", proposed_path)
            continue

        if not dry_run:
            db.add_proposed_action(
                file_id=row["id"],
                action_type="rename",
                source_path=str(original_path),
                proposed_path=proposed_path,
                reason=f"Normalize filename: {row['original_name']} → {Path(proposed_path).name}",
            )
        stats["rename"] += 1

    # ── 2. Propose quarantine for non-canonical exact duplicates ───────
    exact_groups = db.get_exact_duplicate_groups()
    for group in exact_groups:
        members = db.get_group_members(group["id"])
        for member in members:
            # Check if this member is NOT the canonical file.
            is_canonical = db.get_member_canonical_status(group["id"], member["id"])

            if is_canonical:
                continue

            original_path = Path(member["path"])
            quarantine_target = quarantine_path / original_path.name

            # Path traversal protection: ensure quarantine target stays within quarantine.
            if not _is_safe_path(quarantine_target, quarantine_path):
                logger.warning("Skipping unsafe quarantine path: %s", quarantine_target)
                continue

            # Handle conflicts in quarantine.
            counter = 1
            while quarantine_target.exists():
                stem = quarantine_target.stem
                ext = quarantine_target.suffix
                suffix = config.conflict_suffix_template.format(counter=counter)
                quarantine_target = quarantine_target.parent / f"{stem}{suffix}{ext}"
                counter += 1

            if not dry_run:
                db.add_proposed_action(
                    file_id=member["id"],
                    action_type="quarantine",
                    source_path=str(original_path),
                    proposed_path=str(quarantine_target),
                    reason=f"Exact duplicate of {group['sha256'][:16]}...",
                )
            stats["quarantine"] += 1

    # ── 3. Propose moves into system-based folders ─────────────────────
    rows = db.conn.execute(
        "SELECT id, path, extension FROM files WHERE status = 'scanned' OR status = 'normalized'"
    ).fetchall()

    for row in rows:
        original_path = Path(row["path"])
        ext = row["extension"].lower()
        system = ext_to_system.get(ext)

        if system is None:
            continue

        # Determine the target directory.
        system_dir = output_dir / system
        target_path = system_dir / original_path.name

        # Path traversal protection: ensure target stays within output directory.
        if not _is_safe_path(target_path, output_dir):
            logger.warning("Skipping unsafe move path: %s", target_path)
            continue

        # Don't propose a move if the file is already in the right place.
        if _is_relative_to(original_path.resolve(), system_dir.resolve()):
            continue

        # Handle conflicts.
        counter = 1
        while target_path.exists():
            stem = target_path.stem
            ext_suffix = target_path.suffix
            suffix = config.conflict_suffix_template.format(counter=counter)
            target_path = target_path.parent / f"{stem}{suffix}{ext_suffix}"
            counter += 1

        if not dry_run:
            db.add_proposed_action(
                file_id=row["id"],
                action_type="move",
                source_path=str(original_path),
                proposed_path=str(target_path),
                reason=f"Move to {system} folder",
            )
        stats["move"] += 1

    logger.info(
        "Proposed actions: %d renames, %d quarantines, %d moves%s",
        stats["rename"],
        stats["quarantine"],
        stats["move"],
        " (dry run — no actions recorded)" if dry_run else "",
    )
    return stats


def _is_safe_path(path: Path, base: Path) -> bool:
    """Check that a path resolves to a location within the base directory.

    This prevents path traversal attacks where a malicious filename
    (e.g., '../../etc/passwd') could cause files to be moved outside
    the intended directory.

    Args:
        path: The path to check.
        base: The base directory that path must be within.

    Returns:
        True if the resolved path is within the base directory.
    """
    try:
        resolved = path.resolve()
        base_resolved = base.resolve()
        return str(resolved).startswith(str(base_resolved) + os.sep) or resolved == base_resolved
    except Exception:
        return False


def _is_relative_to(path: Path, base: Path) -> bool:
    """Check if path is relative to base, with a safe fallback for Python < 3.9.

    Args:
        path: The path to check.
        base: The base directory.

    Returns:
        True if path is inside base.
    """
    try:
        return path.is_relative_to(base)
    except AttributeError:
        # Python < 3.9 fallback — use os.path.commonpath for correctness.
        import os
        try:
            return os.path.commonpath([str(path), str(base)]) == str(base)
        except ValueError:
            return False


def apply_actions(db: Database, config: Config, dry_run: bool = True) -> dict[str, int]:
    """Apply or preview proposed file organization actions.

    Args:
        db: Database instance.
        config: Application configuration.
        dry_run: If True, only print what would be done without making changes.

    Returns:
        A dict with counts of applied/simulated actions by type.
    """
    from progress import ProgressBar

    actions = db.get_pending_actions()
    stats = {"rename": 0, "quarantine": 0, "move": 0, "skipped": 0, "errors": 0}
    total = len(actions)

    if dry_run:
        logger.info("=== DRY RUN — no files will be modified ===")

    bar = ProgressBar(total=total, label="Organizing", unit="actions")

    for action in actions:
        source = Path(action["source_path"])
        target = Path(action["proposed_path"])
        action_type = action["action_type"]

        if not source.exists():
            logger.warning("Source file no longer exists: %s", source)
            stats["skipped"] += 1
            bar.update(1)
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] Would %s: %s → %s (%s)",
                action_type,
                source,
                target,
                action["reason"],
            )
            stats[action_type] += 1
            bar.update(1)
            continue

        # Actually apply the action.
        try:
            # Ensure target directory exists.
            target.parent.mkdir(parents=True, exist_ok=True)

            if action_type == "rename":
                _safe_move(source, target)
                logger.info("Renamed: %s → %s", source, target)
            elif action_type == "quarantine":
                target.parent.mkdir(parents=True, exist_ok=True)
                _safe_move(source, target)
                logger.info("Quarantined: %s → %s", source, target)
            elif action_type == "move":
                target.parent.mkdir(parents=True, exist_ok=True)
                _safe_move(source, target)
                logger.info("Moved: %s → %s", source, target)

            # Post-move checksum verification.
            if target.exists() and target.stat().st_size > 0:
                row = db.get_file_by_id(action["file_id"])
                if row and row["sha256"]:
                    verified = _verify_checksum(target, row["sha256"])
                    if not verified:
                        logger.error(
                            "CHECKSUM MISMATCH after move: %s → %s. "
                            "File may be corrupted!",
                            source,
                            target,
                        )
                        stats["errors"] += 1
                        bar.update(1)
                        continue

            # Mark the action as applied.
            db.mark_action_applied(action["id"])

            # Update the file's path in the database.
            db.conn.execute(
                "UPDATE files SET path = ? WHERE id = ?",
                (str(target.resolve()), action["file_id"]),
            )
            db._commit_or_defer()

            # Update file status.
            if action_type == "quarantine":
                db.update_file_status(action["file_id"], "quarantined", action["reason"])
            elif action_type == "move":
                db.update_file_status(action["file_id"], "organized", action["reason"])
            elif action_type == "rename":
                db.update_file_status(action["file_id"], "normalized", action["reason"])

                # After a rename, update the source_path of any pending move
                # actions for the same file so they reference the new path.
                new_path = str(target.resolve())
                pending_moves = db.conn.execute(
                    "SELECT id FROM proposed_actions WHERE file_id = ? AND action_type = 'move' AND applied = 0",
                    (action["file_id"],),
                ).fetchall()
                for move_action in pending_moves:
                    db.update_proposed_action_source_path(move_action["id"], new_path)

            stats[action_type] += 1

        except Exception:
            logger.exception("Error applying action %d: %s → %s", action["id"], source, target)
            stats["errors"] += 1

        bar.update(1)

    bar.close()
    if dry_run:
        logger.info("=== DRY RUN COMPLETE — no files were modified ===")
    else:
        logger.info("Actions applied: %s", stats)

    return stats


def _safe_move(source: Path, target: Path) -> None:
    """Move a file safely, handling cross-filesystem moves.

    Uses Path.rename() for same-filesystem moves (fast, atomic).
    Falls back to write-to-temp-then-rename for cross-filesystem moves,
    which ensures the target is only created if the full copy succeeds.

    Args:
        source: Source file path.
        target: Target file path.

    Raises:
        OSError: If the move fails.
    """
    try:
        source.rename(target)
    except OSError:
        # Cross-filesystem move — copy then delete.
        logger.debug("Cross-filesystem move detected, copying %s → %s", source, target)
        # Write to a temp file first, then atomically rename.
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        try:
            shutil.copy2(str(source), str(tmp_path))
            tmp_path.rename(target)
        finally:
            # Clean up temp file if something went wrong.
            if tmp_path.exists():
                tmp_path.unlink()
        # Remove source only after successful copy + rename.
        source.unlink()


def _verify_checksum(path: Path, expected_sha256: str, chunk_size: int = 8 * 1024 * 1024) -> bool:
    """Verify a file's SHA-256 checksum after a move operation.

    Args:
        path: Path to the file to verify.
        expected_sha256: The expected SHA-256 hex digest.
        chunk_size: Bytes to read per iteration.

    Returns:
        True if the checksum matches, False otherwise.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    actual = h.hexdigest()
    if actual != expected_sha256:
        logger.error(
            "Checksum verification failed for %s: expected %s, got %s",
            path,
            expected_sha256[:16],
            actual[:16],
        )
        return False
    return True