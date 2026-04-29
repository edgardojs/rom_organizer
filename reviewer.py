"""Interactive review module for the ROM Organizer.

Walks through the scan report findings and lets the user approve or reject
each action before it's applied. This is the safe, interactive way to act
on the report.

Design decision: We never delete files — even corrupted ones are moved to
a "corrupted" subfolder, not deleted. All actions are recorded in the
database for full auditability and rollback.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from config import Config
from database import Database

logger = logging.getLogger(__name__)


def _prompt_choice(prompt: str, choices: list[str]) -> str:
    """Ask the user to choose from a list of options.

    Args:
        prompt: The question to ask.
        choices: List of valid choices (single-char shortcuts).

    Returns:
        The chosen option.
    """
    while True:
        try:
            answer = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return "q"
        if answer in choices:
            return answer
        print(f"Please choose one of: {', '.join(choices)}")


def review_corrupted(
    db: Database,
    config: Config,
    dry_run: bool = True,
    batch_mode: str = "",
) -> dict[str, int]:
    """Review and handle corrupted/error files interactively.

    Shows each file with a scan or hash error and asks the user what to do.
    Options: move to corrupted folder, skip, or quit.

    Args:
        db: Database instance.
        config: Application configuration.
        dry_run: If True, only show what would be done.
        batch_mode: If set, auto-approve ("move") or auto-skip ("skip")
            all items without prompting. Empty string = interactive.

    Returns:
        A dict with 'moved', 'skipped', 'errors' counts.
    """
    error_files = db.get_error_files()
    if not error_files:
        print("\n✓ No corrupted or error files found.")
        return {"moved": 0, "skipped": 0, "errors": 0}

    stats = {"moved": 0, "skipped": 0, "errors": 0}
    corrupted_dir = Path(config.output_dir) / "corrupted"

    print(f"\n⚠ Found {len(error_files)} corrupted/error file(s):")
    if not batch_mode:
        print("  Actions: [m]ove to corrupted folder, [s]kip, [M]ove all, [S]kip all, [q]uit")
    print()

    move_all = batch_mode == "move"
    skip_all = batch_mode == "skip"

    for f in error_files:
        path = Path(f["path"])
        status = f["status"]
        notes = f["notes"] or "unknown error"

        if move_all:
            choice = "m"
        elif skip_all:
            choice = "s"
        else:
            print(f"  ⚠ [{status.upper().replace('_', ' ')}] {path.name}")
            print(f"    Path: {path}")
            print(f"    Error: {notes}")
            choice = _prompt_choice(
                "    Move to corrupted folder? [m]ove / [s]kip / [M]ove all / [S]kip all / [q]uit: ",
                ["m", "s", "M", "S", "q"],
            )

        if choice == "q":
            print("  Stopping review.")
            break
        elif choice in ("s", "S"):
            if choice == "S":
                skip_all = True
            print(f"    Skipping: {path.name}")
            stats["skipped"] += 1
            continue
        elif choice in ("m", "M"):
            if choice == "M":
                move_all = True

            if dry_run:
                print(f"    [DRY RUN] Would move: {path.name} → {corrupted_dir / path.name}")
                stats["moved"] += 1
                continue

            if not path.exists():
                print(f"    ⚠ File no longer exists: {path}")
                stats["errors"] += 1
                continue

            try:
                corrupted_dir.mkdir(parents=True, exist_ok=True)
                target = corrupted_dir / path.name

                # Handle name conflicts in corrupted dir.
                counter = 1
                while target.exists():
                    stem = path.stem
                    suffix = path.suffix
                    target = corrupted_dir / f"{stem}_{counter}{suffix}"
                    counter += 1

                shutil.move(str(path), str(target))
                logger.info("Moved corrupted file: %s → %s", path, target)

                # Update DB path and status.
                db.conn.execute(
                    "UPDATE files SET path = ?, status = 'corrupted' WHERE id = ?",
                    (str(target.resolve()), f["id"]),
                )
                db._commit_or_defer()
                stats["moved"] += 1
            except Exception as exc:
                logger.exception("Error moving corrupted file: %s", path)
                print(f"    ✗ Error: {exc}")
                stats["errors"] += 1

    print(f"\n  Corrupted files: {stats['moved']} moved, {stats['skipped']} skipped, {stats['errors']} errors")
    return stats


def review_duplicates(
    db: Database,
    config: Config,
    dry_run: bool = True,
    batch_mode: str = "",
) -> dict[str, int]:
    """Review and handle duplicate files interactively.

    Shows each duplicate group and asks the user to approve quarantine
    of the non-canonical files.

    Args:
        db: Database instance.
        config: Application configuration.
        dry_run: If True, only show what would be done.
        batch_mode: If set, auto-approve ("quarantine") or auto-skip ("skip")
            all items without prompting. Empty string = interactive.

    Returns:
        A dict with 'quarantined', 'kept', 'errors' counts.
    """
    exact_groups = db.get_exact_duplicate_groups()
    if not exact_groups:
        print("\n✓ No exact duplicates found.")
        return {"quarantined": 0, "kept": 0, "errors": 0}

    stats = {"quarantined": 0, "kept": 0, "errors": 0}
    quarantine_path = config.get_quarantine_path()

    print(f"\n📋 Found {len(exact_groups)} exact duplicate group(s):")
    if not batch_mode:
        print("  Actions: [q]uarantine duplicates, [k]eep all, [Q]uarantine all, [K]eep all, [s]kip group")
    print()

    quarantine_all = batch_mode == "quarantine"
    keep_all = batch_mode == "skip"

    for group in exact_groups:
        members = db.get_group_members(group["id"])
        sha_short = group["sha256"][:16] if group["sha256"] else "N/A"

        if quarantine_all:
            choice = "q"
        elif keep_all:
            choice = "k"
        else:
            print(f"  Group {group['id']} — Hash: {sha_short}...")
            for member in members:
                is_canonical = db.get_member_canonical_status(group["id"], member["id"])
                marker = "★" if is_canonical else " "
                print(f"    {marker} {member['path']} ({member['size']:,} bytes)")
            choice = _prompt_choice(
                "    Quarantine duplicates? [q]uarantine / [k]eep all / [Q]uarantine all / [K]eep all / [s]kip: ",
                ["q", "k", "Q", "K", "s"],
            )

        if choice == "s":
            continue
        elif choice in ("k", "K"):
            if choice == "K":
                keep_all = True
            print("  Keeping all files in this group.")
            stats["kept"] += 1
            continue
        elif choice in ("q", "Q"):
            if choice == "Q":
                quarantine_all = True

            # Quarantine non-canonical members.
            for member in members:
                is_canonical = db.get_member_canonical_status(group["id"], member["id"])
                if is_canonical:
                    continue

                source = Path(member["path"])
                target = quarantine_path / source.name

                if dry_run:
                    print(f"    [DRY RUN] Would quarantine: {source.name} → {target}")
                    stats["quarantined"] += 1
                    continue

                if not source.exists():
                    print(f"    ⚠ File no longer exists: {source}")
                    stats["errors"] += 1
                    continue

                try:
                    quarantine_path.mkdir(parents=True, exist_ok=True)

                    # Handle name conflicts.
                    counter = 1
                    while target.exists():
                        stem = source.stem
                        suffix = source.suffix
                        target = quarantine_path / f"{stem}_{counter}{suffix}"
                        counter += 1

                    shutil.move(str(source), str(target))
                    logger.info("Quarantined: %s → %s", source, target)

                    # Update DB.
                    db.conn.execute(
                        "UPDATE files SET path = ?, status = 'quarantined' WHERE id = ?",
                        (str(target.resolve()), member["id"]),
                    )
                    db._commit_or_defer()
                    stats["quarantined"] += 1
                except Exception as exc:
                    logger.exception("Error quarantining: %s", source)
                    print(f"    ✗ Error: {exc}")
                    stats["errors"] += 1

    print(f"\n  Duplicates: {stats['quarantined']} quarantined, {stats['kept']} kept, {stats['errors']} errors")
    return stats


def review_actions(
    db: Database,
    config: Config,
    dry_run: bool = True,
    batch_mode: str = "",
) -> dict[str, int]:
    """Review and apply proposed actions interactively.

    Shows each pending rename, move, or quarantine action and asks the
    user to approve or reject it.

    Args:
        db: Database instance.
        config: Application configuration.
        dry_run: If True, only show what would be done.
        batch_mode: If set, auto-approve ("apply") or auto-skip ("skip")
            all items without prompting. Empty string = interactive.

    Returns:
        A dict with counts of applied, skipped, and errored actions.
    """
    pending = db.get_pending_actions()
    if not pending:
        print("\n✓ No pending actions to review.")
        return {"applied": 0, "skipped": 0, "errors": 0}

    stats = {"applied": 0, "skipped": 0, "errors": 0}

    # Group by type for clearer presentation.
    renames = [a for a in pending if a["action_type"] == "rename"]
    moves = [a for a in pending if a["action_type"] == "move"]
    quarantines = [a for a in pending if a["action_type"] == "quarantine"]

    print(f"\n📋 Found {len(pending)} pending action(s):")
    print(f"    {len(renames)} renames, {len(moves)} moves, {len(quarantines)} quarantines")
    if not batch_mode:
        print("  Actions: [a]pply, [s]kip, [A]pply all, [S]kip all, [q]uit")
    print()

    apply_all = batch_mode == "apply"
    skip_all = batch_mode == "skip"

    for action in pending:
        source = Path(action["source_path"])
        target = Path(action["proposed_path"])
        action_type = action["action_type"]
        reason = action["reason"] or ""

        if apply_all:
            choice = "a"
        elif skip_all:
            choice = "s"
        else:
            print(f"  [{action_type.upper()}] {source.name} → {target.name}")
            if reason:
                print(f"    Reason: {reason}")
            print(f"    Source: {source}")
            print(f"    Target: {target}")
            choice = _prompt_choice(
                "    Apply? [a]pply / [s]kip / [A]pply all / [S]kip all / [q]uit: ",
                ["a", "s", "A", "S", "q"],
            )

        if choice == "q":
            print("  Stopping review.")
            break
        elif choice in ("s", "S"):
            if choice == "S":
                skip_all = True
            stats["skipped"] += 1
            continue
        elif choice in ("a", "A"):
            if choice == "A":
                apply_all = True

            if dry_run:
                print(f"    [DRY RUN] Would {action_type}: {source.name} → {target.name}")
                stats["applied"] += 1
                continue

            if not source.exists():
                print(f"    ⚠ Source no longer exists: {source}")
                stats["errors"] += 1
                continue

            try:
                target.parent.mkdir(parents=True, exist_ok=True)

                # Handle name conflicts.
                counter = 1
                final_target = target
                while final_target.exists() and final_target != source:
                    stem = target.stem
                    suffix = target.suffix
                    final_target = target.parent / f"{stem}_{counter}{suffix}"
                    counter += 1

                shutil.move(str(source), str(final_target))
                logger.info("Applied %s: %s → %s", action_type, source, final_target)

                # Mark as applied in DB.
                db.mark_action_applied(action["id"])
                db.conn.execute(
                    "UPDATE files SET path = ? WHERE id = ?",
                    (str(final_target.resolve()), action["file_id"]),
                )
                db._commit_or_defer()

                stats["applied"] += 1
            except Exception as exc:
                logger.exception("Error applying action: %s → %s", source, target)
                print(f"    ✗ Error: {exc}")
                stats["errors"] += 1

    print(f"\n  Actions: {stats['applied']} applied, {stats['skipped']} skipped, {stats['errors']} errors")
    return stats