"""ROM Organizer — CLI entry point.

A safe, offline-first tool for scanning, hashing, deduplicating,
normalizing, and organizing ROM files.

Usage:
    python main.py scan /path/to/roms --config config.json
    python main.py report
    python main.py normalize --dry-run
    python main.py organize --dry-run
    python main.py organize --apply

Design decision: We use argparse subcommands so each operation is
explicit and auditable. The tool is designed to be run step-by-step:
scan → hash → detect duplicates → normalize → organize.
Each step writes to the database, so you can stop and resume at any point.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import Config, load_config
from database import Database
from hasher import hash_all_files
from normalizer import normalize_all_files
from reporter import generate_report
from scanner import scan_directory
from sorter import (
    apply_actions,
    find_exact_duplicates,
    find_possible_duplicates,
    propose_organize_actions,
)

logger = logging.getLogger("rom_organizer")


def setup_logging(log_dir: str, verbose: bool = False) -> None:
    """Configure logging to both console and a timestamped log file.

    Args:
        log_dir: Directory for log files.
        verbose: If True, set console log level to DEBUG.
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"rom_organizer_{timestamp}.log"

    # Root logger.
    root_logger = logging.getLogger("rom_organizer")
    root_logger.setLevel(logging.DEBUG)

    # Console handler.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_fmt = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_fmt)
    root_logger.addHandler(console_handler)

    # File handler.
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_fmt)
    root_logger.addHandler(file_handler)

    logger.info("Log file: %s", log_file)


def cmd_scan(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'scan' subcommand: scan directory and hash files."""
    rom_root = Path(args.rom_root)
    if not rom_root.exists():
        logger.error("ROM directory does not exist: %s", rom_root)
        sys.exit(1)

    # Step 1: Scan for ROM files.
    count = scan_directory(rom_root, config, db)
    if count == 0:
        logger.info("No ROM files found. Check your extension list and directory.")
        return

    # Step 2: Hash all files.
    hash_stats = hash_all_files(db, config)
    logger.info(
        "Hashing: %d done, %d skipped, %d errors",
        hash_stats["hashed"],
        hash_stats["skipped"],
        hash_stats["errors"],
    )

    # Step 3: Detect duplicates.
    exact = find_exact_duplicates(db)
    possible = find_possible_duplicates(db, config)
    logger.info("Duplicates: %d exact groups, %d possible groups", exact, possible)

    # Step 4: Generate report.
    report_path = Path(config.output_dir) / "scan_report.txt"
    report = generate_report(db, report_path)
    print(report)


def cmd_report(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'report' subcommand: generate a report from existing data."""
    report_path = Path(config.output_dir) / "report.txt"
    report = generate_report(db, report_path)
    print(report)


def cmd_normalize(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'normalize' subcommand: compute normalized filenames."""
    dry_run = args.dry_run

    if dry_run:
        logger.info("=== DRY RUN — no files will be renamed ===")

    stats = normalize_all_files(db, config, dry_run=dry_run)
    logger.info(
        "Normalization: %d changed, %d unchanged, %d errors",
        stats["normalized"],
        stats["unchanged"],
        stats["errors"],
    )

    if dry_run:
        # Show what would be renamed.
        rows = db.conn.execute(
            "SELECT path, original_name, normalized_name FROM files WHERE normalized_name != original_name"
        ).fetchall()
        for row in rows:
            logger.info(
                "[DRY RUN] Would rename: %s → %s",
                row["original_name"],
                row["normalized_name"],
            )
        logger.info("=== DRY RUN COMPLETE — no files were renamed ===")


def cmd_organize(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'organize' subcommand: propose and optionally apply actions."""
    dry_run = args.dry_run

    # Step 1: Propose actions.
    action_stats = propose_organize_actions(db, config, dry_run=dry_run)
    logger.info(
        "Proposed: %d renames, %d quarantines, %d moves",
        action_stats["rename"],
        action_stats["quarantine"],
        action_stats["move"],
    )

    # Step 2: Apply or preview.
    apply_stats = apply_actions(db, config, dry_run=dry_run)
    logger.info(
        "Results: %d renames, %d quarantines, %d moves, %d skipped, %d errors",
        apply_stats["rename"],
        apply_stats["quarantine"],
        apply_stats["move"],
        apply_stats["skipped"],
        apply_stats["errors"],
    )

    # Step 3: Generate updated report.
    report_path = Path(config.output_dir) / "organize_report.txt"
    report = generate_report(db, report_path)
    print(report)


def cmd_rollback(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'rollback' subcommand: reverse applied actions.

    Reads applied actions from the database and reverses them by moving
    files back to their original locations. Supports selective rollback
    via --last N or --action-id ID.
    """
    last_n = getattr(args, "last", None)
    action_id = getattr(args, "action_id", None)

    applied = db.get_applied_actions_range(last_n=last_n, action_id=action_id)
    if not applied:
        logger.info("No applied actions to roll back.")
        return

    logger.info("Rolling back %d applied actions...", len(applied))

    # Backup DB before rollback.
    db.backup()

    for action in reversed(applied):
        source = Path(action["proposed_path"])
        target = Path(action["source_path"])

        if not source.exists():
            logger.warning("Cannot roll back — file not found: %s", source)
            continue

        # Check if target already exists — avoid overwriting.
        if target.exists():
            logger.warning(
                "Cannot roll back — target already exists: %s. "
                "Manual intervention required.",
                target,
            )
            continue

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            source.rename(target)
            logger.info("Rolled back: %s → %s", source, target)

            # Update the database.
            db.mark_action_rolled_back(action["id"])
            db.conn.execute(
                "UPDATE files SET path = ? WHERE id = ?",
                (str(target.resolve()), action["file_id"]),
            )
            db._commit_or_defer()

        except Exception:
            logger.exception("Error rolling back action %d", action["id"])

    logger.info("Rollback complete.")


def cmd_load_dats(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'load-dats' subcommand: load DAT files into the database."""
    dat_dir = Path(args.dat_dir or config.dat_dir)
    if not dat_dir.exists():
        logger.error("DAT directory does not exist: %s", dat_dir)
        sys.exit(1)

    from dat_parser import load_dat_dir

    dat_files = load_dat_dir(dat_dir)
    if not dat_files:
        logger.warning("No DAT files found in %s", dat_dir)
        return

    total_games = 0
    total_roms = 0
    with db.transaction():
        for dat in dat_files:
            dat_id = db.upsert_dat_file(
                filename=dat.filename,
                name=dat.header_name,
                description=dat.header_description,
                category=dat.header_category,
                version=dat.header_version,
            )
            for game in dat.games:
                game_id = db.add_dat_game(
                    dat_id=dat_id,
                    game_name=game.name,
                    description=game.description,
                    category=game.category,
                    clone_of=game.clone_of,
                    year=game.year,
                    manufacturer=game.manufacturer,
                )
                for rom in game.roms:
                    db.add_dat_rom(
                        game_id=game_id,
                        rom_name=rom.name,
                        size=rom.size,
                        crc32=rom.crc32,
                        md5=rom.md5,
                        sha256=rom.sha256,
                        merge_name=rom.merge_name,
                    )
                    total_roms += 1
                total_games += 1

    logger.info("Loaded %d DAT files, %d games, %d ROMs.", len(dat_files), total_games, total_roms)


def cmd_identify(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'identify' subcommand: match scanned files against DAT entries."""
    from progress import ProgressBar

    rows = db.get_all_files()
    identified = 0
    unmatched = 0
    total = len(rows)

    bar = ProgressBar(total=total, label="Identifying", unit="files")

    with db.transaction():
        for row in rows:
            sha256 = row["sha256"]
            md5 = row["md5"]
            crc32 = row["crc32"]

            match = None
            if sha256:
                matches = db.lookup_dat_by_sha256(sha256)
                if matches:
                    match = matches[0]
            if not match and md5:
                matches = db.lookup_dat_by_md5(md5)
                if matches:
                    match = matches[0]
            if not match and crc32:
                matches = db.lookup_dat_by_crc32(crc32)
                if matches:
                    match = matches[0]

            if match:
                db.update_file_dat_info(
                    row["id"],
                    game_name=match["game_name"],
                    description=match["description"],
                    system=match["category"],
                )
                identified += 1
                logger.debug("Identified: %s → %s", row["original_name"], match["game_name"])
            else:
                unmatched += 1

            bar.update(1)

    bar.close()
    logger.info("Identification complete: %d identified, %d unmatched.", identified, unmatched)


def cmd_review(args: argparse.Namespace, config: Config, db: Database) -> None:
    """Execute the 'review' subcommand: interactively review and approve actions."""
    from reviewer import review_corrupted, review_duplicates, review_actions

    dry_run = not args.apply
    review_corrupted_flag = args.corrupted
    review_duplicates_flag = args.duplicates
    review_actions_flag = args.actions

    # If no specific flags, review everything.
    if not review_corrupted_flag and not review_duplicates_flag and not review_actions_flag:
        review_corrupted_flag = True
        review_duplicates_flag = True
        review_actions_flag = True

    if dry_run:
        print("=" * 72)
        print("  DRY RUN — no files will be modified. Use --apply to make changes.")
        print("=" * 72)

    # Backup DB before any mutations.
    if not dry_run:
        db.backup()

    if review_corrupted_flag:
        print("\n" + "=" * 72)
        print("  REVIEW: CORRUPTED / ERROR FILES")
        print("=" * 72)
        review_corrupted(db, config, dry_run=dry_run)

    if review_duplicates_flag:
        print("\n" + "=" * 72)
        print("  REVIEW: EXACT DUPLICATES")
        print("=" * 72)
        review_duplicates(db, config, dry_run=dry_run)

    if review_actions_flag:
        print("\n" + "=" * 72)
        print("  REVIEW: PROPOSED ACTIONS")
        print("=" * 72)
        review_actions(db, config, dry_run=dry_run)

    if dry_run:
        print("\n" + "=" * 72)
        print("  DRY RUN COMPLETE — no files were modified.")
        print("  Re-run with --apply to make changes.")
        print("=" * 72)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        The configured ArgumentParser.
    """
    parser = argparse.ArgumentParser(
        prog="rom_organizer",
        description="A safe, offline-first ROM organizer MVP.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a JSON configuration file.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to the SQLite database file (default: rom_organizer.db).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ── scan ───────────────────────────────────────────────────────────
    scan_parser = subparsers.add_parser("scan", help="Scan a ROM directory and hash files.")
    scan_parser.add_argument("rom_root", type=str, help="Root directory to scan for ROMs.")

    # ── report ─────────────────────────────────────────────────────────
    subparsers.add_parser("report", help="Generate a report from existing scan data.")

    # ── normalize ──────────────────────────────────────────────────────
    norm_parser = subparsers.add_parser("normalize", help="Normalize ROM filenames.")
    norm_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview changes without modifying files (default: True).",
    )
    norm_parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply normalization (overrides --dry-run).",
    )

    # ── organize ──────────────────────────────────────────────────────
    org_parser = subparsers.add_parser("organize", help="Organize ROM files.")
    org_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview changes without modifying files (default: True).",
    )
    org_parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply organization (overrides --dry-run).",
    )

    # ── rollback ──────────────────────────────────────────────────────
    rollback_parser = subparsers.add_parser("rollback", help="Roll back applied actions.")
    rollback_parser.add_argument(
        "--last",
        type=int,
        default=None,
        help="Roll back only the last N actions (default: all).",
    )
    rollback_parser.add_argument(
        "--action-id",
        type=int,
        default=None,
        help="Roll back a specific action by ID.",
    )

    # ── load-dats ─────────────────────────────────────────────────────
    dat_parser = subparsers.add_parser("load-dats", help="Load DAT files for ROM identification.")
    dat_parser.add_argument(
        "dat_dir",
        type=str,
        nargs="?",
        default=None,
        help="Directory containing DAT files (overrides config.dat_dir).",
    )

    # ── identify ──────────────────────────────────────────────────────
    subparsers.add_parser("identify", help="Match scanned files against loaded DAT entries.")

    # ── review ───────────────────────────────────────────────────────
    review_parser = subparsers.add_parser(
        "review",
        help="Interactively review and approve actions from the scan report.",
    )
    review_parser.add_argument(
        "--corrupted",
        action="store_true",
        help="Review corrupted/error files.",
    )
    review_parser.add_argument(
        "--duplicates",
        action="store_true",
        help="Review exact duplicate groups.",
    )
    review_parser.add_argument(
        "--actions",
        action="store_true",
        help="Review pending proposed actions (renames, moves, quarantines).",
    )
    review_parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply approved actions (default is dry-run).",
    )

    return parser


def main() -> None:
    """Main entry point for the ROM Organizer CLI."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Load configuration.
    config = load_config(args.config)

    # Override DB path if specified on CLI.
    db_path = args.db or Path(config.db_path)

    # Set up logging.
    setup_logging(config.log_dir, verbose=args.verbose)

    logger.info("ROM Organizer starting — command: %s", args.command)

    # Initialize database.
    db = Database(db_path)

    try:
        if args.command == "scan":
            cmd_scan(args, config, db)
        elif args.command == "report":
            cmd_report(args, config, db)
        elif args.command == "normalize":
            # --apply overrides --dry-run.
            if args.apply:
                args.dry_run = False
            cmd_normalize(args, config, db)
        elif args.command == "organize":
            # --apply overrides --dry-run.
            if args.apply:
                args.dry_run = False
                # Backup DB before destructive operations.
                db.backup()
            cmd_organize(args, config, db)
        elif args.command == "rollback":
            cmd_rollback(args, config, db)
        elif args.command == "load-dats":
            cmd_load_dats(args, config, db)
        elif args.command == "identify":
            cmd_identify(args, config, db)
        elif args.command == "review":
            cmd_review(args, config, db)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception:
        logger.exception("Unexpected error.")
        sys.exit(1)
    finally:
        db.close()

    logger.info("ROM Organizer finished.")


if __name__ == "__main__":
    main()